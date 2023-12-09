package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/acl"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"plugin"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/kelvinmwinuka/memstore/src/utils"
)

type Server struct {
	config utils.Config

	connID atomic.Uint64

	store           map[string]interface{}
	keyLocks        map[string]*sync.RWMutex
	keyCreationLock *sync.Mutex

	commands []utils.Command

	raft *raft.Raft

	memberList     *memberlist.Memberlist
	broadcastQueue *memberlist.TransmitLimitedQueue
	numOfNodes     int

	cancelCh *chan os.Signal

	ACL *acl.ACL
}

func (server *Server) KeyLock(ctx context.Context, key string) (bool, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		default:
			ok := server.keyLocks[key].TryLock()
			if ok {
				return true, nil
			}
		case <-ctx.Done():
			return false, context.Cause(ctx)
		}
		<-ticker.C
	}
}

func (server *Server) KeyUnlock(key string) {
	server.keyLocks[key].Unlock()
}

func (server *Server) KeyRLock(ctx context.Context, key string) (bool, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		default:
			ok := server.keyLocks[key].TryRLock()
			if ok {
				return true, nil
			}
		case <-ctx.Done():
			return false, context.Cause(ctx)
		}
		<-ticker.C
	}
}

func (server *Server) KeyRUnlock(key string) {
	server.keyLocks[key].RUnlock()
}

func (server *Server) KeyExists(key string) bool {
	return server.keyLocks[key] != nil
}

func (server *Server) CreateKeyAndLock(ctx context.Context, key string) (bool, error) {
	server.keyCreationLock.Lock()
	defer server.keyCreationLock.Unlock()

	if !server.KeyExists(key) {
		keyLock := &sync.RWMutex{}
		keyLock.Lock()
		server.keyLocks[key] = keyLock
		return true, nil
	}

	return server.KeyLock(ctx, key)
}

func (server *Server) GetValue(key string) interface{} {
	return server.store[key]
}

func (server *Server) SetValue(ctx context.Context, key string, value interface{}) {
	server.store[key] = value
}

func (server *Server) getCommand(cmd string) (utils.Command, error) {
	for _, command := range server.commands {
		if strings.EqualFold(command.Command, cmd) {
			return command, nil
		}
	}
	return utils.Command{}, fmt.Errorf("command %s not supported", cmd)
}

func (server *Server) handlePluginCommand(ctx context.Context, cmd []string, conn *net.Conn) ([]byte, error) {
	command, err := server.getCommand(cmd[0])
	if err != nil {
		return nil, err
	}
	if command.HandleWithConnection {
		return command.Plugin.HandleCommandWithConnection(ctx, cmd, server, conn)
	}
	return command.Plugin.HandleCommand(ctx, cmd, server)
}

func (server *Server) handleConnection(ctx context.Context, conn net.Conn) {
	server.ACL.RegisterConnection(&conn)

	connRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	cid := server.connID.Add(1)
	ctx = context.WithValue(ctx, utils.ContextConnID("ConnectionID"),
		fmt.Sprintf("%s-%d", ctx.Value(utils.ContextServerID("ServerID")), cid))

	for {
		message, err := utils.ReadMessage(connRW)

		if err != nil && err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println(err)
			continue
		}

		if cmd, err := utils.Decode(message); err != nil {
			// Return error to client
			connRW.Write([]byte(fmt.Sprintf("-Error %s\r\n\n", err.Error())))
			connRW.Flush()
			continue
		} else {
			command, err := server.getCommand(cmd[0])

			if err != nil {
				connRW.WriteString(fmt.Sprintf("-%s\r\n\n", err.Error()))
				connRW.Flush()
			}

			if !server.IsInCluster() || !command.Sync {
				if res, err := server.handlePluginCommand(ctx, cmd, &conn); err != nil {
					connRW.Write([]byte(fmt.Sprintf("-%s\r\n\n", err.Error())))
				} else {
					connRW.Write(res)
				}
				connRW.Flush()
				continue
			}

			// Handle other commands that need to be synced across the cluster
			serverId, _ := ctx.Value(utils.ContextServerID("ServerID")).(string)
			connectionId, _ := ctx.Value(utils.ContextConnID("ConnectionID")).(string)

			applyRequest := utils.ApplyRequest{
				ServerID:     serverId,
				ConnectionID: connectionId,
				CMD:          cmd,
			}

			b, err := json.Marshal(applyRequest)

			if err != nil {
				connRW.Write([]byte("-Error could not parse request\r\n\n"))
				connRW.Flush()
				continue
			}

			if server.isRaftLeader() {
				applyFuture := server.raft.Apply(b, 500*time.Millisecond)

				if err := applyFuture.Error(); err != nil {
					connRW.WriteString(fmt.Sprintf("-Error %s\r\n\n", err.Error()))
					connRW.Flush()
					continue
				}

				r, ok := applyFuture.Response().(utils.ApplyResponse)

				if !ok {
					connRW.WriteString(fmt.Sprintf("-Error unprocessable entity %v\r\n\n", r))
					connRW.Flush()
					continue
				}

				if r.Error != nil {
					connRW.WriteString(fmt.Sprintf("-Error %s\r\n\n", r.Error.Error()))
					connRW.Flush()
					continue
				}

				connRW.Write(r.Response)
				connRW.Flush()

				// TODO: Add command to AOF
			} else {
				// TODO: Forward message to leader and wait for a response
				connRW.Write([]byte("-Error not cluster leader, cannot carry out command\r\n\n"))
				connRW.Flush()
			}
		}
	}

	conn.Close()
}

func (server *Server) StartTCP(ctx context.Context) {
	conf := server.config
	var listener net.Listener

	if conf.TLS {
		// TLS
		fmt.Printf("Starting TLS server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		cer, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
		if err != nil {
			log.Fatal(err)
		}

		if l, err := tls.Listen("tcp", fmt.Sprintf("%s:%d", conf.BindAddr, conf.Port), &tls.Config{
			Certificates: []tls.Certificate{cer},
		}); err != nil {
			log.Fatal(err)
		} else {
			listener = l
		}
	}

	if !conf.TLS {
		// TCP
		fmt.Printf("Starting TCP server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		if l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.BindAddr, conf.Port)); err != nil {
			log.Fatal(err)
		} else {
			listener = l
		}
	}

	// Listen to connection
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Could not establish connection")
			continue
		}
		// Read loop for connection
		go server.handleConnection(ctx, conn)
	}
}

func (server *Server) StartHTTP(ctx context.Context) {
	conf := server.config

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from memstore!"))
	})

	var err error

	if conf.TLS {
		fmt.Printf("Starting HTTPS server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		err = http.ListenAndServeTLS(fmt.Sprintf("%s:%d", conf.BindAddr, conf.Port), conf.Cert, conf.Key, nil)
	} else {
		fmt.Printf("Starting HTTP server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.BindAddr, conf.Port), nil)
	}

	if err != nil {
		panic(err)
	}
}

func (server *Server) LoadPlugins(ctx context.Context) {
	conf := server.config

	// Load ACL Internal Commands
	server.commands = append(server.commands, acl.ACLPlugin.GetCommands()...)

	// Load plugins /usr/local/lib/memstore
	files, err := os.ReadDir(conf.PluginDir)

	if err != nil {
		log.Fatal(err, files)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".so") {
			continue
		}
		p, err := plugin.Open(path.Join(conf.PluginDir, file.Name()))
		if err != nil {
			log.Fatal(err)
		}

		pluginSymbol, err := p.Lookup("Plugin")
		if err != nil {
			fmt.Printf("unexpected plugin symbol in plugin %s\n", file.Name())
			continue
		}

		pl, ok := pluginSymbol.(utils.Plugin)
		if !ok {
			fmt.Printf("invalid plugin signature in plugin %s \n", file.Name())
			continue
		}

		b, err := pl.Commands()
		if err != nil {
			fmt.Println(err)
			continue
		}

		plCommands, err := utils.UnmarshalCommandsJSON(b)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for i := 0; i < len(plCommands); i++ {
			plCommands[i].Plugin = pl
		}

		// Check if a plugin that handles the same command already exists
		commandClash := false

		for _, lc := range server.commands {
			for _, pc := range plCommands {
				if strings.EqualFold(lc.Command, pc.Command) {
					commandClash = true
					fmt.Printf("plugin that handles %s command already exists.\n", pc.Command)
				}
			}
		}

		if !commandClash {
			server.commands = append(server.commands, plCommands...)
		}
	}

	fmt.Println(server.commands)
}

func (server *Server) Start(ctx context.Context) {
	conf := server.config

	server.store = make(map[string]interface{})
	server.keyLocks = make(map[string]*sync.RWMutex)
	server.keyCreationLock = &sync.Mutex{}

	server.LoadPlugins(ctx)

	if conf.TLS && (len(conf.Key) <= 0 || len(conf.Cert) <= 0) {
		fmt.Println("Must provide key and certificate file paths for TLS mode.")
		return
	}

	if server.IsInCluster() {
		server.RaftInit(ctx)
		server.MemberListInit(ctx)
	}

	if conf.HTTP {
		server.StartHTTP(ctx)
	} else {
		server.StartTCP(ctx)
	}
}

func (server *Server) IsInCluster() bool {
	return server.config.BootstrapCluster || server.config.JoinAddr != ""
}

func (server *Server) ShutDown(ctx context.Context) {
	if server.IsInCluster() {
		server.RaftShutdown(ctx)
		server.MemberListShutdown(ctx)
	}
}

func main() {
	config, err := utils.GetConfig()

	if err != nil {
		log.Fatal(err)
	}

	ctx := context.WithValue(context.Background(), utils.ContextServerID("ServerID"), config.ServerID)

	// Default BindAddr if it's not set
	if config.BindAddr == "" {
		if addr, err := utils.GetIPAddress(); err != nil {
			log.Fatal(err)
		} else {
			config.BindAddr = addr
		}
	}

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	server := &Server{
		config: config,

		connID: atomic.Uint64{},

		broadcastQueue: new(memberlist.TransmitLimitedQueue),
		numOfNodes:     0,

		ACL: acl.NewACL(config),

		cancelCh: &cancelCh,
	}

	go server.Start(ctx)

	<-cancelCh

	server.ShutDown(ctx)
}
