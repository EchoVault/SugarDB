package main

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"plugin"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/kelvinmwinuka/memstore/utils"
)

type Plugin interface {
	Name() string
	Commands() []string
	Description() string
	HandleCommand(cmd []string, server interface{}, conn *bufio.Writer)
}

type Data struct {
	mu   sync.Mutex
	data map[string]interface{}
}

type Server struct {
	config     utils.Config
	data       Data
	plugins    []Plugin
	raft       *raft.Raft
	memberList *memberlist.Memberlist
}

func (server *Server) Lock() {
	server.data.mu.Lock()
}

func (server *Server) Unlock() {
	server.data.mu.Unlock()
}

func (server *Server) GetData(key string) interface{} {
	return server.data.data[key]
}

func (server *Server) SetData(key string, value interface{}) {
	server.data.data[key] = value
}

func (server *Server) handleConnection(conn net.Conn) {
	connRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		message, err := utils.ReadMessage(connRW)

		if err != nil && err == io.EOF {
			// Connection closed
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
			// Look for plugin that handles this command and trigger it
			handled := false

			for _, plugin := range server.plugins {
				if utils.Contains[string](plugin.Commands(), strings.ToLower(cmd[0])) {
					plugin.HandleCommand(cmd, server, connRW.Writer)
					handled = true
				}
			}

			if !handled {
				connRW.Write([]byte(fmt.Sprintf("-Error %s command not supported\r\n\n", strings.ToUpper(cmd[0]))))
				connRW.Flush()
			}
		}
	}

	conn.Close()
}

func (server *Server) StartTCP() {
	conf := server.config
	var listener net.Listener

	if conf.TLS {
		// TLS
		fmt.Printf("Starting TLS server at Address %s, Port %d...\n", conf.Addr, conf.Port)
		cer, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
		if err != nil {
			panic(err)
		}

		if l, err := tls.Listen("tcp", fmt.Sprintf("%s:%d", conf.Addr, conf.Port), &tls.Config{
			Certificates: []tls.Certificate{cer},
		}); err != nil {
			panic(err)
		} else {
			listener = l
		}
	}

	if !conf.TLS {
		// TCP
		fmt.Printf("Starting TCP server at Address %s, Port %d...\n", conf.Addr, conf.Port)
		if l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.Addr, conf.Port)); err != nil {
			panic(err)
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
		go server.handleConnection(conn)
	}
}

func (server *Server) StartHTTP() {
	conf := server.config

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from memstore!"))
	})

	var err error

	if conf.TLS {
		fmt.Printf("Starting HTTPS server at Address %s, Port %d...\n", conf.Addr, conf.Port)
		err = http.ListenAndServeTLS(fmt.Sprintf("%s:%d", conf.Addr, conf.Port), conf.Cert, conf.Key, nil)
	} else {
		fmt.Printf("Starting HTTP server at Address %s, Port %d...\n", conf.Addr, conf.Port)
		err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.Addr, conf.Port), nil)
	}

	if err != nil {
		panic(err)
	}
}

func (server *Server) LoadPlugins() {
	conf := server.config

	// Load plugins
	pluginDirs, err := os.ReadDir(conf.Plugins)

	if err != nil {
		log.Fatal(err)
	}

	for _, file := range pluginDirs {
		if file.IsDir() {
			switch file.Name() {
			case "commands":
				files, err := os.ReadDir(path.Join(conf.Plugins, "commands"))

				if err != nil {
					log.Fatal(err)
				}

				for _, file := range files {
					if !strings.HasSuffix(file.Name(), ".so") {
						// Skip files that are not .so
						continue
					}
					p, err := plugin.Open(path.Join(conf.Plugins, "commands", file.Name()))
					if err != nil {
						log.Fatal(err)
					}

					pluginSymbol, err := p.Lookup("Plugin")
					if err != nil {
						fmt.Printf("unexpected plugin symbol in plugin %s\n", file.Name())
						continue
					}

					plugin, ok := pluginSymbol.(Plugin)
					if !ok {
						fmt.Printf("invalid plugin signature in plugin %s \n", file.Name())
						continue
					}

					// Check if a plugin that handles the same command already exists
					for _, loadedPlugin := range server.plugins {
						containsMutual, elem := utils.ContainsMutual[string](loadedPlugin.Commands(), plugin.Commands())
						if containsMutual {
							fmt.Printf("plugin that handles %s command already exists. Please handle a different command.\n", elem)
						}
					}

					server.plugins = append(server.plugins, plugin)
				}
			}
		}
	}
}

// Implement raft.FSM interface
func (server *Server) Apply(log *raft.Log) interface{} {
	return nil
}

func (server *Server) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (server *Server) Restore(snapshot io.ReadCloser) error {
	return nil
}

// Implement raft.StableStore interface
func (server *Server) Set(key []byte, value []byte) error {
	return nil
}

func (server *Server) Get(key []byte) ([]byte, error) {
	return []byte{}, nil
}

func (server *Server) SetUint64(key []byte, val uint64) error {
	return nil
}

func (server *Server) GetUint64(key []byte) (uint64, error) {
	return 0, nil
}

func (server *Server) Start() {
	server.data.data = make(map[string]interface{})

	server.config = utils.GetConfig()
	conf := server.config

	server.LoadPlugins()

	if conf.TLS && (len(conf.Key) <= 0 || len(conf.Cert) <= 0) {
		fmt.Println("Must provide key and certificate file paths for TLS mode.")
		return
	}

	if addr, err := getServerAddresses(); err != nil {
		log.Fatal(err)
	} else {
		conf.Addr = addr
		server.config.Addr = addr
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(conf.ServerID)

	raftLogStore := raft.NewInmemStore()
	raftStableStore := raft.NewInmemStore()
	raftSnapshotStore := raft.NewInmemSnapshotStore()

	raftAddr := fmt.Sprintf("%s:%d", conf.Addr, conf.ClusterPort)
	raftAdvertiseAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		log.Fatal("Could not resolve advertise address.")
	}

	raftTransport, err := raft.NewTCPTransport(
		raftAddr,
		raftAdvertiseAddr,
		10,
		500*time.Millisecond,
		os.Stdout,
	)

	if err != nil {
		log.Fatal(err)
	}

	// Start raft server
	raftServer, err := raft.NewRaft(
		raftConfig,
		&raft.MockFSM{},
		raftLogStore,
		raftStableStore,
		raftSnapshotStore,
		raftTransport,
	)

	if err != nil {
		log.Fatalf("Could not start node with error; %s", err)
	}

	server.raft = raftServer

	if conf.JoinAddr == "" {
		// Start memberlist cluster
		memberList, err := memberlist.Create(memberlist.DefaultLocalConfig())
		if err != nil {
			log.Fatal("Could not start memberlist cluster.")
		}

		server.memberList = memberList

		// Bootstrap raft cluster
		if err := server.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(conf.ServerID),
					Address:  raft.ServerAddress(raftAddr),
				},
			},
		}).Error(); err != nil {
			log.Fatal(err)
		}
	}

	if conf.HTTP {
		server.StartHTTP()
	} else {
		server.StartTCP()
	}
}

func getServerAddresses() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatal(err)
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", errors.New("could not get IP Addresses")
}

func main() {
	server := &Server{}
	server.Start()
}
