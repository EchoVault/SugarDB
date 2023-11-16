package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
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
	"syscall"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/kelvinmwinuka/memstore/src/utils"
)

type Plugin interface {
	Name() string
	Commands() []string
	Description() string
	HandleCommand(cmd []string, server interface{}) ([]byte, error)
}

type Server struct {
	config utils.Config

	store    map[string]interface{}
	keyLocks map[string]*sync.RWMutex

	plugins []Plugin

	raft *raft.Raft

	memberList     *memberlist.Memberlist
	broadcastQueue *memberlist.TransmitLimitedQueue
	numOfNodes     int

	pubSub *PubSub

	cancelCh *chan (os.Signal)
}

func (server *Server) KeyLock(key string) {
	server.keyLocks[key].Lock()
}

func (server *Server) KeyUnlock(key string) {
	server.keyLocks[key].Unlock()
}

func (server *Server) KeyRLock(key string) {
	server.keyLocks[key].RLock()
}

func (server *Server) KeyRUnlock(key string) {
	server.keyLocks[key].RUnlock()
}

func (server *Server) KeyExists(key string) bool {
	return server.keyLocks[key] != nil
}

func (server *Server) CreateKey(key string, value interface{}) {
	server.keyLocks[key] = &sync.RWMutex{}
	server.store[key] = value
}

func (server *Server) GetValue(key string) interface{} {
	return server.store[key]
}

func (server *Server) SetValue(key string, value interface{}) {
	server.store[key] = value
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
			// Handle subscribe command
			if strings.EqualFold(cmd[0], "subscribe") {
				switch len(cmd) {
				case 1:
					server.pubSub.Subscribe(&conn, nil, nil)
				case 2:
					server.pubSub.Subscribe(&conn, cmd[1], nil)
				case 3:
					server.pubSub.Subscribe(&conn, cmd[1], cmd[2])
				default:
					connRW.Write([]byte("-Error wrong number of arguments\r\n\n"))
					connRW.Flush()
					continue
				}

				connRW.Write([]byte("+SUBSCRIBE_OK\r\n\n"))
				connRW.Flush()
				continue
			}

			// Handle unsubscribe command
			if strings.EqualFold(cmd[0], "unsubscribe") {
				switch len(cmd) {
				case 1:
					server.pubSub.Unsubscribe(&conn, nil)
				case 2:
					server.pubSub.Unsubscribe(&conn, cmd[1])
				default:
					connRW.Write([]byte("-Error wrong number of arguments\r\n\n"))
					connRW.Flush()
					continue
				}

				connRW.Write([]byte("+OK\r\n\n"))
				connRW.Flush()
				continue
			}

			// Handle other commands that need to be synced across the cluster
			applyRequest := utils.ApplyRequest{CMD: cmd}
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

func (server *Server) StartTCP() {
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

func (server *Server) LoadPlugins() {
	conf := server.config

	// Load plugins /usr/local/lib/memstore
	pluginDirs, err := os.ReadDir(conf.PluginDir)

	if err != nil {
		log.Fatal(err, pluginDirs)
	}

	for _, file := range pluginDirs {
		if file.IsDir() {
			switch file.Name() {
			case "commands":
				files, err := os.ReadDir(path.Join(conf.PluginDir, "commands"))

				if err != nil {
					log.Fatal(err)
				}

				for _, file := range files {
					if !strings.HasSuffix(file.Name(), ".so") {
						continue
					}
					p, err := plugin.Open(path.Join(conf.PluginDir, "commands", file.Name()))
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

func (server *Server) Start() {
	conf := server.config

	server.store = make(map[string]interface{})
	server.keyLocks = make(map[string]*sync.RWMutex)

	server.LoadPlugins()

	if conf.TLS && (len(conf.Key) <= 0 || len(conf.Cert) <= 0) {
		fmt.Println("Must provide key and certificate file paths for TLS mode.")
		return
	}

	server.RaftInit()
	server.MemberListInit()

	if conf.HTTP {
		server.StartHTTP()
	} else {
		server.StartTCP()
	}
}

func (server *Server) ShutDown() {
	server.RaftShutdown()
	server.MemberListShutdown()
}

func main() {
	config := utils.GetConfig()

	// Default BindAddr if it's not set
	if config.BindAddr == "" {
		if addr, err := utils.GetIPAddress(); err != nil {
			log.Fatal(err)
		} else {
			config.BindAddr = addr
		}
	}

	cancelCh := make(chan (os.Signal), 1)
	signal.Notify(cancelCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	server := &Server{
		config: config,

		broadcastQueue: new(memberlist.TransmitLimitedQueue),
		numOfNodes:     0,

		pubSub: NewPubSub(),

		cancelCh: &cancelCh,
	}

	go server.Start()

	<-cancelCh

	server.ShutDown()
}
