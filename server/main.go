package main

import (
	"bufio"
	"crypto/tls"
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
	config  utils.Config
	data    Data
	plugins []Plugin
}

func (server *Server) Lock() {
	server.data.mu.Lock()
}

func (Server *Server) Unlock() {
	Server.data.mu.Unlock()
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
		fmt.Println("TCP/TLS mode enabled...")
		cer, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
		if err != nil {
			panic(err)
		}

		if l, err := tls.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", conf.Port), &tls.Config{
			Certificates: []tls.Certificate{cer},
		}); err != nil {
			panic(err)
		} else {
			listener = l
		}
	}

	if !conf.TLS {
		// TCP
		fmt.Println("Starting server in TCP mode...")
		if l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", conf.Port)); err != nil {
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
		fmt.Println("Starting server in HTTPS mode...")
		err = http.ListenAndServeTLS(fmt.Sprintf("%s:%d", "localhost", conf.Port), conf.Cert, conf.Key, nil)
	} else {
		fmt.Println("Starting server in HTTP mode...")
		err = http.ListenAndServe(fmt.Sprintf("%s:%d", "localhost", conf.Port), nil)
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

func (server *Server) Start() {
	server.data.data = make(map[string]interface{})

	server.config = utils.GetConfig()
	conf := server.config

	server.LoadPlugins()

	if conf.TLS && (len(conf.Key) <= 0 || len(conf.Cert) <= 0) {
		fmt.Println("Must provide key and certificate file paths for TLS mode.")
		return
	}

	if conf.HTTP {
		server.StartHTTP()
	} else {
		server.StartTCP()
	}
}

func main() {
	server := Server{}
	server.Start()
}
