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
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
)

type Data struct {
	mu   sync.Mutex
	data map[string]interface{}
}

type Server struct {
	config   Config
	data     Data
	commands []Command

	raft *raft.Raft

	memberList     *memberlist.Memberlist
	broadcastQueue *memberlist.TransmitLimitedQueue
	numOfNodes     int

	cancelCh          *chan (os.Signal)
	raftJoinSuccessCh *chan (BroadcastMessage)
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
		message, err := ReadMessage(connRW)

		if err != nil && err == io.EOF {
			// Connection closed
			break
		}

		if err != nil {
			fmt.Println(err)
			continue
		}

		if cmd, err := Decode(message); err != nil {
			// Return error to client
			connRW.Write([]byte(fmt.Sprintf("-Error %s\r\n\n", err.Error())))
			connRW.Flush()
			continue
		} else {
			// Look for plugin that handles this command and trigger it
			handled := false

			for _, c := range server.commands {
				if Contains[string](c.Commands(), strings.ToLower(cmd[0])) {
					c.HandleCommand(cmd, server, connRW.Writer)
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

func (server *Server) Start() {
	server.data.data = make(map[string]interface{})

	conf := server.config

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
	config := GetConfig()

	// Default BindAddr if it's not set
	if config.BindAddr == "" {
		if addr, err := GetIPAddress(); err != nil {
			log.Fatal(err)
		} else {
			config.BindAddr = addr
		}
	}

	cancelCh := make(chan (os.Signal), 1)
	signal.Notify(cancelCh, syscall.SIGINT, syscall.SIGTERM)

	raftJoinSuccessCh := make(chan BroadcastMessage)

	server := &Server{
		config: config,

		broadcastQueue: new(memberlist.TransmitLimitedQueue),
		numOfNodes:     0,

		commands: []Command{
			NewPingCommand(),
			NewSetGetCommand(),
			NewListCommand(),
		},

		cancelCh:          &cancelCh,
		raftJoinSuccessCh: &raftJoinSuccessCh,
	}

	go server.Start()

	<-cancelCh

	server.ShutDown()
}
