package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/kelvinmwinuka/memstore/serialization"
	"github.com/kelvinmwinuka/memstore/utils"
)

type Data struct {
	mu   sync.Mutex
	data map[string]interface{}
}

type Server struct {
	config utils.Config
	data   Data
}

func (server *Server) handleConnection(conn net.Conn) {
	connRW := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		message, err := utils.ReadMessage(connRW)

		if err != nil && err == io.EOF {
			fmt.Println(err)
			break
		}

		if err != nil {
			fmt.Println(err)
			continue
		}

		if cmd, err := serialization.Decode(message); err != nil {
			// Return error to client
			serialization.Encode(connRW, fmt.Sprintf("Error %s", err.Error()))
			continue
		} else {
			processCommand(cmd, connRW, server)
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

func (server *Server) Start() {
	server.data.data = make(map[string]interface{})

	conf := server.config

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
	conf := utils.GetConfig()

	server := Server{
		config: conf,
	}

	server.Start()
}
