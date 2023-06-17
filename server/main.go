package main

import (
	"flag"
	"fmt"
)

type Server struct {
	tls  *bool
	key  *string
	cert *string
}

func (server *Server) Start() {

	if *server.tls && (len(*server.key) <= 0 || len(*server.cert) <= 0) {
		fmt.Println("Must provide key and certificate file paths for TLS mode.")
		return
	}

	if *server.tls {
		fmt.Println("TLS mode activated...")
	} else {
		fmt.Println("Normal TCP mode...")
	}

	// Listen to connection
}

func main() {
	tls := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	key := flag.String("key", "", "The private key file path.")
	cert := flag.String("cert", "", "The signed certificate file path.")

	flag.Parse()

	server := &Server{
		tls:  tls,
		key:  key,
		cert: cert,
	}

	server.Start()
}
