package main

import (
	"errors"
	"flag"
	"log"
)

type Server struct {
	tls  *bool
	key  *string
	cert *string
}

func (server *Server) Start() error {
	return errors.New("server start to be implemented")
}

func main() {
	tls := flag.Bool("tls", false, "Start the server in TLS mode.")
	key := flag.String("key", "", "The private key file path.")
	cert := flag.String("cert", "", "The signed certificate file path.")

	flag.Parse()

	server := &Server{
		tls:  tls,
		key:  key,
		cert: cert,
	}

	err := server.Start()

	if err != nil {
		log.Fatal(err)
	}
}
