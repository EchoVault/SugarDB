package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

type Listener interface {
	Accept() (net.Conn, error)
}

type Config struct {
	TLS  bool   `json:"tls" yaml:"tls"`
	Key  string `json:"key" yaml:"key"`
	Cert string `json:"cert" yaml:"cert"`
	HTTP bool   `json:"http" yaml:"http"`
	Port uint16 `json:"port" yaml:"port"`
}

type Server struct {
	config Config
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
		conn.Write([]byte("Hello, Client!\n"))
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
	tls := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	key := flag.String("key", "", "The private key file path.")
	cert := flag.String("cert", "", "The signed certificate file path.")
	http := flag.Bool("http", false, "Use HTTP protocol instead of raw TCP. Default is false")
	port := flag.Int("port", 7480, "Port to use. Default is 7480")
	config := flag.String(
		"config",
		"",
		`File path to a JSON or YAML config file.The values in this config file will override the flag values.`,
	)

	flag.Parse()

	var conf Config

	if len(*config) > 0 {
		// Load config from config file
		if f, err := os.Open(*config); err != nil {
			panic(err)
		} else {
			defer f.Close()

			ext := path.Ext(f.Name())

			if ext == ".json" {
				json.NewDecoder(f).Decode(&conf)
			}

			if ext == ".yaml" || ext == ".yml" {
				yaml.NewDecoder(f).Decode(&conf)
			}
		}

	} else {
		conf = Config{
			TLS:  *tls,
			Key:  *key,
			Cert: *cert,
			HTTP: *http,
			Port: uint16(*port),
		}
	}

	server := Server{
		config: conf,
	}

	server.Start()
}
