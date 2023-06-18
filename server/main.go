package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

type Config struct {
	TLS  bool   `json:"tls" yaml:"tls"`
	Key  string `json:"key" yaml:"key"`
	Cert string `json:"cert" yaml:"cert"`
	HTTP bool   `json:"http" yaml:"http"`
}

type Server struct {
	config Config
}

func (server *Server) Start() {
	conf := server.config

	if conf.TLS && (len(conf.Key) <= 0 || len(conf.Cert) <= 0) {
		fmt.Println("Must provide key and certificate file paths for TLS mode.")
		return
	}

	if !conf.TLS && conf.HTTP {
		// HTTP
		fmt.Println("HTTP mode enabled...")
	}

	if conf.TLS && conf.HTTP {
		// HTTPS
		fmt.Println("HTTPS mode enabled...")
	}

	if conf.TLS && !conf.HTTP {
		// TLS
		fmt.Println("TCP/TLS mode enabled...")
	}

	if !conf.TLS && !conf.HTTP {
		// TCP
		fmt.Println("TCP mode enabled...")
	}

	// Listen to connection
}

func main() {
	tls := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	key := flag.String("key", "", "The private key file path.")
	cert := flag.String("cert", "", "The signed certificate file path.")
	http := flag.Bool("http", false, "Use HTTP protocol instead of raw TCP. Default is false")
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
		}
	}

	server := Server{
		config: conf,
	}

	server.Start()
}
