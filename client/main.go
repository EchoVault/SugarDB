package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

type Config struct {
	TLS  bool   `json:"tls" yaml:"tls"`
	Key  string `json:"key" yaml:"key"`
	Cert string `json:"cert" yaml:"cert"`
	Port uint16 `json:"port" yaml:"port"`
}

func main() {
	TLS := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	Key := flag.String("key", "", "The private key file path.")
	Cert := flag.String("cert", "", "The signed certificate file path.")
	Port := flag.Int("port", 7480, "Port to use. Default is 7480")

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
			TLS:  *TLS,
			Key:  *Key,
			Cert: *Cert,
			Port: uint16(*Port),
		}
	}

	var conn net.Conn
	var err error

	if !conf.TLS {
		fmt.Println("Starting client in TCP mode...")

		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", conf.Port))
		if err != nil {
			panic(err)
		}
	} else {
		// Dial TLS
		fmt.Println("Starting client in TLS mode...")

		f, err := os.Open(conf.Cert)

		if err != nil {
			panic(err)
		}

		cert, err := io.ReadAll(bufio.NewReader(f))

		if err != nil {
			panic(err)
		}

		rootCAs := x509.NewCertPool()

		ok := rootCAs.AppendCertsFromPEM(cert)
		if !ok {
			panic("Failed to parse certificate")
		}

		conn, err = tls.Dial("tcp", fmt.Sprintf("%s:%d", "localhost", conf.Port), &tls.Config{
			RootCAs: rootCAs,
		})

		if err != nil {
			panic(fmt.Sprintf("Handshake Error: %s", err.Error()))
		}
	}

	defer conn.Close()
}
