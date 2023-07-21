package main

import (
	"encoding/json"
	"flag"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

type Config struct {
	TLS  bool   `json:"tls" yaml:"tls"`
	Key  string `json:"key" yaml:"key"`
	Cert string `json:"cert" yaml:"cert"`
	Port uint16 `json:"port" yaml:"port"`
	Addr string `json:"addr" yaml:"addr"`
}

func GetConfig() Config {
	// Shared
	tls := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	key := flag.String("key", "", "The private key file path.")
	cert := flag.String("cert", "", "The signed certificate file path.")
	port := flag.Int("port", 7480, "Port to use. Default is 7480")
	config := flag.String(
		"config",
		"",
		`File path to a JSON or YAML config file.The values in this config file will override the flag values.`,
	)
	addr := flag.String("addr", "127.0.0.1", "On client, this is the address of a server node to connect to.")

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
			Addr: *addr,
			Port: uint16(*port),
		}
	}

	return conf
}
