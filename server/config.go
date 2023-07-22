package main

import (
	"encoding/json"
	"flag"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

type Config struct {
	TLS                bool   `json:"tls" yaml:"tls"`
	Key                string `json:"key" yaml:"key"`
	Cert               string `json:"cert" yaml:"cert"`
	Port               uint16 `json:"port" yaml:"port"`
	HTTP               bool   `json:"http" yaml:"http"`
	Plugins            string `json:"plugins" yaml:"plugins"`
	ServerID           string `json:"serverId" yaml:"serverId"`
	JoinAddr           string `json:"joinAddr" yaml:"joinAddr"`
	BindAddr           string `json:"bindAddr" yaml:"bindAddr"`
	RaftBindPort       uint16 `json:"raftPort" yaml:"raftPort"`
	MemberListBindPort uint16 `json:"mlPort" yaml:"mlPort"`
}

func GetConfig() Config {
	tls := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	key := flag.String("key", "", "The private key file path.")
	cert := flag.String("cert", "", "The signed certificate file path.")
	port := flag.Int("port", 7480, "Port to use. Default is 7480")
	http := flag.Bool("http", false, "Use HTTP protocol instead of raw TCP. Default is false")
	serverId := flag.String("serverId", "1", "Server ID in raft cluster. Leave empty for client.")
	joinAddr := flag.String("joinAddr", "", "Address of cluster member in a cluster to you want to join.")
	bindAddr := flag.String("bindAddr", "127.0.0.1", "Address to bind the server to.")
	raftBindPort := flag.Int("raftPort", 7481, "Port to use for intra-cluster communication. Leave on the client.")
	mlBindPort := flag.Int("mlPort", 7946, "Port to use for memberlist communication.")
	config := flag.String(
		"config",
		"",
		`File path to a JSON or YAML config file.The values in this config file will override the flag values.`,
	)

	flag.Parse()

	conf := Config{
		TLS:                *tls,
		Key:                *key,
		Cert:               *cert,
		HTTP:               *http,
		Port:               uint16(*port),
		ServerID:           *serverId,
		JoinAddr:           *joinAddr,
		BindAddr:           *bindAddr,
		RaftBindPort:       uint16(*raftBindPort),
		MemberListBindPort: uint16(*mlBindPort),
	}

	if len(*config) > 0 {
		// Override configurations from file
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

	}

	return conf
}
