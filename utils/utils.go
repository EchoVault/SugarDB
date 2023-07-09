package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	TLS     bool   `json:"tls" yaml:"tls"`
	Key     string `json:"key" yaml:"key"`
	Cert    string `json:"cert" yaml:"cert"`
	Port    uint16 `json:"port" yaml:"port"`
	HTTP    bool   `json:"http" yaml:"http"`
	Plugins string `json:"plugins" yaml:"plugins"`
}

func GetConfig() Config {
	tls := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	key := flag.String("key", "", "The private key file path.")
	cert := flag.String("cert", "", "The signed certificate file path.")
	http := flag.Bool("http", false, "Use HTTP protocol instead of raw TCP. Default is false")
	port := flag.Int("port", 7480, "Port to use. Default is 7480")
	plugins := flag.String("plugins", ".", "The path to the plugins folder.")
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
			TLS:     *tls,
			Key:     *key,
			Cert:    *cert,
			HTTP:    *http,
			Port:    uint16(*port),
			Plugins: *plugins,
		}
	}

	return conf
}

func Contains[T comparable](arr []T, elem T) bool {
	for _, v := range arr {
		if v == elem {
			return true
		}
	}
	return false
}

func ContainsMutual[T comparable](arr1 []T, arr2 []T) (bool, T) {
	for _, a := range arr1 {
		for _, b := range arr2 {
			if a == b {
				return true, a
			}
		}
	}
	return false, arr1[0]
}

func IsInteger(n float64) bool {
	return math.Mod(n, 1.0) == 0
}

func AdaptType(s string) interface{} {
	// Adapt the type of the parameter to string, float64 or int
	n, err := strconv.ParseFloat(s, 32)

	if err != nil {
		return s
	}

	if IsInteger(n) {
		return int(n)
	}

	return n
}

func IncrBy(num interface{}, by interface{}) (interface{}, error) {
	if !Contains[string]([]string{"int", "float64"}, reflect.TypeOf(num).String()) {
		return nil, errors.New("can only increment number")
	}
	if !Contains[string]([]string{"int", "float64"}, reflect.TypeOf(by).String()) {
		return nil, errors.New("can only increment by number")
	}

	n, _ := num.(float64)
	b, _ := by.(float64)
	res := n + b

	if IsInteger(res) {
		return int(res), nil
	}

	return res, nil
}

func ReadMessage(r *bufio.ReadWriter) (message string, err error) {
	var line [][]byte

	for {
		b, _, err := r.ReadLine()

		if err != nil {
			return "", err
		}

		if bytes.Equal(b, []byte("")) {
			// End of message
			break
		}

		line = append(line, b)
	}

	return fmt.Sprintf("%s\r\n", string(bytes.Join(line, []byte("\r\n")))), nil
}
