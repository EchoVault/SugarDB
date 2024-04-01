// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package echovault

import (
	"bytes"
	"github.com/echovault/echovault/internal"
	"github.com/tidwall/resp"
	"net"
)

type connMap struct {
	readConn  *net.Conn
	writeConn *net.Conn
}

var conns map[string]connMap

type ReadPubSubMessage func() []string

func (server *EchoVault) SUBSCRIBE(name string, channels ...string) ReadPubSubMessage {
	// Initialize connection tracker if calling subscribe for the first time
	if conns == nil {
		conns = make(map[string]connMap)
	}

	// If connection with this name does not exist, create new connection it
	var readConn net.Conn
	var writeConn net.Conn
	if _, ok := conns[name]; !ok {
		readConn, writeConn = net.Pipe()
		conns[name] = connMap{
			readConn:  &readConn,
			writeConn: &writeConn,
		}
	}

	// Subscribe connection to the provided channels
	cmd := append([]string{"SUBSCRIBE"}, channels...)
	go func() {
		_, _ = server.handleCommand(server.context, internal.EncodeCommand(cmd), conns[name].writeConn, false)
	}()

	return func() []string {
		r := resp.NewConn(readConn)
		v, _, _ := r.ReadValue()

		res := make([]string, len(v.Array()))
		for i := 0; i < len(res); i++ {
			res[i] = v.Array()[i].String()
		}

		return res
	}
}

func (server *EchoVault) UNSUBSCRIBE(name string, channels ...string) {
	if conns == nil {
		return
	}

	if _, ok := conns[name]; !ok {
		return
	}

	cmd := append([]string{"UNSUBSCRIBE"}, channels...)
	_, _ = server.handleCommand(server.context, internal.EncodeCommand(cmd), conns[name].writeConn, false)
}

func (server *EchoVault) PSUBSCRIBE(name string, patterns ...string) ReadPubSubMessage {
	// Initialize connection tracker if calling subscribe for the first time
	if conns == nil {
		conns = make(map[string]connMap)
	}

	// If connection with this name does not exist, create new connection it
	var readConn net.Conn
	var writeConn net.Conn
	if _, ok := conns[name]; !ok {
		readConn, writeConn = net.Pipe()
		conns[name] = connMap{
			readConn:  &readConn,
			writeConn: &writeConn,
		}
	}

	// Subscribe connection to the provided channels
	cmd := append([]string{"PSUBSCRIBE"}, patterns...)
	go func() {
		_, _ = server.handleCommand(server.context, internal.EncodeCommand(cmd), conns[name].writeConn, false)
	}()

	return func() []string {
		r := resp.NewConn(readConn)
		v, _, _ := r.ReadValue()

		res := make([]string, len(v.Array()))
		for i := 0; i < len(res); i++ {
			res[i] = v.Array()[i].String()
		}

		return res
	}
}

func (server *EchoVault) PUNSUBSCRIBE(name string, patterns ...string) {
	if conns == nil {
		return
	}

	if _, ok := conns[name]; !ok {
		return
	}

	cmd := append([]string{"PUNSUBSCRIBE"}, patterns...)
	_, _ = server.handleCommand(server.context, internal.EncodeCommand(cmd), conns[name].writeConn, false)
}

func (server *EchoVault) PUBLISH(channel, message string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PUBLISH", channel, message}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) PUBSUB_CHANNELS(pattern string) ([]string, error) {
	cmd := []string{"PUBSUB", "CHANNELS"}
	if pattern != "" {
		cmd = append(cmd, pattern)
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) PUBSUB_NUMPAT() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PUBSUB", "NUMPAT"}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) PUBSUB_NUMSUB(channels ...string) (map[string]int, error) {
	cmd := append([]string{"PUBSUB", "NUMSUB"}, channels...)

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	r := resp.NewReader(bytes.NewReader(b))
	v, _, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	arr := v.Array()

	result := make(map[string]int, len(arr))
	for _, entry := range arr {
		e := entry.Array()
		result[e[0].String()] = e[1].Integer()
	}

	return result, nil
}
