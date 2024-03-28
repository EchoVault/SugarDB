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
	"github.com/echovault/echovault/internal"
	"strconv"
)

func (server *EchoVault) LLEN(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LLEN", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) LRANGE(key string, start, end int) ([]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"LRANGE", key, strconv.Itoa(start), strconv.Itoa(end)}),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) LINDEX(key string, index int) (string, error) {
	b, err := server.handleCommand(
		server.context, internal.EncodeCommand([]string{"LINDEX", key, strconv.Itoa(index)}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LSET(key string, index int, value string) (string, error) {
	b, err := server.handleCommand(
		server.context, internal.EncodeCommand([]string{"LSET", key, strconv.Itoa(index), value}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LTRIM(key string, start int, end int) (string, error) {
	b, err := server.handleCommand(
		server.context, internal.EncodeCommand([]string{"LTRIM", key, strconv.Itoa(start), strconv.Itoa(end)}),
		nil,
		false,
	)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LREM(key string, count int, value string) (string, error) {
	b, err := server.handleCommand(
		server.context, internal.EncodeCommand([]string{"LREM", key, strconv.Itoa(count), value}),
		nil,
		false,
	)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LMOVE(source, destination, whereFrom, whereTo string) (string, error) {
	b, err := server.handleCommand(
		server.context, internal.EncodeCommand([]string{"LMOVE", source, destination, whereFrom, whereTo}),
		nil,
		false,
	)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LPOP(key string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LPOP", key}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) RPOP(key string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"RPOP", key}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LPUSH(key string, values ...string) (string, error) {
	cmd := append([]string{"LPUSH", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LPUSHX(key string, values ...string) (string, error) {
	cmd := append([]string{"LPUSHX", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) RPUSH(key string, values ...string) (string, error) {
	cmd := append([]string{"RPUSH", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) RPUSHX(key string, values ...string) (string, error) {
	cmd := append([]string{"RPUSHX", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
