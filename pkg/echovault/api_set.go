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

func (server *EchoVault) SADD(key string, members ...string) (int, error) {
	cmd := append([]string{"SADD", key}, members...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SCARD(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SCARD", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SDIFF(keys ...string) ([]string, error) {
	cmd := append([]string{"SDIFF"}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) SDIFFSTORE(destination string, keys ...string) (int, error) {
	cmd := append([]string{"SDIFFSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SINTER(keys ...string) ([]string, error) {
	cmd := append([]string{"SINTER"}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) SINTERCARD(keys []string, limit int) (int, error) {
	cmd := append([]string{"SINTERCARD"}, keys...)
	cmd = append(cmd, strconv.Itoa(limit))
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SINTERSTORE(destination string, keys ...string) (int, error) {
	cmd := append([]string{"SINTERSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SISMEMBER(key, member string) (bool, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SISMEMBER", key, member}), nil, false)
	if err != nil {
		return false, err
	}
	return internal.ParseBooleanResponse(b)
}

func (server *EchoVault) SMEMBERS(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SMEMBERS", key}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) SMISMEMBER(key string, members ...string) ([]bool, error) {
	cmd := append([]string{"SMISMEMBER", key}, members...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseBooleanArrayResponse(b)
}

func (server *EchoVault) SMOVE(source, destination, member string) (bool, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SMOVE", source, destination, member}),
		nil,
		false,
	)
	if err != nil {
		return false, err
	}
	return internal.ParseBooleanResponse(b)
}

func (server *EchoVault) SPOP(key string, count int) ([]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SPOP", key, strconv.Itoa(count)}),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) SRANDMEMBER(key string, count int) ([]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SRANDMEMBER", key, strconv.Itoa(count)}),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) SREM(key string, members ...string) (int, error) {
	cmd := append([]string{"SREM", key}, members...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SUNION(keys ...string) ([]string, error) {
	cmd := append([]string{"SUNION"}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) SUNIONSTORE(destination string, keys ...string) (int, error) {
	cmd := append([]string{"SUNIONSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}
