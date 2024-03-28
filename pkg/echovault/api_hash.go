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

type HRANDFIELDOptions struct {
	Count      int
	WithValues bool
}

func (server *EchoVault) HSET(key string, fieldValuePairs map[string]string) (int, error) {
	cmd := []string{"HSET", key}

	for k, v := range fieldValuePairs {
		cmd = append(cmd, []string{k, v}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) HSETNX(key string, fieldValuePairs map[string]string) (int, error) {
	cmd := []string{"HSETNX", key}

	for k, v := range fieldValuePairs {
		cmd = append(cmd, []string{k, v}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) HSTRLEN(key string, fields ...string) ([]int, error) {
	cmd := append([]string{"HSTRLEN", key}, fields...)

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	return internal.ParseIntegerArrayResponse(b)
}

func (server *EchoVault) HVALS(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HVALS", key}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) HRANDFIELD(key string, options HRANDFIELDOptions) ([]string, error) {
	cmd := []string{"HRANDFIELD", key}

	if options.Count == 0 {
		cmd = append(cmd, strconv.Itoa(1))
	} else {
		cmd = append(cmd, strconv.Itoa(options.Count))
	}

	if options.WithValues {
		cmd = append(cmd, "WITHVALUES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) HLEN(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HLEN", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) HKEYS(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HKEYS", key}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) HINCRBY(key, field string, increment int) (float64, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"HINCRBY", key, field, strconv.Itoa(increment)}),
		nil,
		false,
	)
	if err != nil {
		return 0, err
	}
	return internal.ParseFloatResponse(b)
}

func (server *EchoVault) HINCRBYFLOAT(key, field string, increment float64) (float64, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"HINCRBYFLOAT", key, field, strconv.FormatFloat(increment, 'f', -1, 64)}),
		nil,
		false,
	)
	if err != nil {
		return 0, err
	}
	return internal.ParseFloatResponse(b)
}

func (server *EchoVault) HGETALL(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HGETALL", key}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) HEXISTS(key, field string) (bool, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HEXISTS", key, field}), nil, false)
	if err != nil {
		return false, err
	}
	return internal.ParseBooleanResponse(b)
}

func (server *EchoVault) HDEL(key string, fields ...string) (int, error) {
	cmd := append([]string{"HDEL", key}, fields...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}
