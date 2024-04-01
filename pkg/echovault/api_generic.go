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

type SETOptions struct {
	NX   bool
	XX   bool
	LT   bool
	GT   bool
	GET  bool
	EX   int
	PX   int
	EXAT int
	PXAT int
}

type EXPIREOptions struct {
	NX bool
	XX bool
	LT bool
	GT bool
}

type PEXPIREOptions EXPIREOptions

type EXPIREATOptions EXPIREOptions

type PEXPIREATOptions EXPIREOptions

func (server *EchoVault) SET(key, value string, options SETOptions) (string, error) {
	cmd := []string{"SET", key, value}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	}

	switch {
	case options.EX != 0:
		cmd = append(cmd, []string{"EX", strconv.Itoa(options.EX)}...)
	case options.PX != 0:
		cmd = append(cmd, []string{"PX", strconv.Itoa(options.PX)}...)
	case options.EXAT != 0:
		cmd = append(cmd, []string{"EXAT", strconv.Itoa(options.EXAT)}...)
	case options.PXAT != 0:
		cmd = append(cmd, []string{"PXAT", strconv.Itoa(options.PXAT)}...)
	}

	if options.GET {
		cmd = append(cmd, "GET")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

func (server *EchoVault) MSET(kvPairs map[string]string) (string, error) {
	cmd := []string{"MSET"}

	for k, v := range kvPairs {
		cmd = append(cmd, []string{k, v}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

func (server *EchoVault) GET(key string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"GET", key}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) MGET(keys ...string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand(append([]string{"MGET"}, keys...)), nil, false)
	if err != nil {
		return []string{}, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) DEL(keys ...string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand(append([]string{"DEL"}, keys...)), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) PERSIST(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PERSIST", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) EXPIRETIME(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"EXPIRETIME", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) PEXPIRETIME(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PEXPIRETIME", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) TTL(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"TTL", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) PTTL(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PTTL", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) EXPIRE(key string, seconds int, options EXPIREOptions) (int, error) {
	cmd := []string{"EXPIRE", key, strconv.Itoa(seconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) PEXPIRE(key string, milliseconds int, options PEXPIREOptions) (int, error) {
	cmd := []string{"PEXPIRE", key, strconv.Itoa(milliseconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) EXPIREAT(key string, unixSeconds int, options EXPIREATOptions) (int, error) {
	cmd := []string{"EXPIREAT", key, strconv.Itoa(unixSeconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) PEXPIREAT(key string, unixMilliseconds int, options PEXPIREATOptions) (int, error) {
	cmd := []string{"PEXPIREAT", key, strconv.Itoa(unixMilliseconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}
