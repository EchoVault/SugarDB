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

func (server *EchoVault) SETRANGE(key string, offset int, new string) (int, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SETRANGE", key, strconv.Itoa(offset), new}),
		nil,
		false,
	)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) STRLEN(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"STRLEN", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SUBSTR(key string, start, end int) (string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SUBSTR", key, strconv.Itoa(start), strconv.Itoa(end)}),
		nil,
		false,
	)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) GETRANGE(key string, start, end int) (string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"GETRANGE", key, strconv.Itoa(start), strconv.Itoa(end)}),
		nil,
		false,
	)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
