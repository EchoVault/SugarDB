// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");s
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

import "github.com/echovault/echovault/internal"

type CommandListOptions struct {
	ACLCAT  string
	PATTERN string
	MODULE  string
}

func (server *EchoVault) COMMAND_LIST(options CommandListOptions) ([]string, error) {
	cmd := []string{"COMMAND", "LIST"}

	switch {
	case options.ACLCAT != "":
		cmd = append(cmd, []string{"FILTERBY", "ACLCAT", options.ACLCAT}...)
	case options.PATTERN != "":
		cmd = append(cmd, []string{"FILTERBY", "PATTERN", options.PATTERN}...)
	case options.MODULE != "":
		cmd = append(cmd, []string{"FILTERBY", "MODULE", options.MODULE}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) COMMAND_COUNT() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"COMMAND", "COUNT"}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) SAVE() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SAVE"}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) LASTSAVE() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LASTSAVE"}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) REWRITEAOF() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"REWRITEAOF"}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
