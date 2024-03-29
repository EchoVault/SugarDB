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
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/tidwall/resp"
)

type ACLLOADOptions struct {
	Merge   bool
	Replace bool
}

type SETUSEROptions struct {
	Username      string
	Enabled       bool
	NoPassword    bool
	NoKeys        bool
	NoCommands    bool
	ResetPass     bool
	ResetKeys     bool
	ResetChannels bool

	AddPlainPasswords    []string
	RemovePlainPasswords []string
	AddHashPasswords     []string
	RemoveHashPasswords  []string

	IncludeCategories []string
	ExcludeCategories []string

	IncludeCommands []string
	ExcludeCommands []string

	IncludeReadWriteKeys []string
	IncludeReadKeys      []string
	IncludeWriteKeys     []string

	IncludeChannels []string
	ExcludeChannels []string
}

func (server *EchoVault) ACL_CAT(category ...string) ([]string, error) {
	cmd := []string{"ACL", "CAT"}
	if len(category) > 0 {
		cmd = append(cmd, category[0])
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) ACL_USERS() ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "USERS"}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) ACL_SETUSER(options SETUSEROptions) (string, error) {
	cmd := []string{"ACL", "SETUSER", options.Username}

	if options.Enabled {
		cmd = append(cmd, "on")
	} else {
		cmd = append(cmd, "off")
	}

	if options.NoPassword {
		cmd = append(cmd, "nopass")
	}

	if options.NoKeys {
		cmd = append(cmd, "nokeys")
	}

	if options.NoCommands {
		cmd = append(cmd, "nocommands")
	}

	if options.ResetPass {
		cmd = append(cmd, "resetpass")
	}

	if options.ResetKeys {
		cmd = append(cmd, "resetkeys")
	}

	if options.ResetChannels {
		cmd = append(cmd, "resetchannels")
	}

	for _, password := range options.AddPlainPasswords {
		cmd = append(cmd, fmt.Sprintf(">%s", password))
	}

	for _, password := range options.RemovePlainPasswords {
		cmd = append(cmd, fmt.Sprintf("<%s", password))
	}

	for _, password := range options.AddHashPasswords {
		cmd = append(cmd, fmt.Sprintf("#%s", password))
	}

	for _, password := range options.RemoveHashPasswords {
		cmd = append(cmd, fmt.Sprintf("!%s", password))
	}

	for _, category := range options.IncludeCategories {
		cmd = append(cmd, fmt.Sprintf("+@%s", category))
	}

	for _, category := range options.ExcludeCategories {
		cmd = append(cmd, fmt.Sprintf("-@%s", category))
	}

	for _, command := range options.IncludeCommands {
		cmd = append(cmd, fmt.Sprintf("+%s", command))
	}

	for _, command := range options.ExcludeCommands {
		cmd = append(cmd, fmt.Sprintf("-%s", command))
	}

	for _, key := range options.IncludeReadWriteKeys {
		cmd = append(cmd, fmt.Sprintf("%s~%s", "%RW", key))
	}

	for _, key := range options.IncludeReadKeys {
		cmd = append(cmd, fmt.Sprintf("%s~%s", "%R", key))
	}

	for _, key := range options.IncludeWriteKeys {
		cmd = append(cmd, fmt.Sprintf("%s~%s", "%W", key))
	}

	for _, channel := range options.IncludeChannels {
		cmd = append(cmd, fmt.Sprintf("+&%s", channel))
	}

	for _, channel := range options.ExcludeChannels {
		cmd = append(cmd, fmt.Sprintf("-&%s", channel))
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

func (server *EchoVault) ACL_GETUSER(username string) (map[string][]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "GETUSER", username}), nil, false)
	if err != nil {
		return nil, err
	}

	r := resp.NewReader(bytes.NewReader(b))
	v, _, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	arr := v.Array()
	result := make(map[string][]string, len(arr)/2)

	for i := 0; i < len(arr); i += 2 {
		key := arr[i].String()
		value := arr[i+1].Array()

		result[key] = make([]string, len(value))

		for j := 0; j < len(value); j++ {
			result[key][i] = value[i].String()
		}
	}

	return result, nil
}

func (server *EchoVault) ACL_DELUSER(usernames ...string) (string, error) {
	cmd := append([]string{"ACL", "DELUSER"}, usernames...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

func (server *EchoVault) ACL_LIST() ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "LIST"}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

func (server *EchoVault) ACL_LOAD(options ACLLOADOptions) (string, error) {
	cmd := []string{"ACL", "LOAD"}
	switch {
	case options.Merge:
		cmd = append(cmd, "MERGE")
	case options.Replace:
		cmd = append(cmd, "REPLACE")
	default:
		cmd = append(cmd, "REPLACE")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

func (server *EchoVault) ACL_SAVE() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "SAVE"}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
