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

import (
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/pkg/types"
	"strings"
)

// CommandListOptions modifies the result from the COMMAND_LIST command.
//
// ACLCAT filters the results by the provided category. Has the highest priority.
//
// PATTERN filters the result that match the given glob pattern. Has the second-highest priority.
//
// MODULE filters the result by the provided module. Has the lowest priority.
type CommandListOptions struct {
	ACLCAT  string
	PATTERN string
	MODULE  string
}

// TODO: Write godoc comment for CommandOptions type
type CommandOptions struct {
	Command           string
	Module            string
	Categories        []string
	Description       string
	Sync              bool
	KeyExtractionFunc types.PluginKeyExtractionFunc
	HandlerFunc       types.PluginHandlerFunc
}

// COMMAND_LIST returns the list of commands currently loaded in the EchoVault instance.
//
// Parameters:
//
// `options` - CommandListOptions.
//
// Returns: a string slice of all the loaded commands. SubCommands are represented as "command|subcommand".
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

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	return internal.ParseStringArrayResponse(b)
}

// COMMAND_COUNT returns the number of commands currently loaded in the EchoVault instance.
//
// Returns: integer representing the count of all available commands.
func (server *EchoVault) COMMAND_COUNT() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"COMMAND", "COUNT"}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SAVE triggers a new snapshot.
func (server *EchoVault) SAVE() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SAVE"}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LASTSAVE returns the unix epoch milliseconds timestamp of the last save.
func (server *EchoVault) LASTSAVE() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LASTSAVE"}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// REWRITEAOF triggers a compaction of the AOF file.
func (server *EchoVault) REWRITEAOF() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"REWRITEAOF"}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// TODO: Write godoc comment for ADD_COMMAND method
func (server *EchoVault) ADD_COMMAND(command CommandOptions) error {
	// Check if commands already exists
	for _, c := range server.commands {
		if strings.EqualFold(c.Command, command.Command) {
			return fmt.Errorf("command %s already exists", command.Command)
		}
	}
	server.commands = append(server.commands, internal.Command{
		Command: command.Command,
		Module:  strings.ToLower(command.Module), // Convert module to lower case for uniformity
		Categories: func() []string {
			// Convert all the categories to lower case for uniformity
			cats := make([]string, len(command.Categories))
			for i, cat := range command.Categories {
				cats[i] = strings.ToLower(cat)
			}
			return cats
		}(),
		Description: command.Description,
		Sync:        command.Sync,
		KeyExtractionFunc: internal.KeyExtractionFunc(func(cmd []string) (internal.AccessKeys, error) {
			accessKeys, err := command.KeyExtractionFunc(cmd)
			if err != nil {
				return internal.AccessKeys{}, err
			}
			return internal.AccessKeys{
				Channels:  []string{},
				ReadKeys:  accessKeys.ReadKeys,
				WriteKeys: accessKeys.WriteKeys,
			}, nil
		}),
		HandlerFunc: internal.HandlerFunc(func(params internal.HandlerFuncParams) ([]byte, error) {
			return command.HandlerFunc(types.PluginHandlerFuncParams{
				Context:          params.Context,
				Command:          params.Command,
				Connection:       params.Connection,
				KeyLock:          params.KeyLock,
				KeyUnlock:        params.KeyUnlock,
				KeyRLock:         params.KeyRLock,
				KeyRUnlock:       params.KeyRUnlock,
				KeyExists:        params.KeyExists,
				CreateKeyAndLock: params.CreateKeyAndLock,
				GetValue:         params.GetValue,
				SetValue:         params.SetValue,
				GetExpiry:        params.GetExpiry,
				SetExpiry:        params.SetExpiry,
				RemoveExpiry:     params.RemoveExpiry,
				DeleteKey:        params.DeleteKey,
			})
		}),
	})
	return nil
}

// TODO: Write godoc comment for EXECUTE_COMMAND method
func (server *EchoVault) EXECUTE_COMMAND(command []string) ([]byte, error) {
	return server.handleCommand(server.context, internal.EncodeCommand(command), nil, false, true)
}
