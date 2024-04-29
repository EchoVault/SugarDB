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
	"github.com/echovault/echovault/types"
	"slices"
	"strings"
)

// CommandListOptions modifies the result from the CommandList command.
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
	SubCommand        []SubCommandOptions
	Sync              bool
	KeyExtractionFunc types.PluginKeyExtractionFunc
	HandlerFunc       types.PluginHandlerFunc
}

// TODO: Write godoc comment for SubCommandOptions type
type SubCommandOptions struct {
	Command           string
	Module            string
	Categories        []string
	Description       string
	Sync              bool
	KeyExtractionFunc types.PluginKeyExtractionFunc
	HandlerFunc       types.PluginHandlerFunc
}

// CommandList returns the list of commands currently loaded in the EchoVault instance.
//
// Parameters:
//
// `options` - CommandListOptions.
//
// Returns: a string slice of all the loaded commands. SubCommands are represented as "command|subcommand".
func (server *EchoVault) CommandList(options CommandListOptions) ([]string, error) {
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

// CommandCount returns the number of commands currently loaded in the EchoVault instance.
//
// Returns: integer representing the count of all available commands.
func (server *EchoVault) CommandCount() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"COMMAND", "COUNT"}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// Save triggers a new snapshot.
func (server *EchoVault) Save() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SAVE"}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LastSave returns the unix epoch milliseconds timestamp of the last save.
func (server *EchoVault) LastSave() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LASTSAVE"}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// RewriteAOF triggers a compaction of the AOF file.
func (server *EchoVault) RewriteAOF() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"REWRITEAOF"}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// TODO: Write godoc comment for AddCommand method
func (server *EchoVault) AddCommand(command CommandOptions) error {
	// Check if command already exists
	for _, c := range server.commands {
		if strings.EqualFold(c.Command, command.Command) {
			return fmt.Errorf("command %s already exists", command.Command)
		}
	}

	if command.SubCommand == nil || len(command.SubCommand) == 0 {
		// Add command with no subcommands
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
			KeyExtractionFunc: internal.KeyExtractionFunc(func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				accessKeys, err := command.KeyExtractionFunc(cmd)
				if err != nil {
					return internal.KeyExtractionFuncResult{}, err
				}
				return internal.KeyExtractionFuncResult{
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

	// Add command with subcommands
	newCommand := internal.Command{
		Command: command.Command,
		Module:  command.Module,
		Categories: func() []string {
			// Convert all the categories to lower case for uniformity
			cats := make([]string, len(command.Categories))
			for j, cat := range command.Categories {
				cats[j] = strings.ToLower(cat)
			}
			return cats
		}(),
		Description: command.Description,
		Sync:        command.Sync,
		KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
			return internal.KeyExtractionFuncResult{}, nil
		},
		HandlerFunc: func(param internal.HandlerFuncParams) ([]byte, error) { return nil, nil },
		SubCommands: make([]internal.SubCommand, len(command.SubCommand)),
	}

	for i, sc := range command.SubCommand {
		// Skip the subcommand if it already exists in newCommand
		if slices.ContainsFunc(newCommand.SubCommands, func(subcommand internal.SubCommand) bool {
			return strings.EqualFold(subcommand.Command, sc.Command)
		}) {
			continue
		}
		newCommand.SubCommands[i] = internal.SubCommand{
			Command: sc.Command,
			Module:  strings.ToLower(command.Module),
			Categories: func() []string {
				// Convert all the categories to lower case for uniformity
				cats := make([]string, len(sc.Categories))
				for j, cat := range sc.Categories {
					cats[j] = strings.ToLower(cat)
				}
				return cats
			}(),
			Description: sc.Description,
			Sync:        sc.Sync,
			KeyExtractionFunc: internal.KeyExtractionFunc(func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				accessKeys, err := sc.KeyExtractionFunc(cmd)
				if err != nil {
					return internal.KeyExtractionFuncResult{}, err
				}
				return internal.KeyExtractionFuncResult{
					Channels:  []string{},
					ReadKeys:  accessKeys.ReadKeys,
					WriteKeys: accessKeys.WriteKeys,
				}, nil
			}),
			HandlerFunc: internal.HandlerFunc(func(params internal.HandlerFuncParams) ([]byte, error) {
				return sc.HandlerFunc(types.PluginHandlerFuncParams{
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
		}
	}

	server.commands = append(server.commands, newCommand)

	return nil
}

// TODO: Write godoc comment for ExecuteCommand method
func (server *EchoVault) ExecuteCommand(command []string) ([]byte, error) {
	return server.handleCommand(server.context, internal.EncodeCommand(command), nil, false, true)
}

// TODO: Write godoc commend for RemoveCommand method
func (server *EchoVault) RemoveCommand(command ...string) {
	switch len(command) {
	case 1:
		// Remove command
		server.commands = slices.DeleteFunc(server.commands, func(c internal.Command) bool {
			return strings.EqualFold(c.Command, command[0])
		})
	case 2:
		// Remove subcommand
		for i := 0; i < len(server.commands); i++ {
			if !strings.EqualFold(server.commands[i].Command, command[0]) {
				continue
			}
			if server.commands[i].SubCommands != nil && len(server.commands[i].SubCommands) > 0 {
				server.commands[i].SubCommands = slices.DeleteFunc(server.commands[i].SubCommands, func(sc internal.SubCommand) bool {
					return strings.EqualFold(sc.Command, command[1])
				})
			}
		}
	}
}
