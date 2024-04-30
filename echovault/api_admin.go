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
	"context"
	"fmt"
	"github.com/echovault/echovault/internal"
	"net"
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

// CommandKeyExtractionFuncResult specifies the keys accessed by the associated command or subcommand.
// ReadKeys is a string slice containing the keys that the commands read from.
// WriteKeys is a string slice containing the keys that the command writes to.
//
// These keys will typically be extracted from the command slice, but they can also be hardcoded.
type CommandKeyExtractionFuncResult struct {
	ReadKeys  []string
	WriteKeys []string
}

// CommandKeyExtractionFunc if the function that extracts the keys accessed by the command or subcommand.
type CommandKeyExtractionFunc func(cmd []string) (CommandKeyExtractionFuncResult, error)

// CommandHandlerFunc is the handler function for the command or subcommand.
//
// This function must return a byte slice containing a valid RESP2 response, or an error.
type CommandHandlerFunc func(params CommandHandlerFuncParams) ([]byte, error)

// CommandHandlerFuncParams contains the helper parameters passed to the command's handler by EchoVault.
//
// Command is the string slice command containing the command that triggered this handler.
//
// Connection is the TCP connection that triggered this command. In embedded mode, this will always be nil.
// Any TCP client that trigger the custom command will have its connection passed to the handler here.
//
// KeyExists returns true if the key passed to it exists in the store.
//
// CreateKeyAndLock creates the new key and immediately write locks it. If the key already exists, then
// it is simply write locked which makes this function safe to call even if the key already exists. Always call
// KeyUnlock when done after CreateKeyAndLock.
//
// KeyLock acquires a write lock for the specified key. If the lock is successfully acquired, the function will return
// (true, nil). Otherwise, it will return false and an error describing why the locking failed. Always call KeyUnlock
// when done after KeyLock.
//
// KeyUnlock releases the write lock for the specified key. Always call this after KeyLock otherwise the key will not be
// lockable by any future invocations of this command or other commands.
//
// KeyRLock acquires a read lock for the specified key. If the lock is successfully acquired, the function will return
// (true, nil). Otherwise, it will return false and an error describing why the locking failed. Always call KeyRUnlock
// when done after KeyRLock.
//
// KeyRUnlock releases the real lock for the specified key. Always call this after KeyRLock otherwise the key will not be
// write-lockable by any future invocations of this command or other commands.
//
// GetValue returns the value held at the specified key as an interface{}. Make sure to invoke KeyLock or KeyRLock on the
// key before GetValue to ensure thread safety.
//
// SetValue sets the value at the specified key. Make sure to invoke KeyLock on the key before
// SetValue to ensure thread safety.
type CommandHandlerFuncParams struct {
	Context          context.Context
	Command          []string
	Connection       *net.Conn
	KeyExists        func(ctx context.Context, key string) bool
	CreateKeyAndLock func(ctx context.Context, key string) (bool, error)
	KeyLock          func(ctx context.Context, key string) (bool, error)
	KeyUnlock        func(ctx context.Context, key string)
	KeyRLock         func(ctx context.Context, key string) (bool, error)
	KeyRUnlock       func(ctx context.Context, key string)
	GetValue         func(ctx context.Context, key string) interface{}
	SetValue         func(ctx context.Context, key string, value interface{}) error
}

// CommandOptions provides the specification of the command to be added to the EchoVault instance.
//
// Command is the keyword used to trigger this command (e.g. LPUSH, ZADD, ACL ...).
//
// Module is a string that classifies a group of commands.
//
// Categories is a string slice of all the categories that this command belongs to.
//
// Description is a string describing the command, can include an example of how to trigger the command.
//
// SubCommand is a slice of subcommands for this command.
//
// Sync is a boolean value that determines whether this command should be synced across a replication cluster.
// If subcommands are specified, each subcommand will override this value for its own execution.
//
// KeyExtractionFunc is a function that extracts the keys from the command if the command accesses any keys.
// the extracted keys are used by the ACL layer to determine whether a TCP client is authorized to execute this command.
// If subcommands are specified, this function is discarded and each subcommands must implement its own KeyExtractionFunc.
//
// HandlerFunc is the command handler. This function must return a valid RESP2 response as it the command will be
// available to RESP clients. If subcommands are specified, this function is discarded and each subcommand must implement
// its own HandlerFunc.
type CommandOptions struct {
	Command           string
	Module            string
	Categories        []string
	Description       string
	SubCommand        []SubCommandOptions
	Sync              bool
	KeyExtractionFunc CommandKeyExtractionFunc
	HandlerFunc       CommandHandlerFunc
}

// SubCommandOptions provides the specification of a subcommand within CommandOptions.
//
// Command is the keyword used to trigger this subcommand (e.g. "CAT" for the subcommand "ACL CAT").
//
// Module is a string that classifies a group of commands/subcommands.
//
// Categories is a string slice of all the categories that this subcommand belongs to.
//
// Description is a string describing the subcommand, can include an example of how to trigger the subcommand.
//
// Sync is a boolean value that determines whether this subcommand should be synced across a replication cluster.
// This value overrides the Sync value set by the parent command. It's possible to have some synced and un-synced
// subcommands with the same parent command regardless of the parent's Sync value.
//
// KeyExtractionFunc is a function that extracts the keys from the subcommand if it accesses any keys.
//
// HandlerFunc is the subcommand handler. This function must return a valid RESP2 response as it will be
// available to RESP clients.
type SubCommandOptions struct {
	Command           string
	Module            string
	Categories        []string
	Description       string
	Sync              bool
	KeyExtractionFunc CommandKeyExtractionFunc
	HandlerFunc       CommandHandlerFunc
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

// AddCommand adds a new command to EchoVault. The added command can be executed using the ExecuteCommand method.
//
// Parameters:
//
// `command` - CommandOptions.
//
// Errors:
//
// "command <command> already exists" - If a command with the same command name as the passed command already exists.
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
				return command.HandlerFunc(CommandHandlerFuncParams{
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
				return sc.HandlerFunc(CommandHandlerFuncParams{
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
				})
			}),
		}
	}

	server.commands = append(server.commands, newCommand)

	return nil
}

// ExecuteCommand executes the command passed to it. If 1 string is passed, EchoVault will try to
// execute the command. If 2 strings are passed, EchoVault will attempt to execute the subcommand of the command.
// If more than 2 strings are provided, all additional strings will be ignored.
//
// This method returns the raw RESP response from the command handler. You will have to parse the RESP response if
// you want to use the return value from the handler.
//
// This method does not work with handlers that manipulate the client connection directly (i.e SUBSCRIBE, PSUBSCRIBE).
// If you'd like to (p)subscribe or (p)unsubscribe, use the (P)SUBSCRIBE and (P)UNSUBSCRIBE methods instead.
//
// Parameters:
//
// `command` - ...string.
//
// Returns: []byte - Raw RESP response returned by the command handler.
//
// Errors:
//
// All errors from the command handler are forwarded to the caller. Other errors returned include:
//
// "command <command> not supported" - If the command does not exist.
//
// "command <command> <subcommand> not supported" - If the command exists but the subcommand does not exist for that command.
func (server *EchoVault) ExecuteCommand(command ...string) ([]byte, error) {
	return server.handleCommand(server.context, internal.EncodeCommand(command), nil, false, true)
}

// RemoveCommand removes the specified command or subcommand from EchoVault.
// When commands are removed, they will no longer be available for both the embedded instance and for TCP clients.
//
// Note: If a command is removed, the API wrapper for the command will also be unusable.
// For example, calling RemoveCommand("LPUSH") will cause the LPUSH method to always return a
// "command LPUSH not supported" error so use this method with caution.
//
// If one string is passed, the command matching that string is removed along will all of its subcommand if it has any.
// If two strings are passed, only the subcommand of the specified command is removed.
// If more than 2 strings are passed, all additional strings are ignored.
//
// Parameters:
//
// `command` - ...string.
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
