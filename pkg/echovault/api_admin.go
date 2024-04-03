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

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	return internal.ParseStringArrayResponse(b)
}

// COMMAND_COUNT returns the number of commands currently loaded in the EchoVault instance.
//
// Returns: integer representing the count of all available commands.
func (server *EchoVault) COMMAND_COUNT() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"COMMAND", "COUNT"}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SAVE triggers a new snapshot.
func (server *EchoVault) SAVE() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SAVE"}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LASTSAVE returns the unix epoch milliseconds timestamp of the last save.
func (server *EchoVault) LASTSAVE() (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LASTSAVE"}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// REWRITEAOF triggers a compaction of the AOF file.
func (server *EchoVault) REWRITEAOF() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"REWRITEAOF"}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
