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

package sugardb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/echovault/sugardb/internal/clock"
	"github.com/echovault/sugardb/internal/constants"
	"github.com/tidwall/resp"
	"os"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSugarDB_AddCommand(t *testing.T) {
	type args struct {
		command CommandOptions
	}
	type scenarios struct {
		name    string
		command []string
		wantRes int
		wantErr error
	}
	tests := []struct {
		name      string
		args      args
		scenarios []scenarios
		wantErr   bool
	}{
		{
			name:    "1 Add command without subcommands",
			wantErr: false,
			args: args{
				command: CommandOptions{
					Command: "CommandOne",
					Module:  "test-module",
					Description: `(CommandOne write-key read-key <value>) 
Test command to handle successful addition of a single command without subcommands.
The value passed must be an integer.`,
					Categories: []string{},
					Sync:       false,
					KeyExtractionFunc: func(cmd []string) (CommandKeyExtractionFuncResult, error) {
						if len(cmd) != 4 {
							return CommandKeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
						}
						return CommandKeyExtractionFuncResult{
							WriteKeys: cmd[1:2],
							ReadKeys:  cmd[2:3],
						}, nil
					},
					HandlerFunc: func(params CommandHandlerFuncParams) ([]byte, error) {
						if len(params.Command) != 4 {
							return nil, errors.New(constants.WrongArgsResponse)
						}
						value := params.Command[3]
						i, err := strconv.ParseInt(value, 10, 64)
						if err != nil {
							return nil, errors.New("value must be an integer")
						}
						return []byte(fmt.Sprintf(":%d\r\n", i)), nil
					},
				},
			},
			scenarios: []scenarios{
				{
					name:    "1 Successfully execute the command and return the expected integer.",
					command: []string{"CommandOne", "write-key1", "read-key1", "1111"},
					wantRes: 1111,
					wantErr: nil,
				},
				{
					name:    "2 Get error due to command being too long",
					command: []string{"CommandOne", "write-key1", "read-key1", "1111", "2222"},
					wantRes: 0,
					wantErr: errors.New(constants.WrongArgsResponse),
				},
				{
					name:    "3 Get error due to command being too short",
					command: []string{"CommandOne", "write-key1", "read-key1"},
					wantRes: 0,
					wantErr: errors.New(constants.WrongArgsResponse),
				},
				{
					name:    "4 Get error due to value not being an integer",
					command: []string{"CommandOne", "write-key1", "read-key1", "string"},
					wantRes: 0,
					wantErr: errors.New("value must be an integer"),
				},
			},
		},
		{
			name:    "2 Add command with subcommands",
			wantErr: false,
			args: args{
				command: CommandOptions{
					Command: "CommandTwo",
					SubCommand: []SubCommandOptions{
						{
							Command: "SubCommandOne",
							Module:  "test-module",
							Description: `(CommandTwo SubCommandOne write-key read-key <value>) 
Test command to handle successful addition of a single command with subcommands.
The value passed must be an integer.`,
							Categories: []string{},
							Sync:       false,
							KeyExtractionFunc: func(cmd []string) (CommandKeyExtractionFuncResult, error) {
								if len(cmd) != 5 {
									return CommandKeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
								}
								return CommandKeyExtractionFuncResult{
									WriteKeys: cmd[2:3],
									ReadKeys:  cmd[3:4],
								}, nil
							},
							HandlerFunc: func(params CommandHandlerFuncParams) ([]byte, error) {
								if len(params.Command) != 5 {
									return nil, errors.New(constants.WrongArgsResponse)
								}
								value := params.Command[4]
								i, err := strconv.ParseInt(value, 10, 64)
								if err != nil {
									return nil, errors.New("value must be an integer")
								}
								return []byte(fmt.Sprintf(":%d\r\n", i)), nil
							},
						},
					},
				},
			},
			scenarios: []scenarios{
				{
					name:    "1 Successfully execute the command and return the expected integer.",
					command: []string{"CommandTwo", "SubCommandOne", "write-key1", "read-key1", "1111"},
					wantRes: 1111,
					wantErr: nil,
				},
				{
					name:    "2 Get error due to command being too long",
					command: []string{"CommandTwo", "SubCommandOne", "write-key1", "read-key1", "1111", "2222"},
					wantRes: 0,
					wantErr: errors.New(constants.WrongArgsResponse),
				},
				{
					name:    "3 Get error due to command being too short",
					command: []string{"CommandTwo", "SubCommandOne", "write-key1", "read-key1"},
					wantRes: 0,
					wantErr: errors.New(constants.WrongArgsResponse),
				},
				{
					name:    "4 Get error due to value not being an integer",
					command: []string{"CommandTwo", "SubCommandOne", "write-key1", "read-key1", "string"},
					wantRes: 0,
					wantErr: errors.New("value must be an integer"),
				},
			},
		},
	}
	for _, tt := range tests {
		server := createSugarDB()
		t.Run(tt.name, func(t *testing.T) {
			if err := server.AddCommand(tt.args.command); (err != nil) != tt.wantErr {
				t.Errorf("AddCommand() error = %v, wantErr %v", err, tt.wantErr)
			}
			for _, scenario := range tt.scenarios {
				b, err := server.ExecuteCommand(scenario.command...)
				if scenario.wantErr != nil {
					if scenario.wantErr.Error() != err.Error() {
						t.Errorf("AddCommand() error = %v, wantErr %v", err, scenario.wantErr)
					}
					continue
				}
				r := resp.NewReader(bytes.NewReader(b))
				v, _, _ := r.ReadValue()
				if v.Integer() != scenario.wantRes {
					t.Errorf("AddCommand() res = %v, wantRes %v", resp.BytesValue(b).Integer(), scenario.wantRes)
				}
			}
		})
	}
}

func TestSugarDB_ExecuteCommand(t *testing.T) {
	type args struct {
		key         string
		presetValue []string
		command     []string
	}
	tests := []struct {
		name    string
		args    args
		wantRes int
		wantErr error
	}{
		{
			name: "1 Execute LPUSH command and get expected result",
			args: args{
				key:         "key1",
				presetValue: []string{"1", "2", "3"},
				command:     []string{"LPUSH", "key1", "4", "5", "6", "7", "8", "9", "10"},
			},
			wantRes: 10,
			wantErr: nil,
		},
		{
			name: "2 Expect error when trying to execute non-existent command",
			args: args{
				key:         "key2",
				presetValue: nil,
				command:     []string{"NON-EXISTENT", "key1", "key2"},
			},
			wantRes: 0,
			wantErr: errors.New("command NON-EXISTENT not supported"),
		},
	}
	for _, tt := range tests {
		server := createSugarDB()
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.presetValue != nil {
				_, _ = server.LPush(tt.args.key, tt.args.presetValue...)
			}
			b, err := server.ExecuteCommand(tt.args.command...)
			if tt.wantErr != nil {
				if err.Error() != tt.wantErr.Error() {
					t.Errorf("ExecuteCommand() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
			r := resp.NewReader(bytes.NewReader(b))
			v, _, _ := r.ReadValue()
			if v.Integer() != tt.wantRes {
				t.Errorf("ExecuteCommand() response = %d, wantRes %d", v.Integer(), tt.wantRes)
			}
		})
	}
}

func TestSugarDB_RemoveCommand(t *testing.T) {
	type args struct {
		removeCommand  []string
		executeCommand []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "1 Remove command and expect error when the command is called",
			args: args{
				removeCommand:  []string{"LPUSH"},
				executeCommand: []string{"LPUSH", "key", "item"},
			},
			wantErr: errors.New("command LPUSH not supported"),
		},
		{
			name: "2 Remove sub-command and expect error when the subcommand is called",
			args: args{
				removeCommand:  []string{"ACL", "CAT"},
				executeCommand: []string{"ACL", "CAT"},
			},
			wantErr: errors.New("command ACL CAT not supported"),
		},
		{
			name: "3 Remove sub-command and expect successful response from calling another subcommand",
			args: args{
				removeCommand:  []string{"ACL", "WHOAMI"},
				executeCommand: []string{"ACL", "DELUSER", "user-one"},
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		server := createSugarDB()
		t.Run(tt.name, func(t *testing.T) {
			server.RemoveCommand(tt.args.removeCommand...)
			_, err := server.ExecuteCommand(tt.args.executeCommand...)
			if tt.wantErr != nil {
				if err.Error() != tt.wantErr.Error() {
					t.Errorf("RemoveCommand() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func TestSugarDB_Plugins(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll("./testdata/modules")
	})

	server := createSugarDB()

	moduleSet := path.Join(".", "testdata", "modules", "module_set", "module_set.so")
	moduleGet := path.Join(".", "testdata", "modules", "module_get", "module_get.so")
	nonExistent := path.Join(".", "testdata", "modules", "non_existent", "module_non_existent.so")

	// Load module.set module
	if err := server.LoadModule(moduleSet); err != nil {
		t.Error(err)
	}
	// Execute module.set command and expect "OK" response
	res, err := server.ExecuteCommand("module.set", "key1", "15")
	if err != nil {
		t.Error(err)
	}
	rv, _, err := resp.NewReader(bytes.NewReader(res)).ReadValue()
	if err != nil {
		t.Error(err)
	}
	if rv.String() != "OK" {
		t.Errorf("expected response \"OK\", got \"%s\"", rv.String())
	}

	// Load module.get module with args
	if err := server.LoadModule(moduleGet, "10"); err != nil {
		t.Error(err)
	}
	// Execute module.get command and expect an integer with the value 150
	res, err = server.ExecuteCommand("module.get", "key1")
	rv, _, err = resp.NewReader(bytes.NewReader(res)).ReadValue()
	if err != nil {
		t.Error(err)
	}
	if rv.Integer() != 150 {
		t.Errorf("expected response 150, got %d", rv.Integer())
	}

	// Return error when trying to load module that does not exist
	if err := server.LoadModule(nonExistent); err == nil {
		t.Error("expected error but got nil instead")
	} else {
		if err.Error() != fmt.Sprintf("load module: module %s not found", nonExistent) {
			t.Errorf(
				"expected error \"%s\", got \"%s\"",
				fmt.Sprintf("load module: module %s not found", nonExistent),
				err.Error(),
			)
		}
	}

	// Module list should contain module_get and module_set modules
	modules := server.ListModules()
	for _, mod := range []string{moduleSet, moduleGet} {
		if !slices.Contains(modules, mod) {
			t.Errorf("expected modules list to contain module \"%s\" but did not find it", mod)
		}
	}

	// Unload modules
	server.UnloadModule(moduleSet)
	server.UnloadModule(moduleGet)

	// Make sure the modules are no longer loaded
	modules = server.ListModules()
	for _, mod := range []string{moduleSet, moduleGet} {
		if slices.Contains(modules, mod) {
			t.Errorf("expected modules list to not contain module \"%s\" but found it", mod)
		}
	}
}

func TestSugarDB_CommandList(t *testing.T) {
	server := createSugarDB()

	tests := []struct {
		name    string
		options interface{}
		want    []string
		wantErr bool
	}{
		{
			name:    "1. Get all present commands when no options are passed",
			options: nil,
			want: func() []string {
				var commands []string
				for _, command := range server.commands {
					if command.SubCommands == nil || len(command.SubCommands) == 0 {
						commands = append(commands, strings.ToLower(command.Command))
						continue
					}
					for _, subcommand := range command.SubCommands {
						commands = append(commands, strings.ToLower(fmt.Sprintf("%s %s", command.Command, subcommand.Command)))
					}
				}
				return commands
			}(),
			wantErr: false,
		},
		{
			name:    "2. Get commands filtered by hash ACL category",
			options: CommandListOptions{ACLCAT: constants.HashCategory},
			want: func() []string {
				var commands []string
				for _, command := range server.commands {
					if slices.Contains(command.Categories, constants.HashCategory) {
						commands = append(commands, strings.ToLower(command.Command))
					}
				}
				return commands
			}(),
			wantErr: false,
		},
		{
			name:    "3. Get commands filtered by pattern",
			options: CommandListOptions{PATTERN: "z*"},
			want: func() []string {
				var commands []string
				for _, command := range server.commands {
					if strings.EqualFold(command.Module, constants.SortedSetModule) {
						commands = append(commands, strings.ToLower(command.Command))
					}
				}
				return commands
			}(),
			wantErr: false,
		},
		{
			name:    "4. Get commands filtered by module",
			options: CommandListOptions{MODULE: constants.ListModule},
			want: func() []string {
				var commands []string
				for _, command := range server.commands {
					if strings.EqualFold(command.Module, constants.ListModule) {
						commands = append(commands, strings.ToLower(command.Command))
					}
				}
				return commands
			}(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			var err error
			if tt.options == nil {
				got, err = server.CommandList()
			} else {
				got, err = server.CommandList(tt.options.(CommandListOptions))
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("CommandList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CommandList() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSugarDB_CommandCount(t *testing.T) {
	server := createSugarDB()

	tests := []struct {
		name    string
		want    int
		wantErr bool
	}{
		{
			name: "1. Get the count of all commands/subcommands on the server",
			want: func() int {
				var commands []string
				for _, command := range server.commands {
					if command.SubCommands == nil || len(command.SubCommands) == 0 {
						commands = append(commands, strings.ToLower(command.Command))
						continue
					}
					for _, subcommand := range command.SubCommands {
						commands = append(commands, strings.ToLower(fmt.Sprintf("%s %s", command.Command, subcommand.Command)))
					}
				}
				return len(commands)
			}(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := server.CommandCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("CommandCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CommandCount() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSugarDB_Save(t *testing.T) {
	conf := DefaultConfig()
	conf.DataDir = path.Join(".", "testdata", "data")
	conf.EvictionPolicy = constants.NoEviction
	server := createSugarDBWithConfig(conf)

	tests := []struct {
		name    string
		want    bool
		wantErr bool
	}{
		{
			name:    "1. Return true response when save process is started",
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := server.Save()
			if (err != nil) != tt.wantErr {
				t.Errorf("Save() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Save() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSugarDB_LastSave(t *testing.T) {
	server := createSugarDB()
	server.setLatestSnapshot(clock.NewClock().Now().Add(5 * time.Minute).UnixMilli())

	tests := []struct {
		name    string
		want    int
		wantErr bool
	}{
		{
			name:    "1. Get latest snapshot time milliseconds",
			want:    int(clock.NewClock().Now().Add(5 * time.Minute).UnixMilli()),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := server.LastSave()
			if (err != nil) != tt.wantErr {
				t.Errorf("LastSave() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LastSave() got = %v, want %v", got, tt.want)
			}
		})
	}
}
