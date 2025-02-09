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

func TestSugarDB_Admin(t *testing.T) {
	t.Run("TestSugarDB_AddCommand", func(t *testing.T) {
		t.Parallel()

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
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				server := createSugarDB()
				t.Cleanup(func() {
					server.ShutDown()
				})

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
	})

	t.Run("TestSugarDB_ExecuteCommand", func(t *testing.T) {
		t.Parallel()

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
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				server := createSugarDB()
				t.Cleanup(func() {
					server.ShutDown()
				})
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
	})

	t.Run("TestSugarDB_RemoveCommand", func(t *testing.T) {
		t.Parallel()

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
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				server := createSugarDB()
				t.Cleanup(func() {
					server.ShutDown()
				})

				server.RemoveCommand(tt.args.removeCommand...)
				_, err := server.ExecuteCommand(tt.args.executeCommand...)
				if tt.wantErr != nil {
					if err.Error() != tt.wantErr.Error() {
						t.Errorf("RemoveCommand() error = %v, wantErr %v", err, tt.wantErr)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_Plugins", func(t *testing.T) {
		t.Parallel()

		t.Cleanup(func() {
			_ = os.RemoveAll("./testdata/modules")
		})

		server := createSugarDB()
		t.Cleanup(func() {
			server.ShutDown()
		})

		tests := []struct {
			name    string
			path    string
			expect  bool
			args    []string
			cmd     []string
			want    string
			wantErr error
		}{
			{
				name:    "1. Test shared object plugin MODULE.SET",
				path:    path.Join(".", "testdata", "modules", "module_set", "module_set.so"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"MODULE.SET", "key1", "15"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "2. Test shared object plugin MODULE.GET",
				path:    path.Join(".", "testdata", "modules", "module_get", "module_get.so"),
				expect:  true,
				args:    []string{"10"},
				cmd:     []string{"MODULE.GET", "key1"},
				want:    "150",
				wantErr: nil,
			},
			{
				name:   "3. Test Non existent module.",
				path:   path.Join(".", "testdata", "modules", "non_existent", "module_non_existent.so"),
				expect: false,
				args:   []string{},
				cmd:    []string{"NONEXISTENT", "key", "value"},
				want:   "",
				wantErr: fmt.Errorf("load module: module %s not found",
					path.Join(".", "testdata", "modules", "non_existent", "module_non_existent.so")),
			},
			{
				name:    "4. Test LUA module that handles hash values",
				path:    path.Join("..", "internal", "volumes", "modules", "lua", "hash.lua"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"LUA.HASH", "LUA.HASH_KEY_1"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "5. Test LUA module that handles set values",
				path:    path.Join("..", "internal", "volumes", "modules", "lua", "set.lua"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"LUA.SET", "LUA.SET_KEY_1", "LUA.SET_KEY_2", "LUA.SET_KEY_3"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "6. Test LUA module that handles zset values",
				path:    path.Join("..", "internal", "volumes", "modules", "lua", "zset.lua"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"LUA.ZSET", "LUA.ZSET_KEY_1", "LUA.ZSET_KEY_2", "LUA.ZSET_KEY_3"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "6. Test LUA module that handles list values",
				path:    path.Join("..", "internal", "volumes", "modules", "lua", "list.lua"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"LUA.LIST", "LUA.LIST_KEY_1"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "8. Test LUA module that handles primitive types",
				path:    path.Join("..", "internal", "volumes", "modules", "lua", "example.lua"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"LUA.EXAMPLE"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "9. Test JS module that handles primitive types",
				path:    path.Join("..", "internal", "volumes", "modules", "js", "example.js"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"JS.EXAMPLE"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "10. Test JS module that handles hashes",
				path:    path.Join("..", "internal", "volumes", "modules", "js", "hash.js"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"JS.HASH", "JS_HASH_KEY1"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "11. Test JS module that handles sets",
				path:    path.Join("..", "internal", "volumes", "modules", "js", "set.js"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"JS.SET", "JS_SET_KEY1", "member1"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "12. Test JS module that handles sorted sets",
				path:    path.Join("..", "internal", "volumes", "modules", "js", "zset.js"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"JS.ZSET", "JS_ZSET_KEY1", "member1", "2.142"},
				want:    "OK",
				wantErr: nil,
			},
			{
				name:    "13. Test JS module that handles lists",
				path:    path.Join("..", "internal", "volumes", "modules", "js", "list.js"),
				expect:  true,
				args:    []string{},
				cmd:     []string{"JS.LIST", "JS_LIST_KEY1"},
				want:    "OK",
				wantErr: nil,
			},
		}

		for _, test := range tests {
			// Load module
			err := server.LoadModule(test.path, test.args...)
			if err != nil {
				if test.wantErr == nil || err.Error() != test.wantErr.Error() {
					t.Error(fmt.Errorf("%s: %v", test.name, err))
					return
				}
				continue
			}
			// Execute command and check expected response
			res, err := server.ExecuteCommand(test.cmd...)
			if err != nil {
				t.Error(fmt.Errorf("%s: %v", test.name, err))
			}
			rv, _, err := resp.NewReader(bytes.NewReader(res)).ReadValue()
			if err != nil {
				t.Error(err)
			}
			if test.wantErr != nil {
				if test.wantErr.Error() != rv.Error().Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.wantErr.Error(), rv.Error().Error())
				}
				return
			}
			if rv.String() != test.want {
				t.Errorf("expected response \"%s\", got \"%s\"", test.want, rv.String())
			}
		}

		// Module list should contain all the modules above
		modules := server.ListModules()
		for _, test := range tests {
			// Skip the module if it's not expected
			if !test.expect {
				continue
			}
			// Check if module is loaded
			if !slices.Contains(modules, test.path) {
				t.Errorf("expected modules list to contain module \"%s\" but did not find it", test.path)
			}
			// Unload the module
			server.UnloadModule(test.path)
		}

		// Make sure the modules are no longer loaded
		modules = server.ListModules()
		for _, test := range tests {
			if slices.Contains(modules, test.path) {
				t.Errorf("expected modules list to not contain module \"%s\" but found it", test.path)
			}
		}
	})

	t.Run("TestSugarDB_CommandList", func(t *testing.T) {
		t.Parallel()

		server := createSugarDB()
		t.Cleanup(func() {
			server.ShutDown()
		})

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
				t.Parallel()
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
	})

	t.Run("TestSugarDB_CommandCount", func(t *testing.T) {
		t.Parallel()

		server := createSugarDB()
		t.Cleanup(func() {
			server.ShutDown()
		})

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
				t.Parallel()
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
	})

	t.Run("TestSugarDB_Save", func(t *testing.T) {
		t.Parallel()

		conf := DefaultConfig()
		conf.DataDir = path.Join(".", "testdata", "data")
		conf.EvictionPolicy = constants.NoEviction

		server := createSugarDBWithConfig(conf)
		t.Cleanup(func() {
			server.ShutDown()
		})

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
	})

	t.Run("TestSugarDB_LastSave", func(t *testing.T) {
		t.Parallel()

		server := createSugarDB()
		server.setLatestSnapshot(clock.NewClock().Now().Add(5 * time.Minute).UnixMilli())
		t.Cleanup(func() {
			server.ShutDown()
		})

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
	})
}
