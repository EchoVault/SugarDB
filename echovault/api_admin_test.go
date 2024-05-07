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
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal/constants"
	"github.com/tidwall/resp"
	"strconv"
	"testing"
)

func TestEchoVault_AddCommand(t *testing.T) {
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
		server := createEchoVault()
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

func TestEchoVault_ExecuteCommand(t *testing.T) {
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
		server := createEchoVault()
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

func TestEchoVault_RemoveCommand(t *testing.T) {
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
		server := createEchoVault()
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
