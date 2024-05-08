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

package admin_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/echovault"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/constants"
	"github.com/echovault/echovault/internal/modules/acl"
	"github.com/echovault/echovault/internal/modules/admin"
	"github.com/echovault/echovault/internal/modules/connection"
	"github.com/echovault/echovault/internal/modules/generic"
	"github.com/echovault/echovault/internal/modules/hash"
	"github.com/echovault/echovault/internal/modules/list"
	"github.com/echovault/echovault/internal/modules/pubsub"
	"github.com/echovault/echovault/internal/modules/set"
	"github.com/echovault/echovault/internal/modules/sorted_set"
	str "github.com/echovault/echovault/internal/modules/string"
	"github.com/tidwall/resp"
	"net"
	"os"
	"path"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func setupServer(port uint16) (*echovault.EchoVault, error) {
	cfg := echovault.DefaultConfig()
	cfg.DataDir = ""
	cfg.BindAddr = "localhost"
	cfg.Port = port
	cfg.EvictionPolicy = constants.NoEviction
	return echovault.NewEchoVault(echovault.WithConfig(cfg))
}

func getUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func getHandler(mockServer *echovault.EchoVault, commands ...string) internal.HandlerFunc {
	if len(commands) == 0 {
		return nil
	}
	getCommands :=
		getUnexportedField(reflect.ValueOf(mockServer).Elem().FieldByName("getCommands")).(func() []internal.Command)
	for _, c := range getCommands() {
		if strings.EqualFold(commands[0], c.Command) && len(commands) == 1 {
			// Get command handler
			return c.HandlerFunc
		}
		if strings.EqualFold(commands[0], c.Command) {
			// Get sub-command handler
			for _, sc := range c.SubCommands {
				if strings.EqualFold(commands[1], sc.Command) {
					return sc.HandlerFunc
				}
			}
		}
	}
	return nil
}

func getHandlerFuncParams(ctx context.Context, mockServer *echovault.EchoVault, cmd []string, conn *net.Conn) internal.HandlerFuncParams {
	getCommands :=
		getUnexportedField(reflect.ValueOf(mockServer).Elem().FieldByName("getCommands")).(func() []internal.Command)
	return internal.HandlerFuncParams{
		Context:        ctx,
		Command:        cmd,
		Connection:     conn,
		GetAllCommands: getCommands,
	}
}

func Test_AdminCommand(t *testing.T) {
	t.Cleanup(func() {
		_ = os.RemoveAll("./testdata")
	})

	t.Run("Test COMMANDS command", func(t *testing.T) {
		t.Parallel()

		port, err := internal.GetFreePort()
		if err != nil {
			t.Error(err)
			return
		}
		mockServer, err := setupServer(uint16(port))
		if err != nil {
			t.Error(err)
			return
		}

		res, err := getHandler(mockServer, "COMMANDS")(
			getHandlerFuncParams(context.Background(), mockServer, []string{"commands"}, nil),
		)
		if err != nil {
			t.Error(err)
		}

		rd := resp.NewReader(bytes.NewReader(res))
		rv, _, err := rd.ReadValue()
		if err != nil {
			t.Error(err)
		}

		// Get all the commands from the existing modules.
		var commands []internal.Command
		commands = append(commands, acl.Commands()...)
		commands = append(commands, admin.Commands()...)
		commands = append(commands, generic.Commands()...)
		commands = append(commands, hash.Commands()...)
		commands = append(commands, list.Commands()...)
		commands = append(commands, connection.Commands()...)
		commands = append(commands, pubsub.Commands()...)
		commands = append(commands, set.Commands()...)
		commands = append(commands, sorted_set.Commands()...)
		commands = append(commands, str.Commands()...)

		// Flatten the commands and subcommands.
		var allCommands []string
		for _, c := range commands {
			if c.SubCommands == nil || len(c.SubCommands) == 0 {
				allCommands = append(allCommands, c.Command)
				continue
			}
			for _, sc := range c.SubCommands {
				allCommands = append(allCommands, fmt.Sprintf("%s|%s", c.Command, sc.Command))
			}
		}

		if len(allCommands) != len(rv.Array()) {
			t.Errorf("expected commands list to be of length %d, got %d", len(allCommands), len(rv.Array()))
		}
	})

	t.Run("Test COMMAND COUNT command", func(t *testing.T) {
		t.Parallel()

		port, err := internal.GetFreePort()
		if err != nil {
			t.Error(err)
			return
		}
		mockServer, err := setupServer(uint16(port))
		if err != nil {
			t.Error(err)
			return
		}

		res, err := getHandler(mockServer, "COMMAND", "COUNT")(
			getHandlerFuncParams(context.Background(), mockServer, []string{"command", "count"}, nil),
		)
		if err != nil {
			t.Error(err)
		}

		rd := resp.NewReader(bytes.NewReader(res))
		rv, _, err := rd.ReadValue()
		if err != nil {
			t.Error(err)
		}

		// Get all the commands from the existing modules.
		var commands []internal.Command
		commands = append(commands, acl.Commands()...)
		commands = append(commands, admin.Commands()...)
		commands = append(commands, generic.Commands()...)
		commands = append(commands, hash.Commands()...)
		commands = append(commands, list.Commands()...)
		commands = append(commands, connection.Commands()...)
		commands = append(commands, pubsub.Commands()...)
		commands = append(commands, set.Commands()...)
		commands = append(commands, sorted_set.Commands()...)
		commands = append(commands, str.Commands()...)

		// Flatten the commands and subcommands.
		var allCommands []string
		for _, c := range commands {
			if c.SubCommands == nil || len(c.SubCommands) == 0 {
				allCommands = append(allCommands, c.Command)
				continue
			}
			for _, sc := range c.SubCommands {
				allCommands = append(allCommands, fmt.Sprintf("%s|%s", c.Command, sc.Command))
			}
		}

		if len(allCommands) != rv.Integer() {
			t.Errorf("expected COMMAND COUNT to return %d, got %d", len(allCommands), rv.Integer())
		}
	})

	t.Run("Test COMMAND LIST command", func(t *testing.T) {
		t.Parallel()

		port, err := internal.GetFreePort()
		if err != nil {
			t.Error(err)
			return
		}

		mockServer, err := setupServer(uint16(port))
		if err != nil {
			t.Error(err)
			return
		}

		// Get all the commands from the existing modules.
		var allCommands []internal.Command
		allCommands = append(allCommands, acl.Commands()...)
		allCommands = append(allCommands, admin.Commands()...)
		allCommands = append(allCommands, generic.Commands()...)
		allCommands = append(allCommands, hash.Commands()...)
		allCommands = append(allCommands, list.Commands()...)
		allCommands = append(allCommands, connection.Commands()...)
		allCommands = append(allCommands, pubsub.Commands()...)
		allCommands = append(allCommands, set.Commands()...)
		allCommands = append(allCommands, sorted_set.Commands()...)
		allCommands = append(allCommands, str.Commands()...)

		tests := []struct {
			name string
			cmd  []string
			want []string
		}{
			{
				name: "1. Return all commands with no filter specified",
				cmd:  []string{"COMMAND", "LIST"},
				want: func() []string {
					var commands []string
					for _, command := range allCommands {
						if command.SubCommands == nil || len(command.SubCommands) == 0 {
							commands = append(commands, command.Command)
							continue
						}
						for _, subcommand := range command.SubCommands {
							commands = append(commands, fmt.Sprintf("%s %s", command.Command, subcommand.Command))
						}
					}
					return commands
				}(),
			},
			{
				name: "2. Return all commands that contain the provided ACL category",
				cmd:  []string{"COMMAND", "LIST", "FILTERBY", "ACLCAT", constants.FastCategory},
				want: func() []string {
					var commands []string
					for _, command := range allCommands {
						if (command.SubCommands == nil || len(command.SubCommands) == 0) &&
							slices.Contains(command.Categories, constants.FastCategory) {
							commands = append(commands, command.Command)
							continue
						}
						for _, subcommand := range command.SubCommands {
							if slices.Contains(subcommand.Categories, constants.FastCategory) {
								commands = append(commands, fmt.Sprintf("%s %s", command.Command, subcommand.Command))
							}
						}
					}
					return commands
				}(),
			},
			{
				name: "3. Return all commands that match the provided pattern",
				cmd:  []string{"COMMAND", "LIST", "FILTERBY", "PATTERN", "z*"},
				want: func() []string {
					var commands []string
					for _, command := range sorted_set.Commands() {
						commands = append(commands, command.Command)
					}
					return commands
				}(),
			},
			{
				name: "4. Return all commands that belong to the specified module",
				cmd:  []string{"COMMAND", "LIST", "FILTERBY", "MODULE", constants.HashModule},
				want: func() []string {
					var commands []string
					for _, command := range hash.Commands() {
						commands = append(commands, command.Command)
					}
					return commands
				}(),
			},
		}

		for _, test := range tests {
			res, err := getHandler(mockServer, test.cmd...)(
				getHandlerFuncParams(context.Background(), mockServer, test.cmd, nil),
			)
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if len(rv.Array()) != len(test.want) {
				t.Errorf("expected response of length %d, got %d", len(test.want), len(rv.Array()))
			}

			for _, command := range rv.Array() {
				if !slices.ContainsFunc(test.want, func(c string) bool {
					return strings.EqualFold(c, command.String())
				}) {
					t.Errorf("command \"%s\" is not expected in response but is returned", command.String())
				}
			}
		}
	})

	t.Run("Test MODULE LOAD command", func(t *testing.T) {
		t.Parallel()

		port, err := internal.GetFreePort()
		if err != nil {
			t.Error(err)
			return
		}
		mockServer, err := setupServer(uint16(port))
		if err != nil {
			t.Error(err)
			return
		}

		tests := []struct {
			name        string
			execCommand []resp.Value
			wantExecRes string
			wantExecErr error
			testCommand []resp.Value
			wantTestRes string
			wantTestErr error
		}{
			{
				name: "1. Successfully load module_set module and return a response from the module handler",
				execCommand: []resp.Value{
					resp.StringValue("MODULE"),
					resp.StringValue("LOAD"),
					resp.StringValue(path.Join(".", "testdata", "modules", "module_set", "module_set.so")),
				},
				wantExecRes: "OK",
				wantExecErr: nil,
				testCommand: []resp.Value{
					resp.StringValue("MODULE.SET"),
					resp.StringValue("key1"),
					resp.StringValue("20"),
				},
				wantTestRes: "OK",
				wantTestErr: nil,
			},
			{
				name: "2. Successfully load module_get module and return a response from the module handler",
				execCommand: []resp.Value{
					resp.StringValue("MODULE"),
					resp.StringValue("LOAD"),
					resp.StringValue(path.Join(".", "testdata", "modules", "module_get", "module_get.so")),
					resp.StringValue("10"), // With args
				},
				wantExecRes: "OK",
				wantExecErr: nil,
				testCommand: []resp.Value{
					resp.StringValue("MODULE.GET"),
					resp.StringValue("key1"),
				},
				wantTestRes: "200",
				wantTestErr: nil,
			},
			{
				name:        "3. Return error from module_set command handler",
				execCommand: make([]resp.Value, 0),
				wantExecRes: "",
				wantExecErr: nil,
				testCommand: []resp.Value{resp.StringValue("MODULE.SET"), resp.StringValue("key2")},
				wantTestRes: "",
				wantTestErr: errors.New("wrong no of args for module.set command"),
			},
			{
				name: "4. Return error from module_get command handler",
				execCommand: []resp.Value{
					resp.StringValue("SET"),
					resp.StringValue("key2"),
					resp.StringValue("value1"),
				},
				wantExecRes: "OK",
				wantExecErr: nil,
				testCommand: []resp.Value{
					resp.StringValue("MODULE.GET"),
					resp.StringValue("key2"),
				},
				wantTestRes: "",
				wantTestErr: errors.New("value at key key2 is not an integer"),
			},
			{
				name: "5. Return OK when reloading module that is already loaded",
				execCommand: []resp.Value{
					resp.StringValue("MODULE"),
					resp.StringValue("LOAD"),
					resp.StringValue(path.Join(".", "testdata", "modules", "module_set", "module_set.so")),
				},
				wantExecRes: "OK",
				testCommand: []resp.Value{
					resp.StringValue("MODULE.SET"),
					resp.StringValue("key3"),
					resp.StringValue("20"),
				},
				wantTestRes: "OK",
				wantTestErr: nil,
			},
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			wg.Done()
			mockServer.Start()
		}()
		wg.Wait()

		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Error(err)
		}

		respConn := resp.NewConn(conn)

		for i := 0; i < len(tests); i++ {
			if len(tests[i].wantExecRes) > 0 {
				// If the length of execCommand is > 0, write the command to the connection.
				if err := respConn.WriteArray(tests[i].execCommand); err != nil {
					t.Error(err)
				}
				// Read the response from the server.
				r, _, err := respConn.ReadValue()
				if err != nil {
					t.Error(err)
				}
				// If we expect an error, check if the error matches the one we expect.
				if tests[i].wantExecErr != nil {
					if !strings.Contains(strings.ToLower(r.Error().Error()), strings.ToLower(tests[i].wantExecErr.Error())) {
						t.Errorf("expected error to contain \"%s\", got \"%s\"", tests[i].wantExecErr.Error(), r.Error().Error())
						return
					}
				}
				// If there's no expected error, check if the response is what's expected.
				if tests[i].wantExecRes != "" {
					if r.String() != tests[i].wantExecRes {
						t.Errorf("expected exec response \"%s\", got \"%s\"", tests[i].wantExecRes, r.String())
						return
					}
				}
			}

			if len(tests[i].testCommand) > 0 {
				// If the length of test command is > 0, write teh command to the connections.
				if err := respConn.WriteArray(tests[i].testCommand); err != nil {
					t.Error(err)
				}
				// Read the response from the server.
				r, _, err := respConn.ReadValue()
				if err != nil {
					t.Error(err)
				}
				// If we expect an error, check if the error is what's expected.
				if tests[i].wantTestErr != nil {
					if !strings.Contains(strings.ToLower(r.Error().Error()), strings.ToLower(tests[i].wantTestErr.Error())) {
						t.Errorf("expected error to contain \"%s\", got \"%s\"", tests[i].wantTestErr.Error(), r.Error().Error())
						return
					}
				}
				// Check if the response is what's expected.
				if tests[i].wantTestRes != "" {
					if r.String() != tests[i].wantTestRes {
						t.Errorf("expected test response \"%s\", got \"%s\"", tests[i].wantTestRes, r.String())
						return
					}
				}
			}
		}
	})

	t.Run("Test MODULE UNLOAD command", func(t *testing.T) {
		t.Parallel()

		port, err := internal.GetFreePort()
		if err != nil {
			t.Error(err)
			return
		}
		mockServer, err := setupServer(uint16(port))
		if err != nil {
			t.Error(err)
			return
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			wg.Done()
			mockServer.Start()
		}()
		wg.Wait()

		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Error(err)
		}

		respConn := resp.NewConn(conn)

		// Load module.set module
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("MODULE"),
			resp.StringValue("LOAD"),
			resp.StringValue(path.Join(".", "testdata", "modules", "module_set", "module_set.so")),
		}); err != nil {
			t.Errorf("load module_set: %v", err)
			return
		}
		// Expect OK response
		r, _, err := respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if r.String() != "OK" {
			t.Errorf("expected response OK, got \"%s\"", r.String())
			return
		}

		// Load module.get module with arg
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("MODULE"),
			resp.StringValue("LOAD"),
			resp.StringValue(path.Join(".", "testdata", "modules", "module_get", "module_get.so")),
			resp.StringValue("10"),
		}); err != nil {
			t.Errorf("load module_get: %v", err)
			return
		}
		// Expect OK response
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if r.String() != "OK" {
			t.Errorf("expected response OK, got \"%s\"", r.String())
			return
		}

		// Execute module.set command, expect OK response
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("module.set"),
			resp.StringValue("key1"),
			resp.StringValue("50"),
		}); err != nil {
			t.Errorf("exec module.set: %v", err)
			return
		}
		// Expect OK response
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if r.String() != "OK" {
			t.Errorf("expected response OK, got \"%s\"", r.String())
			return
		}

		// Execute module.get command, expect integer response
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("module.get"),
			resp.StringValue("key1"),
		}); err != nil {
			t.Errorf("exec module.get: %v", err)
			return
		}
		// Expect integer response
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if r.Integer() != 500 {
			t.Errorf("expected response 500, got \"%d\"", r.Integer())
			return
		}

		// Unload module.set module
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("MODULE"),
			resp.StringValue("UNLOAD"),
			resp.StringValue(path.Join(".", "testdata", "modules", "module_set", "module_set.so")),
		}); err != nil {
			t.Errorf("unload module_set: %v", err)
			return
		}
		// Expect OK response
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if r.String() != "OK" {
			t.Errorf("expected response OK, got \"%s\"", r.String())
			return
		}

		// Unload module.get module
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("MODULE"),
			resp.StringValue("UNLOAD"),
			resp.StringValue(path.Join(".", "testdata", "modules", "module_get", "module_get.so")),
		}); err != nil {
			t.Errorf("unload module_get: %v", err)
			return
		}
		// Expect OK response
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if r.String() != "OK" {
			t.Errorf("expected response OK, got \"%s\"", r.String())
			return
		}

		// Try to execute module.set command, should receive command not supported error
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("module.set"),
			resp.StringValue("key1"),
			resp.StringValue("50"),
		}); err != nil {
			t.Errorf("retry module.set: %v", err)
			return
		}
		// Expect command not supported response
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if !strings.Contains(r.Error().Error(), "command module.set not supported") {
			t.Errorf("expected error to contain \"command module.set not supported\", got \"%s\"", r.Error().Error())
			return
		}

		// Try to execute module.get command, should receive command not supported error
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("module.get"),
			resp.StringValue("key1"),
		}); err != nil {
			t.Errorf("retry module.get: %v", err)
			return
		}
		// Expect command not supported response
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if !strings.Contains(r.Error().Error(), "command module.get not supported") {
			t.Errorf("expected error to contain \"command module.get not supported\", got \"%s\"", r.Error().Error())
			return
		}
	})

	t.Run("Test MODULE LIST command", func(t *testing.T) {
		t.Parallel()

		port, err := internal.GetFreePort()
		if err != nil {
			t.Error(err)
			return
		}
		mockServer, err := setupServer(uint16(port))
		if err != nil {
			t.Error(err)
			return
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			wg.Done()
			mockServer.Start()
		}()
		wg.Wait()

		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Error(err)
		}

		respConn := resp.NewConn(conn)

		// Load module.get module with arg
		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("MODULE"),
			resp.StringValue("LOAD"),
			resp.StringValue(path.Join(".", "testdata", "modules", "module_get", "module_get.so")),
		}); err != nil {
			t.Errorf("load module_get: %v", err)
			return
		}
		// Expect OK response
		r, _, err := respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if r.String() != "OK" {
			t.Errorf("expected response OK, got \"%s\"", r.String())
			return
		}

		if err := respConn.WriteArray([]resp.Value{
			resp.StringValue("MODULE"),
			resp.StringValue("LIST"),
		}); err != nil {
			t.Errorf("list module: %v", err)
		}
		r, _, err = respConn.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}

		serverModules := mockServer.ListModules()

		if len(r.Array()) != len(serverModules) {
			t.Errorf("expected response of length %d, got %d", len(serverModules), len(r.Array()))
			return
		}

		for _, resModule := range r.Array() {
			if !slices.ContainsFunc(serverModules, func(serverModule string) bool {
				return resModule.String() == serverModule
			}) {
				t.Errorf("could not file module \"%s\" in the loaded server modules \"%s\"", resModule, serverModules)
			}
		}
	})
}
