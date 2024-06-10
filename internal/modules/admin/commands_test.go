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
	"errors"
	"fmt"
	"github.com/echovault/echovault/echovault"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/clock"
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
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"time"
)

func setupServer(port uint16) (*echovault.EchoVault, error) {
	cfg := echovault.DefaultConfig()
	cfg.DataDir = ""
	cfg.BindAddr = "localhost"
	cfg.Port = port
	cfg.EvictionPolicy = constants.NoEviction
	return echovault.NewEchoVault(echovault.WithConfig(cfg))
}

func Test_AdminCommands(t *testing.T) {
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

	go func() {
		mockServer.Start()
	}()

	t.Cleanup(func() {
		mockServer.ShutDown()
	})

	t.Run("Test COMMANDS command", func(t *testing.T) {
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := resp.NewConn(conn)

		if err = client.WriteArray([]resp.Value{resp.StringValue("COMMANDS")}); err != nil {
			t.Error(err)
			return
		}

		res, _, err := client.ReadValue()
		if err != nil {
			t.Error(err)
			return
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

		if len(allCommands) != len(res.Array()) {
			t.Errorf("expected commands list to be of length %d, got %d", len(allCommands), len(res.Array()))
		}
	})

	t.Run("Test COMMAND COUNT command", func(t *testing.T) {
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := resp.NewConn(conn)

		if err = client.WriteArray([]resp.Value{resp.StringValue("COMMAND"), resp.StringValue("COUNT")}); err != nil {
			t.Error(err)
			return
		}

		res, _, err := client.ReadValue()
		if err != nil {
			t.Error(err)
			return
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

		if len(allCommands) != res.Integer() {
			t.Errorf("expected COMMAND COUNT to return %d, got %d", len(allCommands), res.Integer())
		}
	})

	t.Run("Test COMMAND LIST command", func(t *testing.T) {
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := resp.NewConn(conn)

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
			command := make([]resp.Value, len(test.cmd))
			for i, c := range test.cmd {
				command[i] = resp.StringValue(c)
			}
			if err = client.WriteArray(command); err != nil {
				t.Error(err)
				return
			}

			res, _, err := client.ReadValue()
			if err != nil {
				t.Error(err)
				return
			}

			if len(res.Array()) != len(test.want) {
				t.Errorf("expected response of length %d, got %d", len(test.want), len(res.Array()))
			}

			for _, command := range res.Array() {
				if !slices.ContainsFunc(test.want, func(c string) bool {
					return strings.EqualFold(c, command.String())
				}) {
					t.Errorf("command \"%s\" is not expected in response but is returned", command.String())
				}
			}
		}
	})

	t.Run("Test MODULE LOAD command", func(t *testing.T) {
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

		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
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
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
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
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
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

	t.Run("Test SAVE/LASTSAVE commands", func(t *testing.T) {
		t.Parallel()

		dataDir := path.Join(".", "testdata", "test_snapshot")
		t.Cleanup(func() {
			_ = os.RemoveAll(dataDir)
		})

		tests := []struct {
			name         string
			dataDir      string
			values       map[string]string
			snapshotFunc func(mockServer *echovault.EchoVault, port int) error
			lastSaveFunc func(mockServer *echovault.EchoVault, port int) (int, error)
			wantLastSave int
		}{
			{
				name:    "1. Snapshot with TCP connection",
				dataDir: path.Join(dataDir, "with_tcp_connection"),
				values: map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
					"key4": "value4",
				},
				snapshotFunc: func(mockServer *echovault.EchoVault, port int) error {
					// Start the server's TCP listener
					go func() {
						mockServer.Start()
					}()
					conn, err := internal.GetConnection("localhost", port)
					if err != nil {
						return err
					}
					defer func() {
						_ = conn.Close()
					}()
					client := resp.NewConn(conn)
					if err = client.WriteArray([]resp.Value{resp.StringValue("SAVE")}); err != nil {
						return err
					}
					res, _, err := client.ReadValue()
					if err != nil {
						return err
					}
					if !strings.EqualFold(res.String(), "ok") {
						return fmt.Errorf("expected save response to be \"OK\", got \"%s\"", res.String())
					}
					return nil
				},
				lastSaveFunc: func(mockServer *echovault.EchoVault, port int) (int, error) {
					conn, err := internal.GetConnection("localhost", port)
					if err != nil {
						return 0, err
					}
					defer func() {
						_ = conn.Close()
					}()
					client := resp.NewConn(conn)
					if err = client.WriteArray([]resp.Value{resp.StringValue("LASTSAVE")}); err != nil {
						return 0, err
					}
					res, _, err := client.ReadValue()
					if err != nil {
						return 0, err
					}
					return res.Integer(), nil
				},
				wantLastSave: int(clock.NewClock().Now().UnixMilli()),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				port, err := internal.GetFreePort()
				if err != nil {
					t.Error(err)
					return
				}

				conf := echovault.DefaultConfig()
				conf.DataDir = test.dataDir
				conf.BindAddr = "localhost"
				conf.Port = uint16(port)
				conf.RestoreSnapshot = true

				mockServer, err := echovault.NewEchoVault(echovault.WithConfig(conf))
				if err != nil {
					t.Error(err)
					return
				}
				defer func() {
					// Shutdown
					mockServer.ShutDown()
				}()

				// Trigger some write commands
				for key, value := range test.values {
					if _, _, err = mockServer.Set(key, value, echovault.SetOptions{}); err != nil {
						t.Error(err)
						return
					}
				}

				// Function to trigger snapshot save
				if err = test.snapshotFunc(mockServer, port); err != nil {
					t.Error(err)
				}

				// Yield to allow snapshot to complete sync.
				ticker := time.NewTicker(200 * time.Millisecond)
				<-ticker.C
				ticker.Stop()

				// Restart server with the same config. This should restore the snapshot
				mockServer, err = echovault.NewEchoVault(echovault.WithConfig(conf))
				if err != nil {
					t.Error(err)
					return
				}

				// Check that all the key/value pairs have been restored into the store.
				for key, value := range test.values {
					res, err := mockServer.Get(key)
					if err != nil {
						t.Error(err)
						return
					}
					if res != value {
						t.Errorf("expected value at key \"%s\" to be \"%s\", got \"%s\"", key, value, res)
						return
					}
				}

				// Check that the lastsave is the time the last snapshot was taken.
				lastSave, err := test.lastSaveFunc(mockServer, port)
				if err != nil {
					t.Error(err)
					return
				}

				if lastSave != test.wantLastSave {
					t.Errorf("expected lastsave to be %d, got %d", test.wantLastSave, lastSave)
				}
			})
		}
	})

	t.Run("Test REWRITEAOF command", func(t *testing.T) {
		t.Parallel()

		ticker := time.NewTicker(200 * time.Millisecond)

		dataDir := path.Join(".", "testdata", "test_aof")
		t.Cleanup(func() {
			_ = os.RemoveAll(dataDir)
			ticker.Stop()
		})

		// Prepare data for testing.
		data := map[string]map[string]string{
			"before-rewrite": {
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
				"key4": "value4",
			},
			"after-rewrite": {
				"key3": "value3-updated",
				"key4": "value4-updated",
				"key5": "value5",
				"key6": "value6",
			},
			"expected-values": {
				"key1": "value1",
				"key2": "value2",
				"key3": "value3-updated",
				"key4": "value4-updated",
				"key5": "value5",
				"key6": "value6",
			},
		}

		port, err := internal.GetFreePort()
		if err != nil {
			t.Error(err)
			return
		}

		conf := echovault.DefaultConfig()
		conf.BindAddr = "localhost"
		conf.Port = uint16(port)
		conf.RestoreAOF = true
		conf.DataDir = dataDir
		conf.AOFSyncStrategy = "always"

		// Start new server
		mockServer, err := echovault.NewEchoVault(echovault.WithConfig(conf))
		if err != nil {
			t.Error(err)
			return
		}
		go func() {
			mockServer.Start()
		}()

		// Get client connection
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		client := resp.NewConn(conn)

		// Perform write commands from "before-rewrite"
		for key, value := range data["before-rewrite"] {
			if err := client.WriteArray([]resp.Value{
				resp.StringValue("SET"),
				resp.StringValue(key),
				resp.StringValue(value),
			}); err != nil {
				t.Error(err)
				return
			}
			res, _, err := client.ReadValue()
			if err != nil {
				t.Error(err)
				return
			}
			if !strings.EqualFold(res.String(), "ok") {
				t.Errorf("expected response OK, got \"%s\"", res.String())
			}
		}

		// Yield
		<-ticker.C

		// Rewrite AOF
		if err := client.WriteArray([]resp.Value{resp.StringValue("REWRITEAOF")}); err != nil {
			t.Error(err)
			return
		}
		res, _, err := client.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		if !strings.EqualFold(res.String(), "ok") {
			t.Errorf("expected response OK, got \"%s\"", res.String())
		}

		// Perform write commands from "after-rewrite"
		for key, value := range data["after-rewrite"] {
			if err := client.WriteArray([]resp.Value{
				resp.StringValue("SET"),
				resp.StringValue(key),
				resp.StringValue(value),
			}); err != nil {
				t.Error(err)
				return
			}
			res, _, err := client.ReadValue()
			if err != nil {
				t.Error(err)
				return
			}
			if !strings.EqualFold(res.String(), "ok") {
				t.Errorf("expected response OK, got \"%s\"", res.String())
			}
		}

		// Yield
		<-ticker.C

		// Shutdown the EchoVault instance and close current client connection
		mockServer.ShutDown()
		_ = conn.Close()

		// Start another instance of EchoVault
		mockServer, err = echovault.NewEchoVault(echovault.WithConfig(conf))
		if err != nil {
			t.Error(err)
			return
		}
		go func() {
			mockServer.Start()
		}()

		// Get a new client connection
		conn, err = internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		client = resp.NewConn(conn)

		// Check if the servers contains the keys and values from "expected-values"
		for key, value := range data["expected-values"] {
			if err := client.WriteArray([]resp.Value{resp.StringValue("GET"), resp.StringValue(key)}); err != nil {
				t.Error(err)
				return
			}
			res, _, err := client.ReadValue()
			if err != nil {
				t.Error(err)
				return
			}
			if res.String() != value {
				t.Errorf("expected value at key \"%s\" to be \"%s\", got \"%s\"", key, value, res)
				return
			}
		}

		// Shutdown server and close client connection
		_ = conn.Close()
		mockServer.ShutDown()
	})
}
