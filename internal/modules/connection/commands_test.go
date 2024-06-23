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

package connection_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/echovault/echovault/echovault"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/constants"
	"github.com/tidwall/resp"
)

func setUpServer(port int, requirePass bool, aclConfig string) (*echovault.EchoVault, error) {
	conf := config.Config{
		BindAddr:       "localhost",
		Port:           uint16(port),
		DataDir:        "",
		EvictionPolicy: constants.NoEviction,
		RequirePass:    requirePass,
		Password:       "password1",
		AclConfig:      aclConfig,
	}

	mockServer, err := echovault.NewEchoVault(
		echovault.WithConfig(conf),
	)
	if err != nil {
		return nil, err
	}

	// Add the initial test users to the ACL module.
	for _, user := range generateInitialTestUsers() {
		if _, err := mockServer.ACLSetUser(user); err != nil {
			return nil, err
		}
	}

	return mockServer, nil
}

func generateInitialTestUsers() []echovault.User {
	return []echovault.User{
		{
			// User with both hash password and plaintext password.
			Username:          "with_password_user",
			Enabled:           true,
			IncludeCategories: []string{"*"},
			IncludeCommands:   []string{"*"},
			AddPlainPasswords: []string{"password2"},
			AddHashPasswords:  []string{generateSHA256Password("password3")},
		},
		{
			// User with NoPassword option.
			Username:          "no_password_user",
			Enabled:           true,
			NoPassword:        true,
			AddPlainPasswords: []string{"password4"},
		},
		{
			// Disabled user.
			Username:          "disabled_user",
			Enabled:           false,
			AddPlainPasswords: []string{"password5"},
		},
	}
}

func generateSHA256Password(plain string) string {
	h := sha256.New()
	h.Write([]byte(plain))
	return hex.EncodeToString(h.Sum(nil))
}

func Test_Connection(t *testing.T) {
	port, err := internal.GetFreePort()
	if err != nil {
		t.Error(err)
		return
	}

	mockServer, err := setUpServer(port, true, "")
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

	t.Run("Test_HandleAuth", func(t *testing.T) {
		t.Parallel()

		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			if conn != nil {
				_ = conn.Close()
			}
		}()

		r := resp.NewConn(conn)

		tests := []struct {
			name    string
			cmd     []resp.Value
			wantRes string
			wantErr string
		}{
			{
				name:    "1. Authenticate with default user without specifying username",
				cmd:     []resp.Value{resp.StringValue("AUTH"), resp.StringValue("password1")},
				wantRes: "OK",
				wantErr: "",
			},
			{
				name: "2. Authenticate with plaintext password",
				cmd: []resp.Value{
					resp.StringValue("AUTH"),
					resp.StringValue("with_password_user"),
					resp.StringValue("password2"),
				},
				wantRes: "OK",
				wantErr: "",
			},
			{
				name: "3. Authenticate with SHA256 password",
				cmd: []resp.Value{
					resp.StringValue("AUTH"),
					resp.StringValue("with_password_user"),
					resp.StringValue("password3"),
				},
				wantRes: "OK",
				wantErr: "",
			},
			{
				name: "4. Authenticate with no password user",
				cmd: []resp.Value{
					resp.StringValue("AUTH"),
					resp.StringValue("no_password_user"),
					resp.StringValue("password4"),
				},
				wantRes: "OK",
				wantErr: "",
			},
			{
				name: "5. Fail to authenticate with disabled user",
				cmd: []resp.Value{
					resp.StringValue("AUTH"),
					resp.StringValue("disabled_user"),
					resp.StringValue("password5"),
				},
				wantRes: "",
				wantErr: "Error user disabled_user is disabled",
			},
			{
				name: "6. Fail to authenticate with non-existent user",
				cmd: []resp.Value{
					resp.StringValue("AUTH"),
					resp.StringValue("non_existent_user"),
					resp.StringValue("password6"),
				},
				wantRes: "",
				wantErr: "Error no user with username non_existent_user",
			},
			{
				name: "7. Fail to authenticate with the wrong password",
				cmd: []resp.Value{
					resp.StringValue("AUTH"),
					resp.StringValue("with_password_user"),
					resp.StringValue("wrong_password"),
				},
				wantRes: "",
				wantErr: "Error could not authenticate user",
			},
			{
				name:    "8. Command too short",
				cmd:     []resp.Value{resp.StringValue("AUTH")},
				wantRes: "",
				wantErr: fmt.Sprintf("Error %s", constants.WrongArgsResponse),
			},
			{
				name: "9. Command too long",
				cmd: []resp.Value{
					resp.StringValue("AUTH"),
					resp.StringValue("user"),
					resp.StringValue("password1"),
					resp.StringValue("password2"),
				},
				wantRes: "",
				wantErr: fmt.Sprintf("Error %s", constants.WrongArgsResponse),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if err = r.WriteArray(test.cmd); err != nil {
					t.Error(err)
				}
				rv, _, err := r.ReadValue()
				if err != nil {
					t.Error(err)
				}
				if test.wantErr != "" {
					if rv.Error().Error() != test.wantErr {
						t.Errorf("expected error response \"%s\", got \"%s\"", test.wantErr, rv.Error().Error())
					}
					return
				}
				if rv.String() != test.wantRes {
					t.Errorf("expected response \"%s\", got \"%s\"", test.wantRes, rv.String())
				}
			})
		}
	})

	t.Run("Test_HandlePing", func(t *testing.T) {
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := resp.NewConn(conn)

		tests := []struct {
			command     []resp.Value
			expected    string
			expectedErr error
		}{
			{
				command:     []resp.Value{resp.StringValue("PING")},
				expected:    "PONG",
				expectedErr: nil,
			},
			{
				command:     []resp.Value{resp.StringValue("PING"), resp.StringValue("Hello, world!")},
				expected:    "Hello, world!",
				expectedErr: nil,
			},
			{
				command: []resp.Value{
					resp.StringValue("PING"),
					resp.StringValue("Hello, world!"),
					resp.StringValue("Once more"),
				},
				expected:    "",
				expectedErr: errors.New(constants.WrongArgsResponse),
			},
		}

		for _, test := range tests {
			if err = client.WriteArray(test.command); err != nil {
				t.Error(err)
				return
			}

			res, _, err := client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if test.expectedErr != nil {
				if !strings.Contains(res.Error().Error(), test.expectedErr.Error()) {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedErr.Error(), res.Error().Error())
				}
				continue
			}

			if res.String() != test.expected {
				t.Errorf("expected response \"%s\", got \"%s\"", test.expected, res.String())
			}
		}
	})

	t.Run("Test_HandleEcho", func(t *testing.T) {
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := resp.NewConn(conn)

		tests := []struct {
			command     []resp.Value
			expected    string
			expectedErr error
		}{
			{
				command:     []resp.Value{resp.StringValue("ECHO"), resp.StringValue("Hello, EchoVault!")},
				expected:    "Hello, EchoVault!",
				expectedErr: nil,
			},
			{
				command:     []resp.Value{resp.StringValue("ECHO")},
				expected:    "",
				expectedErr: errors.New(constants.WrongArgsResponse),
			},
			{
				command: []resp.Value{
					resp.StringValue("ECHO"),
					resp.StringValue("Hello, EchoVault!"),
					resp.StringValue("Once more"),
				},
				expected:    "",
				expectedErr: errors.New(constants.WrongArgsResponse),
			},
		}

		for _, test := range tests {
			if err = client.WriteArray(test.command); err != nil {
				t.Error(err)
				return
			}

			res, _, err := client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if test.expectedErr != nil {
				if !strings.Contains(res.Error().Error(), test.expectedErr.Error()) {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedErr.Error(), res.Error().Error())
				}
				continue
			}

			if res.String() != test.expected {
				t.Errorf("expected response \"%s\", got \"%s\"", test.expected, res.String())
			}
		}
	})

}
