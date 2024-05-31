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
	"errors"
	"github.com/echovault/echovault/echovault"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/constants"
	"github.com/tidwall/resp"
	"strings"
	"testing"
)

func Test_Connection(t *testing.T) {
	port, err := internal.GetFreePort()
	if err != nil {
		t.Error(err)
		return
	}

	mockServer, err := echovault.NewEchoVault(
		echovault.WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
			BindAddr:       "localhost",
			Port:           uint16(port),
		}),
	)
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

}
