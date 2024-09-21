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
	"bufio"
	"bytes"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/constants"
	"github.com/echovault/echovault/internal/modules/connection"
	"github.com/tidwall/resp"
	"reflect"
	"testing"
)

func TestSugarDB_Hello(t *testing.T) {
	t.Parallel()

	port, err := internal.GetFreePort()
	if err != nil {
		t.Error(err)
		return
	}

	conf := DefaultConfig()
	conf.Port = uint16(port)
	conf.RequirePass = false

	mockServer := createSugarDBWithConfig(conf)
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

	tests := []struct {
		name    string
		command []resp.Value
		wantRes []byte
	}{
		{
			name:    "1. Hello",
			command: []resp.Value{resp.StringValue("HELLO")},
			wantRes: connection.BuildHelloResponse(
				internal.ServerInfo{
					Server:  "echovault",
					Version: constants.Version,
					Id:      "",
					Mode:    "standalone",
					Role:    "master",
					Modules: mockServer.ListModules(),
				},
				internal.ConnectionInfo{
					Id:       1,
					Name:     "",
					Protocol: 2,
					Database: 0,
				},
			),
		},
		{
			name:    "2. Hello 2",
			command: []resp.Value{resp.StringValue("HELLO"), resp.StringValue("2")},
			wantRes: connection.BuildHelloResponse(
				internal.ServerInfo{
					Server:  "echovault",
					Version: constants.Version,
					Id:      "",
					Mode:    "standalone",
					Role:    "master",
					Modules: mockServer.ListModules(),
				},
				internal.ConnectionInfo{
					Id:       2,
					Name:     "",
					Protocol: 2,
					Database: 0,
				},
			),
		},
		{
			name:    "3. Hello 3",
			command: []resp.Value{resp.StringValue("HELLO"), resp.StringValue("3")},
			wantRes: connection.BuildHelloResponse(
				internal.ServerInfo{
					Server:  "echovault",
					Version: constants.Version,
					Id:      "",
					Mode:    "standalone",
					Role:    "master",
					Modules: mockServer.ListModules(),
				},
				internal.ConnectionInfo{
					Id:       3,
					Name:     "",
					Protocol: 3,
					Database: 0,
				},
			),
		},
	}

	for i := 0; i < len(tests); i++ {
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		client := resp.NewConn(conn)

		if err = client.WriteArray(tests[i].command); err != nil {
			t.Error(err)
			return
		}

		buf := bufio.NewReader(conn)
		res, err := internal.ReadMessage(buf)
		if err != nil {
			t.Error(err)
			return
		}

		if !bytes.Equal(tests[i].wantRes, res) {
			t.Errorf("expected byte resposne:\n%s, \n\ngot:\n%s", string(tests[i].wantRes), string(res))
			return
		}

		// Close connection
		_ = conn.Close()
	}
}

func TestSugarDB_SelectDB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		presetValues map[int]map[string]string
		database     int
		want         map[int][]string
		wantErr      bool
	}{
		{
			name: "1. Change database and read new values",
			presetValues: map[int]map[string]string{
				0: {"key1": "value-01", "key2": "value-02", "key3": "value-03"},
				1: {"key1": "value-11", "key2": "value-12", "key3": "value-13"},
			},
			database: 1,
			want: map[int][]string{
				0: {"value-01", "value-02", "value-03"},
				1: {"value-11", "value-12", "value-13"},
			},
			wantErr: false,
		},
		{
			name: "2. Error when database parameter is < 0",
			presetValues: map[int]map[string]string{
				0: {"key1": "value-01", "key2": "value-02", "key3": "value-03"},
			},
			database: -1,
			want: map[int][]string{
				0: {"value-01", "value-02", "value-03"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := createSugarDB()

			if tt.presetValues != nil {
				for db, data := range tt.presetValues {
					_ = server.SelectDB(db)
					if _, err := server.MSet(data); err != nil {
						t.Errorf("SelectDB() error = %v", err)
						return
					}
				}
				_ = server.SelectDB(0)
			}

			// Check the values for DB 0
			values, err := server.MGet("key1", "key2", "key3")
			if err != nil {
				t.Errorf("SelectDB() error = %v", err)
				return
			}

			if !reflect.DeepEqual(values, tt.want[0]) {
				t.Errorf("SelectDB() result-0 = %v, want-0 %v", values, tt.want[0])
				return
			}

			err = server.SelectDB(tt.database)
			if tt.wantErr {
				if err == nil {
					t.Errorf("SelectDB() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("SelectDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check the values the new DB
			values, err = server.MGet("key1", "key2", "key3")
			if err != nil {
				t.Errorf("SelectDB() error = %v", err)
				return
			}

			if !reflect.DeepEqual(values, tt.want[1]) {
				t.Errorf("SelectDB() result-1 = %v, want-1 %v", values, tt.want[1])
				return
			}
		})
	}
}

func TestSugarDB_SetProtocol(t *testing.T) {
	t.Parallel()
	server := createSugarDB()
	tests := []struct {
		name     string
		protocol int
		wantErr  bool
	}{
		{
			name:     "1. Change protocol to 2",
			protocol: 2,
			wantErr:  false,
		},
		{
			name:     "2. Change protocol to 3",
			protocol: 3,
			wantErr:  false,
		},
		{
			name:     "3. Return error when protocol is neither 2 or 3",
			protocol: 4,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := server.SetProtocol(tt.protocol)
			if tt.wantErr {
				if err == nil {
					t.Errorf("SetProtocol() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("SetProtocol() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Check if the protocol has been changed
			if server.connInfo.embedded.Protocol != tt.protocol {
				t.Errorf("SetProtocol() protocol = %v, wantProtocol %v",
					server.connInfo.embedded.Protocol, tt.protocol)
			}
		})
	}
}
