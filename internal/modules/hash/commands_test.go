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

package hash_test

import (
	"errors"
	"github.com/echovault/echovault/echovault"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/constants"
	"github.com/tidwall/resp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func Test_Hash(t *testing.T) {
	port, err := internal.GetFreePort()
	if err != nil {
		t.Error(err)
		return
	}

	mockServer, err := echovault.NewEchoVault(
		echovault.WithConfig(config.Config{
			BindAddr:       "localhost",
			Port:           uint16(port),
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
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

	t.Run("Test_HandleHSET", func(t *testing.T) {
		t.Parallel()
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := resp.NewConn(conn)

		// Tests for both HSet and HSetNX
		tests := []struct {
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse int // Change count
			expectedValue    map[string]string
			expectedError    error
		}{
			{
				name:             "1. HSETNX set field on non-existent hash map",
				key:              "HsetKey1",
				presetValue:      nil,
				command:          []string{"HSETNX", "HsetKey1", "field1", "value1"},
				expectedResponse: 1,
				expectedValue:    map[string]string{"field1": "value1"},
				expectedError:    nil,
			},
			{
				name:             "2. HSETNX set field on existing hash map",
				key:              "HsetKey2",
				presetValue:      map[string]string{"field1": "value1"},
				command:          []string{"HSETNX", "HsetKey2", "field2", "value2"},
				expectedResponse: 1,
				expectedValue:    map[string]string{"field1": "value1", "field2": "value2"},
				expectedError:    nil,
			},
			{
				name:             "3. HSETNX skips operation when setting on existing field",
				key:              "HsetKey3",
				presetValue:      map[string]string{"field1": "value1"},
				command:          []string{"HSETNX", "HsetKey3", "field1", "value1-new"},
				expectedResponse: 0,
				expectedValue:    map[string]string{"field1": "value1"},
				expectedError:    nil,
			},
			{
				name:             "4. Regular HSET command on non-existent hash map",
				key:              "HsetKey4",
				presetValue:      nil,
				command:          []string{"HSET", "HsetKey4", "field1", "value1", "field2", "value2"},
				expectedResponse: 2,
				expectedValue:    map[string]string{"field1": "value1", "field2": "value2"},
				expectedError:    nil,
			},
			{
				name:             "5. Regular HSET update on existing hash map",
				key:              "HsetKey5",
				presetValue:      map[string]string{"field1": "value1", "field2": "value2"},
				command:          []string{"HSET", "HsetKey5", "field1", "value1-new", "field2", "value2-ne2", "field3", "value3"},
				expectedResponse: 3,
				expectedValue:    map[string]string{"field1": "value1-new", "field2": "value2-ne2", "field3": "value3"},
				expectedError:    nil,
			},
			{
				name:             "6. HSET overwrites when the target key is not a map",
				key:              "HsetKey6",
				presetValue:      "Default preset value",
				command:          []string{"HSET", "HsetKey6", "field1", "value1"},
				expectedResponse: 1,
				expectedValue:    map[string]string{"field1": "value1"},
				expectedError:    nil,
			},
			{
				name:             "7. HSET returns error when there's a mismatch in key/values",
				key:              "HsetKey7",
				presetValue:      nil,
				command:          []string{"HSET", "HsetKey7", "field1", "value1", "field2"},
				expectedResponse: 0,
				expectedValue:    map[string]string{},
				expectedError:    errors.New("each field must have a corresponding value"),
			},
			{
				name:             "8. Command too short",
				key:              "HsetKey8",
				presetValue:      nil,
				command:          []string{"HSET", "field1"},
				expectedResponse: 0,
				expectedValue:    map[string]string{},
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if res.Integer() != test.expectedResponse {
					t.Errorf("expected response \"%d\", got \"%d\"", test.expectedResponse, res.Integer())
				}

				// Check that all the values are what is expected
				if err := client.WriteArray([]resp.Value{
					resp.StringValue("HGETALL"),
					resp.StringValue(test.key),
				}); err != nil {
					t.Error(err)
				}
				res, _, err = client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				for idx, field := range res.Array() {
					if idx%2 == 0 {
						if res.Array()[idx+1].String() != test.expectedValue[field.String()] {
							t.Errorf(
								"expected value \"%+v\" for field \"%s\", got \"%+v\"",
								test.expectedValue[field.String()], field.String(), res.Array()[idx+1].String(),
							)
						}
					}
				}
			})
		}
	})

	t.Run("Test_HandleHINCRBY", func(t *testing.T) {
		t.Parallel()
		conn, err := internal.GetConnection("localhost", port)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := resp.NewConn(conn)

		// Tests for both HIncrBy and HIncrByFloat
		tests := []struct {
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse string // Change count
			expectedValue    map[string]string
			expectedError    error
		}{
			{
				name:             "1. Increment by integer on non-existent hash should create a new one",
				key:              "HincrbyKey1",
				presetValue:      nil,
				command:          []string{"HINCRBY", "HincrbyKey1", "field1", "1"},
				expectedResponse: "1",
				expectedValue:    map[string]string{"field1": "1"},
				expectedError:    nil,
			},
			{
				name:             "2. Increment by float on non-existent hash should create one",
				key:              "HincrbyKey2",
				presetValue:      nil,
				command:          []string{"HINCRBYFLOAT", "HincrbyKey2", "field1", "3.142"},
				expectedResponse: "3.142",
				expectedValue:    map[string]string{"field1": "3.142"},
				expectedError:    nil,
			},
			{
				name:             "3. Increment by integer on existing hash",
				key:              "HincrbyKey3",
				presetValue:      map[string]string{"field1": "1"},
				command:          []string{"HINCRBY", "HincrbyKey3", "field1", "10"},
				expectedResponse: "11",
				expectedValue:    map[string]string{"field1": "11"},
				expectedError:    nil,
			},
			{
				name:             "4. Increment by float on an existing hash",
				key:              "HincrbyKey4",
				presetValue:      map[string]string{"field1": "3.142"},
				command:          []string{"HINCRBYFLOAT", "HincrbyKey4", "field1", "3.142"},
				expectedResponse: "6.284",
				expectedValue:    map[string]string{"field1": "6.284"},
				expectedError:    nil,
			},
			{
				name:             "5. Command too short",
				key:              "HincrbyKey5",
				presetValue:      nil,
				command:          []string{"HINCRBY", "HincrbyKey5"},
				expectedResponse: "0",
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "6. Command too long",
				key:              "HincrbyKey6",
				presetValue:      nil,
				command:          []string{"HINCRBY", "HincrbyKey6", "field1", "23", "45"},
				expectedResponse: "0",
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "7. Error when increment by float does not pass valid float",
				key:              "HincrbyKey7",
				presetValue:      nil,
				command:          []string{"HINCRBYFLOAT", "HincrbyKey7", "field1", "three point one four two"},
				expectedResponse: "0",
				expectedValue:    nil,
				expectedError:    errors.New("increment must be a float"),
			},
			{
				name:             "8. Error when increment does not pass valid integer",
				key:              "HincrbyKey8",
				presetValue:      nil,
				command:          []string{"HINCRBY", "HincrbyKey8", "field1", "three"},
				expectedResponse: "0",
				expectedValue:    nil,
				expectedError:    errors.New("increment must be an integer"),
			},
			{
				name:             "9. Error when trying to increment on a key that is not a hash",
				key:              "HincrbyKey9",
				presetValue:      "Default value",
				command:          []string{"HINCRBY", "HincrbyKey9", "field1", "3"},
				expectedResponse: "0",
				expectedValue:    nil,
				expectedError:    errors.New("value at HincrbyKey9 is not a hash"),
			},
			{
				name:             "10. Error when trying to increment a hash field that is not a number",
				key:              "HincrbyKey10",
				presetValue:      map[string]string{"field1": "value1"},
				command:          []string{"HINCRBY", "HincrbyKey10", "field1", "3"},
				expectedResponse: "0",
				expectedValue:    nil,
				expectedError:    errors.New("value at field field1 is not a number"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if res.String() != test.expectedResponse {
					t.Errorf("expected response \"%s\", got \"%s\"", test.expectedResponse, res.String())
				}

				// Check that all the values are what is expected
				if err := client.WriteArray([]resp.Value{
					resp.StringValue("HGETALL"),
					resp.StringValue(test.key),
				}); err != nil {
					t.Error(err)
				}
				res, _, err = client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				for idx, field := range res.Array() {
					if idx%2 == 0 {
						if res.Array()[idx+1].String() != test.expectedValue[field.String()] {
							t.Errorf(
								"expected value \"%+v\" for field \"%s\", got \"%+v\"",
								test.expectedValue[field.String()], field.String(), res.Array()[idx+1].String(),
							)
						}
					}
				}
			})
		}
	})

	t.Run("Test_HandleHGET", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse []string // Change count
			expectedValue    map[string]string
			expectedError    error
		}{
			{
				name:             "1. Get values from existing hash.",
				key:              "HgetKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "365", "field3": "3.142"},
				command:          []string{"HGET", "HgetKey1", "field1", "field2", "field3", "field4"},
				expectedResponse: []string{"value1", "365", "3.142", ""},
				expectedValue:    map[string]string{"field1": "value1", "field2": "365", "field3": "3.142"},
				expectedError:    nil,
			},
			{
				name:             "2. Return nil when attempting to get from non-existed key",
				key:              "HgetKey2",
				presetValue:      nil,
				command:          []string{"HGET", "HgetKey2", "field1"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    nil,
			},
			{
				name:             "3. Error when trying to get from a value that is not a hash map",
				key:              "HgetKey3",
				presetValue:      "Default Value",
				command:          []string{"HGET", "HgetKey3", "field1"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New("value at HgetKey3 is not a hash"),
			},
			{
				name:             "4. Command too short",
				key:              "HgetKey4",
				presetValue:      nil,
				command:          []string{"HGET", "HgetKey4"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if test.expectedResponse == nil {
					if !res.IsNull() {
						t.Errorf("expected nil response, got %+v", res)
					}
					return
				}

				for _, item := range res.Array() {
					if !slices.Contains(test.expectedResponse, item.String()) {
						t.Errorf("unexpected element \"%s\" in response", item.String())
					}
				}

				// Check that all the values are what is expected
				if err := client.WriteArray([]resp.Value{
					resp.StringValue("HGETALL"),
					resp.StringValue(test.key),
				}); err != nil {
					t.Error(err)
				}
				res, _, err = client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				for idx, field := range res.Array() {
					if idx%2 == 0 {
						if res.Array()[idx+1].String() != test.expectedValue[field.String()] {
							t.Errorf(
								"expected value \"%+v\" for field \"%s\", got \"%+v\"",
								test.expectedValue[field.String()], field.String(), res.Array()[idx+1].String(),
							)
						}
					}
				}
			})
		}
	})

	t.Run("Test_HandleHMGET", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse []string // Change count
			expectedValue    map[string]string
			expectedError    error
		}{
			{
				name:             "1. Get values from existing hash.",
				key:              "HmgetKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "365", "field3": "3.142"},
				command:          []string{"HMGET", "HmgetKey1", "field1", "field2", "field3", "field4"},
				expectedResponse: []string{"value1", "365", "3.142", ""},
				expectedValue:    map[string]string{"field1": "value1", "field2": "365", "field3": "3.142"},
				expectedError:    nil,
			},
			{
				name:             "2. Return nil when attempting to get from non-existed key",
				key:              "HmgetKey2",
				presetValue:      nil,
				command:          []string{"HMGET", "HmgetKey2", "field1"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    nil,
			},
			{
				name:             "3. Error when trying to get from a value that is not a hash map",
				key:              "HmgetKey3",
				presetValue:      "Default Value",
				command:          []string{"HMGET", "HmgetKey3", "field1"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New("value at HgetKey3 is not a hash"),
			},
			{
				name:             "4. Command too short",
				key:              "HmgetKey4",
				presetValue:      nil,
				command:          []string{"HMGET", "HmgetKey4"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if test.expectedResponse == nil {
					if !res.IsNull() {
						t.Errorf("expected nil response, got %+v", res)
					}
					return
				}

				for _, item := range res.Array() {
					if !slices.Contains(test.expectedResponse, item.String()) {
						t.Errorf("unexpected element \"%s\" in response", item.String())
					}
				}

				// Check that all the values are what is expected
				if err := client.WriteArray([]resp.Value{
					resp.StringValue("HGETALL"),
					resp.StringValue(test.key),
				}); err != nil {
					t.Error(err)
				}

				res, _, err = client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				for idx, field := range res.Array() {
					if idx%2 == 0 {
						if res.Array()[idx+1].String() != test.expectedValue[field.String()] {
							t.Errorf(
								"expected value \"%+v\" for field \"%s\", got \"%+v\"",
								test.expectedValue[field.String()], field.String(), res.Array()[idx+1].String(),
							)
						}
					}
				}
			})
		}
	})

	t.Run("Test_HandleHSTRLEN", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse []int // Change count
			expectedValue    map[string]string
			expectedError    error
		}{
			{
				// Return lengths of field values.
				// If the key does not exist, its length should be 0.
				name:             "1. Return lengths of field values.",
				key:              "HstrlenKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HSTRLEN", "HstrlenKey1", "field1", "field2", "field3", "field4"},
				expectedResponse: []int{len("value1"), len("123456789"), len("3.142"), 0},
				expectedValue:    map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				expectedError:    nil,
			},
			{
				name:             "2. Nil response when trying to get HSTRLEN non-existent key",
				key:              "HstrlenKey2",
				presetValue:      nil,
				command:          []string{"HSTRLEN", "HstrlenKey2", "field1"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    nil,
			},
			{
				name:             "3. Command too short",
				key:              "HstrlenKey3",
				presetValue:      nil,
				command:          []string{"HSTRLEN", "HstrlenKey3"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "4. Trying to get lengths on a non hash map returns error",
				key:              "HstrlenKey4",
				presetValue:      "Default value",
				command:          []string{"HSTRLEN", "HstrlenKey4", "field1"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New("value at HstrlenKey4 is not a hash"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if test.expectedResponse == nil {
					if !res.IsNull() {
						t.Errorf("expected nil response, got %+v", res)
					}
					return
				}

				for _, item := range res.Array() {
					if !slices.Contains(test.expectedResponse, item.Integer()) {
						t.Errorf("unexpected element \"%d\" in response", item.Integer())
					}
				}

				// Check that all the values are what is expected
				if err := client.WriteArray([]resp.Value{
					resp.StringValue("HGETALL"),
					resp.StringValue(test.key),
				}); err != nil {
					t.Error(err)
				}
				res, _, err = client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				for idx, field := range res.Array() {
					if idx%2 == 0 {
						if res.Array()[idx+1].String() != test.expectedValue[field.String()] {
							t.Errorf(
								"expected value \"%+v\" for field \"%s\", got \"%+v\"",
								test.expectedValue[field.String()], field.String(), res.Array()[idx+1].String(),
							)
						}
					}
				}
			})
		}
	})

	t.Run("Test_HandleHVALS", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse []string
			expectedValue    map[string]string
			expectedError    error
		}{
			{
				name:             "1. Return all the values from a hash",
				key:              "HvalsKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HVALS", "HvalsKey1"},
				expectedResponse: []string{"value1", "123456789", "3.142"},
				expectedValue:    map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				expectedError:    nil,
			},
			{
				name:             "2. Empty array response when trying to get HSTRLEN non-existent key",
				key:              "HvalsKey2",
				presetValue:      nil,
				command:          []string{"HVALS", "HvalsKey2"},
				expectedResponse: []string{},
				expectedValue:    nil,
				expectedError:    nil,
			},
			{
				name:             "3. Command too short",
				key:              "HvalsKey3",
				presetValue:      nil,
				command:          []string{"HVALS"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "4. Command too long",
				key:              "HvalsKey4",
				presetValue:      nil,
				command:          []string{"HVALS", "HvalsKey4", "HvalsKey4"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "5. Trying to get lengths on a non hash map returns error",
				key:              "HvalsKey5",
				presetValue:      "Default value",
				command:          []string{"HVALS", "HvalsKey5"},
				expectedResponse: nil,
				expectedValue:    nil,
				expectedError:    errors.New("value at HvalsKey5 is not a hash"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if test.expectedResponse == nil {
					if !res.IsNull() {
						t.Errorf("expected nil response, got %+v", res)
					}
					return
				}

				for _, item := range res.Array() {
					if !slices.Contains(test.expectedResponse, item.String()) {
						t.Errorf("unexpected element \"%s\" in response", item.String())
					}
				}
			})
		}
	})

	t.Run("Test_HandleHRANDFIELD", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse []string
			expectedError    error
		}{
			{
				name:             "1. Get a random field",
				key:              "HrandfieldKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HRANDFIELD", "HrandfieldKey1"},
				expectedResponse: []string{"field1", "field2", "field3"},
				expectedError:    nil,
			},
			{
				name:             "2. Get a random field with a value",
				key:              "HrandfieldKey2",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HRANDFIELD", "HrandfieldKey2", "1", "WITHVALUES"},
				expectedResponse: []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
				expectedError:    nil,
			},
			{
				name: "3.  Get several random fields",
				key:  "HrandfieldKey3",
				presetValue: map[string]string{
					"field1": "value1",
					"field2": "123456789",
					"field3": "3.142",
					"field4": "value4",
					"field5": "value5",
				},
				command:          []string{"HRANDFIELD", "HrandfieldKey3", "3"},
				expectedResponse: []string{"field1", "field2", "field3", "field4", "field5"},
				expectedError:    nil,
			},
			{
				name: "4. Get several random fields with their corresponding values",
				key:  "HrandfieldKey4",
				presetValue: map[string]string{
					"field1": "value1",
					"field2": "123456789",
					"field3": "3.142",
					"field4": "value4",
					"field5": "value5",
				},
				command: []string{"HRANDFIELD", "HrandfieldKey4", "3", "WITHVALUES"},
				expectedResponse: []string{
					"field1", "value1", "field2", "123456789", "field3",
					"3.142", "field4", "value4", "field5", "value5",
				},
				expectedError: nil,
			},
			{
				name: "5. Get the entire hash",
				key:  "HrandfieldKey5",
				presetValue: map[string]string{
					"field1": "value1",
					"field2": "123456789",
					"field3": "3.142",
					"field4": "value4",
					"field5": "value5",
				},
				command:          []string{"HRANDFIELD", "HrandfieldKey5", "5"},
				expectedResponse: []string{"field1", "field2", "field3", "field4", "field5"},
				expectedError:    nil,
			},
			{
				name: "6. Get the entire hash with values",
				key:  "HrandfieldKey5",
				presetValue: map[string]string{
					"field1": "value1",
					"field2": "123456789",
					"field3": "3.142",
					"field4": "value4",
					"field5": "value5",
				},
				command: []string{"HRANDFIELD", "HrandfieldKey5", "5", "WITHVALUES"},
				expectedResponse: []string{
					"field1", "value1", "field2", "123456789", "field3",
					"3.142", "field4", "value4", "field5", "value5",
				},
				expectedError: nil,
			},
			{
				name:          "7. Command too short",
				key:           "HrandfieldKey10",
				presetValue:   nil,
				command:       []string{"HRANDFIELD"},
				expectedError: errors.New(constants.WrongArgsResponse),
			},
			{
				name:          "8. Command too long",
				key:           "HrandfieldKey11",
				presetValue:   nil,
				command:       []string{"HRANDFIELD", "HrandfieldKey11", "HrandfieldKey11", "HrandfieldKey11", "HrandfieldKey11"},
				expectedError: errors.New(constants.WrongArgsResponse),
			},
			{
				name:          "9. Trying to get random field on a non hash map returns error",
				key:           "HrandfieldKey12",
				presetValue:   "Default value",
				command:       []string{"HRANDFIELD", "HrandfieldKey12"},
				expectedError: errors.New("value at HrandfieldKey12 is not a hash"),
			},
			{
				name:          "10. Throw error when count provided is not an integer",
				key:           "HrandfieldKey12",
				presetValue:   "Default value",
				command:       []string{"HRANDFIELD", "HrandfieldKey12", "COUNT"},
				expectedError: errors.New("count must be an integer"),
			},
			{
				name:          "11. If fourth argument is provided, it must be \"WITHVALUES\"",
				key:           "HrandfieldKey12",
				presetValue:   "Default value",
				command:       []string{"HRANDFIELD", "HrandfieldKey12", "10", "FLAG"},
				expectedError: errors.New("result modifier must be withvalues"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if test.expectedResponse == nil {
					if !res.IsNull() {
						t.Errorf("expected nil response, got %+v", res)
					}
					return
				}

				for _, item := range res.Array() {
					if !slices.Contains(test.expectedResponse, item.String()) {
						t.Errorf("unexpected element \"%s\" in response", item.String())
					}
				}
			})
		}
	})

	t.Run("Test_HandleHLEN", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse int // Change count
			expectedError    error
		}{
			{
				name:             "1. Return the correct length of the hash",
				key:              "HlenKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HLEN", "HlenKey1"},
				expectedResponse: 3,
				expectedError:    nil,
			},
			{
				name:             "2. 0 response when trying to call HLEN on non-existent key",
				key:              "HlenKey2",
				presetValue:      nil,
				command:          []string{"HLEN", "HlenKey2"},
				expectedResponse: 0,
				expectedError:    nil,
			},
			{
				name:             "3. Command too short",
				key:              "HlenKey3",
				presetValue:      nil,
				command:          []string{"HLEN"},
				expectedResponse: 0,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "4. Command too long",
				presetValue:      nil,
				command:          []string{"HLEN", "HlenKey4", "HlenKey4"},
				expectedResponse: 0,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "5. Trying to get lengths on a non hash map returns error",
				key:              "HlenKey5",
				presetValue:      "Default value",
				command:          []string{"HLEN", "HlenKey5"},
				expectedResponse: 0,
				expectedError:    errors.New("value at HlenKey5 is not a hash"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if res.Integer() != test.expectedResponse {
					t.Errorf("expected response %d, got %d", test.expectedResponse, res.Integer())
				}
			})
		}
	})

	t.Run("Test_HandleHKeys", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse []string
			expectedError    error
		}{
			{
				name:             "1. Return an array containing all the keys of the hash",
				key:              "HkeysKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HKEYS", "HkeysKey1"},
				expectedResponse: []string{"field1", "field2", "field3"},
				expectedError:    nil,
			},
			{
				name:             "2. Empty array response when trying to call HKEYS on non-existent key",
				key:              "HkeysKey2",
				presetValue:      nil,
				command:          []string{"HKEYS", "HkeysKey2"},
				expectedResponse: []string{},
				expectedError:    nil,
			},
			{
				name:             "3. Command too short",
				key:              "HkeysKey3",
				presetValue:      nil,
				command:          []string{"HKEYS"},
				expectedResponse: nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "4. Command too long",
				key:              "HkeysKey4",
				presetValue:      nil,
				command:          []string{"HKEYS", "HkeysKey4", "HkeysKey4"},
				expectedResponse: nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:          "5. Trying to get lengths on a non hash map returns error",
				key:           "HkeysKey5",
				presetValue:   "Default value",
				command:       []string{"HKEYS", "HkeysKey5"},
				expectedError: errors.New("value at HkeysKey5 is not a hash"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				for _, item := range res.Array() {
					if !slices.Contains(test.expectedResponse, item.String()) {
						t.Errorf("unexpected value \"%s\" in response", item.String())
					}
				}
			})
		}
	})

	t.Run("Test_HandleHGETALL", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse map[string]string
			expectedError    error
		}{
			{
				name:             "1. Return an array containing all the fields and values of the hash",
				key:              "HGetAllKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HGETALL", "HGetAllKey1"},
				expectedResponse: map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				expectedError:    nil,
			},
			{
				name:             "2. Empty array response when trying to call HGETALL on non-existent key",
				key:              "HGetAllKey2",
				presetValue:      nil,
				command:          []string{"HGETALL", "HGetAllKey2"},
				expectedResponse: nil,
				expectedError:    nil,
			},
			{
				name:             "3. Command too short",
				key:              "HGetAllKey3",
				presetValue:      nil,
				command:          []string{"HGETALL"},
				expectedResponse: nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "4. Command too long",
				key:              "HGetAllKey4",
				presetValue:      nil,
				command:          []string{"HGETALL", "HGetAllKey4", "HGetAllKey4"},
				expectedResponse: nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "5. Trying to get lengths on a non hash map returns error",
				key:              "HGetAllKey5",
				presetValue:      "Default value",
				command:          []string{"HGETALL", "HGetAllKey5"},
				expectedResponse: nil,
				expectedError:    errors.New("value at HGetAllKey5 is not a hash"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if test.expectedResponse == nil {
					if len(res.Array()) != 0 {
						t.Errorf("expected response to be empty array, got %+v", res)
					}
					return
				}

				for i, item := range res.Array() {
					if i%2 == 0 {
						field := item.String()
						value := res.Array()[i+1].String()
						if test.expectedResponse[field] != value {
							t.Errorf("expected value at field \"%s\" to be \"%s\", got \"%s\"", field, test.expectedResponse[field], value)
						}
					}
				}

			})
		}
	})

	t.Run("Test_HandleHEXISTS", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse bool
			expectedError    error
		}{
			{
				name:             "1. Return 1 if the field exists in the hash",
				key:              "HexistsKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142"},
				command:          []string{"HEXISTS", "HexistsKey1", "field1"},
				expectedResponse: true,
				expectedError:    nil,
			},
			{
				name:             "2. 0 response when trying to call HEXISTS on non-existent key",
				key:              "HexistsKey2",
				presetValue:      nil,
				command:          []string{"HEXISTS", "HexistsKey2", "field1"},
				expectedResponse: false,
				expectedError:    nil,
			},
			{
				name:             "3. Command too short",
				key:              "HexistsKey3",
				presetValue:      nil,
				command:          []string{"HEXISTS", "HexistsKey3"},
				expectedResponse: false,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "4. Command too long",
				key:              "HexistsKey4",
				presetValue:      nil,
				command:          []string{"HEXISTS", "HexistsKey4", "field1", "field2"},
				expectedResponse: false,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "5. Trying to get lengths on a non hash map returns error",
				key:              "HexistsKey5",
				presetValue:      "Default value",
				command:          []string{"HEXISTS", "HexistsKey5", "field1"},
				expectedResponse: false,
				expectedError:    errors.New("value at HexistsKey5 is not a hash"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if res.Bool() != test.expectedResponse {
					t.Errorf("expected response to be %v, got %v", test.expectedResponse, res.Bool())
				}
			})
		}
	})

	t.Run("Test_HandleHDEL", func(t *testing.T) {
		t.Parallel()
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
			name             string
			key              string
			presetValue      interface{}
			command          []string
			expectedResponse int
			expectedValue    map[string]string
			expectedError    error
		}{
			{
				name:             "1. Return count of deleted fields in the specified hash",
				key:              "HdelKey1",
				presetValue:      map[string]string{"field1": "value1", "field2": "123456789", "field3": "3.142", "field7": "value7"},
				command:          []string{"HDEL", "HdelKey1", "field1", "field2", "field3", "field4", "field5", "field6"},
				expectedResponse: 3,
				expectedValue:    map[string]string{"field7": "value7"},
				expectedError:    nil,
			},
			{
				name:             "2. 0 response when passing delete fields that are non-existent on valid hash",
				key:              "HdelKey2",
				presetValue:      map[string]string{"field1": "value1", "field2": "value2", "field3": "value3"},
				command:          []string{"HDEL", "HdelKey2", "field4", "field5", "field6"},
				expectedResponse: 0,
				expectedValue:    map[string]string{"field1": "value1", "field2": "value2", "field3": "value3"},
				expectedError:    nil,
			},
			{
				name:             "3. 0 response when trying to call HDEL on non-existent key",
				key:              "HdelKey3",
				presetValue:      nil,
				command:          []string{"HDEL", "HdelKey3", "field1"},
				expectedResponse: 0,
				expectedValue:    nil,
				expectedError:    nil,
			},
			{
				name:             "4. Command too short",
				key:              "HdelKey4",
				presetValue:      nil,
				command:          []string{"HDEL", "HdelKey4"},
				expectedResponse: 0,
				expectedValue:    nil,
				expectedError:    errors.New(constants.WrongArgsResponse),
			},
			{
				name:             "5. Trying to get lengths on a non hash map returns error",
				key:              "HdelKey5",
				presetValue:      "Default value",
				command:          []string{"HDEL", "HdelKey5", "field1"},
				expectedResponse: 0,
				expectedValue:    nil,
				expectedError:    errors.New("value at HdelKey5 is not a hash"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if test.presetValue != nil {
					var command []resp.Value
					var expected string

					switch test.presetValue.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(test.key),
							resp.StringValue(test.presetValue.(string)),
						}
						expected = "ok"
					case map[string]string:
						command = []resp.Value{resp.StringValue("HSET"), resp.StringValue(test.key)}
						for key, value := range test.presetValue.(map[string]string) {
							command = append(command, []resp.Value{
								resp.StringValue(key),
								resp.StringValue(value)}...,
							)
						}
						expected = strconv.Itoa(len(test.presetValue.(map[string]string)))
					}

					if err = client.WriteArray(command); err != nil {
						t.Error(err)
					}
					res, _, err := client.ReadValue()
					if err != nil {
						t.Error(err)
					}

					if !strings.EqualFold(res.String(), expected) {
						t.Errorf("expected preset response to be \"%s\", got %s", expected, res.String())
					}
				}

				command := make([]resp.Value, len(test.command))
				for i, c := range test.command {
					command[i] = resp.StringValue(c)
				}

				if err = client.WriteArray(command); err != nil {
					t.Error(err)
				}
				res, _, err := client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if test.expectedError != nil {
					if !strings.Contains(res.Error().Error(), test.expectedError.Error()) {
						t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
					}
					return
				}

				if res.Integer() != test.expectedResponse {
					t.Errorf("expected response %d, got %d", test.expectedResponse, res.Integer())
				}

				for idx, field := range res.Array() {
					if idx%2 == 0 {
						if res.Array()[idx+1].String() != test.expectedValue[field.String()] {
							t.Errorf(
								"expected value \"%+v\" for field \"%s\", got \"%+v\"",
								test.expectedValue[field.String()], field.String(), res.Array()[idx+1].String(),
							)
						}
					}
				}
			})
		}
	})
}
