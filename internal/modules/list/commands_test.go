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

package list_test

import (
	"errors"
	"fmt"
	"github.com/echovault/echovault/echovault"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/constants"
	"github.com/tidwall/resp"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var mockServer *echovault.EchoVault
var addr = "localhost"
var port int

func init() {
	port, _ = internal.GetFreePort()
	mockServer, _ = echovault.NewEchoVault(
		echovault.WithConfig(config.Config{
			BindAddr:       addr,
			Port:           uint16(port),
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		mockServer.Start()
	}()
	wg.Wait()
}

func Test_HandleLLEN(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name             string
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse int
		expectedError    error
	}{
		{
			name:             "1. If key exists and is a list, return the lists length",
			key:              "LlenKey1",
			presetValue:      []string{"value1", "value2", "value3", "value4"},
			command:          []string{"LLEN", "LlenKey1"},
			expectedResponse: 4,
			expectedError:    nil,
		},
		{
			name:             "2. If key does not exist, return 0",
			key:              "LlenKey2",
			presetValue:      nil,
			command:          []string{"LLEN", "LlenKey2"},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			key:              "LlenKey3",
			presetValue:      nil,
			command:          []string{"LLEN"},
			expectedResponse: 0,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			key:              "LlenKey4",
			presetValue:      nil,
			command:          []string{"LLEN", "LlenKey4", "LlenKey4"},
			expectedResponse: 0,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non-list returns error",
			key:              "LlenKey5",
			presetValue:      "Default value",
			command:          []string{"LLEN", "LlenKey5"},
			expectedResponse: 0,
			expectedError:    errors.New("LLEN command on non-list item"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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
				t.Errorf("expected response to be %d, got %d", test.expectedResponse, res.Integer())
			}
		})
	}
}

func Test_HandleLINDEX(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name             string
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse string
		expectedError    error
	}{
		{
			name:             "1. Return last element within range",
			key:              "LindexKey1",
			presetValue:      []string{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "LindexKey1", "3"},
			expectedResponse: "value4",
			expectedError:    nil,
		},
		{
			name:             "2. Return first element within range",
			key:              "LindexKey2",
			presetValue:      []string{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "LindexKey1", "0"},
			expectedResponse: "value1",
			expectedError:    nil,
		},
		{
			name:             "3. Return middle element within range",
			key:              "LindexKey3",
			presetValue:      []string{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "LindexKey1", "1"},
			expectedResponse: "value2",
			expectedError:    nil,
		},
		{
			name:             "4. If key does not exist, return error",
			key:              "LindexKey4",
			presetValue:      nil,
			command:          []string{"LINDEX", "LindexKey4", "0"},
			expectedResponse: "",
			expectedError:    errors.New("LINDEX command on non-list item"),
		},
		{
			name:             "5. Command too short",
			key:              "LindexKey3",
			presetValue:      nil,
			command:          []string{"LINDEX", "LindexKey3"},
			expectedResponse: "",
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             " 6. Command too long",
			key:              "LindexKey4",
			presetValue:      nil,
			command:          []string{"LINDEX", "LindexKey4", "0", "20"},
			expectedResponse: "",
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "7. Trying to get element by index on a non-list returns error",
			key:              "LindexKey5",
			presetValue:      "Default value",
			command:          []string{"LINDEX", "LindexKey5", "0"},
			expectedResponse: "",
			expectedError:    errors.New("LINDEX command on non-list item"),
		},
		{
			name:             "8. Trying to get index out of range index beyond last index",
			key:              "LindexKey6",
			presetValue:      []string{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "LindexKey6", "3"},
			expectedResponse: "",
			expectedError:    errors.New("index must be within list range"),
		},
		{
			name:             "9. Trying to get index out of range with negative index",
			key:              "LindexKey7",
			presetValue:      []string{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "LindexKey7", "-1"},
			expectedResponse: "",
			expectedError:    errors.New("index must be within list range"),
		},
		{
			name:             " 10. Return error when index is not an integer",
			key:              "LindexKey8",
			presetValue:      []string{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "LindexKey8", "index"},
			expectedResponse: "",
			expectedError:    errors.New("index must be an integer"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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
		})
	}
}

func Test_HandleLRANGE(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
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
			// Return sub-list within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			name:             "1. Return sub-list within range.",
			key:              "LrangeKey1",
			presetValue:      []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "LrangeKey1", "3", "6"},
			expectedResponse: []string{"value4", "value5", "value6", "value7"},
			expectedError:    nil,
		},
		{
			name:             "2. Return sub-list from start index to the end of the list when end index is -1",
			key:              "LrangeKey2",
			presetValue:      []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "LrangeKey2", "3", "-1"},
			expectedResponse: []string{"value4", "value5", "value6", "value7", "value8"},
			expectedError:    nil,
		},
		{
			name:             "3. Return the reversed sub-list when the end index is greater than -1 but less than start index",
			key:              "LrangeKey3",
			presetValue:      []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "LrangeKey3", "3", "0"},
			expectedResponse: []string{"value4", "value3", "value2", "value1"},
			expectedError:    nil,
		},
		{
			name:             "4. If key does not exist, return error",
			key:              "LrangeKey4",
			presetValue:      nil,
			command:          []string{"LRANGE", "LrangeKey4", "0", "2"},
			expectedResponse: nil,
			expectedError:    errors.New("LRANGE command on non-list item"),
		},
		{
			name:             "5. Command too short",
			key:              "LrangeKey5",
			presetValue:      nil,
			command:          []string{"LRANGE", "LrangeKey5"},
			expectedResponse: nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "6. Command too long",
			key:              "LrangeKey6",
			presetValue:      nil,
			command:          []string{"LRANGE", "LrangeKey6", "0", "element", "element"},
			expectedResponse: nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "7. Error when executing command on non-list command",
			key:              "LrangeKey5",
			presetValue:      "Default value",
			command:          []string{"LRANGE", "LrangeKey5", "0", "3"},
			expectedResponse: nil,
			expectedError:    errors.New("LRANGE command on non-list item"),
		},
		{
			name:             "8. Error when start index is less than 0",
			key:              "LrangeKey7",
			presetValue:      []string{"value1", "value2", "value3", "value4"},
			command:          []string{"LRANGE", "LrangeKey7", "-1", "3"},
			expectedResponse: nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{
			name:             "9. Error when start index is higher than the length of the list",
			key:              "LrangeKey8",
			presetValue:      []string{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey8", "10", "11"},
			expectedResponse: nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{
			name:             "10. Return error when start index is not an integer",
			key:              "LrangeKey9",
			presetValue:      []string{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey9", "start", "7"},
			expectedResponse: nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{
			name:             "11. Return error when end index is not an integer",
			key:              "LrangeKey10",
			presetValue:      []string{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey10", "0", "end"},
			expectedResponse: nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{
			name:             "12. Error when start and end indices are equal",
			key:              "LrangeKey11",
			presetValue:      []string{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey11", "1", "1"},
			expectedResponse: nil,
			expectedError:    errors.New("start and end indices cannot be equal"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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

			if len(res.Array()) != len(test.expectedResponse) {
				t.Errorf("expected response of length %d, got length %d", len(test.expectedResponse), len(res.Array()))
			}

			for _, item := range res.Array() {
				if !slices.Contains(test.expectedResponse, item.String()) {
					t.Errorf("unexpected element \"%s\" in response", item.String())
				}
			}
		})
	}
}

func Test_HandleLSET(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name          string
		key           string
		presetValue   interface{}
		command       []string
		expectedValue []string
		expectedError error
	}{
		{
			name:          "1. Return last element within range",
			key:           "LsetKey1",
			presetValue:   []string{"value1", "value2", "value3", "value4"},
			command:       []string{"LSET", "LsetKey1", "3", "new-value"},
			expectedValue: []string{"value1", "value2", "value3", "new-value"},
			expectedError: nil,
		},
		{
			name:          "2. Return first element within range",
			key:           "LsetKey2",
			presetValue:   []string{"value1", "value2", "value3", "value4"},
			command:       []string{"LSET", "LsetKey2", "0", "new-value"},
			expectedValue: []string{"new-value", "value2", "value3", "value4"},
			expectedError: nil,
		},
		{
			name:          "3. Return middle element within range",
			key:           "LsetKey3",
			presetValue:   []string{"value1", "value2", "value3", "value4"},
			command:       []string{"LSET", "LsetKey3", "1", "new-value"},
			expectedValue: []string{"value1", "new-value", "value3", "value4"},
			expectedError: nil,
		},
		{
			name:          "4. If key does not exist, return error",
			key:           "LsetKey4",
			presetValue:   nil,
			command:       []string{"LSET", "LsetKey4", "0", "element"},
			expectedValue: nil,
			expectedError: errors.New("LSET command on non-list item"),
		},
		{
			name:          "5. Command too short",
			key:           "LsetKey5",
			presetValue:   nil,
			command:       []string{"LSET", "LsetKey5"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "6. Command too long",
			key:           "LsetKey6",
			presetValue:   nil,
			command:       []string{"LSET", "LsetKey6", "0", "element", "element"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "7. Trying to get element by index on a non-list returns error",
			key:           "LsetKey5",
			presetValue:   "Default value",
			command:       []string{"LSET", "LsetKey5", "0", "element"},
			expectedValue: nil,
			expectedError: errors.New("LSET command on non-list item"),
		},
		{
			name:          "8. Trying to get index out of range index beyond last index",
			key:           "LsetKey6",
			presetValue:   []string{"value1", "value2", "value3"},
			command:       []string{"LSET", "LsetKey6", "3", "element"},
			expectedValue: nil,
			expectedError: errors.New("index must be within list range"),
		},
		{
			name:          "9. Trying to get index out of range with negative index",
			key:           "LsetKey7",
			presetValue:   []string{"value1", "value2", "value3"},
			command:       []string{"LSET", "LsetKey7", "-1", "element"},
			expectedValue: nil,
			expectedError: errors.New("index must be within list range"),
		},
		{
			name:          "10. Return error when index is not an integer",
			key:           "LsetKey8",
			presetValue:   []string{"value1", "value2", "value3"},
			command:       []string{"LSET", "LsetKey8", "index", "element"},
			expectedValue: nil,
			expectedError: errors.New("index must be an integer"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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

			if !strings.EqualFold(res.String(), "ok") {
				t.Errorf("expected response OK, got \"%s\"", res.String())
			}

			if err = client.WriteArray([]resp.Value{
				resp.StringValue("LRANGE"),
				resp.StringValue(test.key),
				resp.StringValue("0"),
				resp.StringValue("-1"),
			}); err != nil {
				t.Error(err)
			}

			res, _, err = client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if len(res.Array()) != len(test.expectedValue) {
				t.Errorf("expected list at key \"%s\" to be length %d, got %d",
					test.key, len(test.expectedValue), len(res.Array()))
			}

			for _, item := range res.Array() {
				if !slices.Contains(test.expectedValue, item.String()) {
					t.Errorf("unexpected value \"%s\" in updated list", item.String())
				}
			}
		})
	}
}

func Test_HandleLTRIM(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name          string
		key           string
		presetValue   interface{}
		command       []string
		expectedValue []string
		expectedError error
	}{
		{
			// Return trim within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			name:          "1. Return trim within range.",
			key:           "LtrimKey1",
			presetValue:   []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:       []string{"LTRIM", "LtrimKey1", "3", "6"},
			expectedValue: []string{"value4", "value5", "value6"},
			expectedError: nil,
		},
		{
			name:          "2. Return element from start index to end index when end index is greater than length of the list",
			key:           "LtrimKey2",
			presetValue:   []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:       []string{"LTRIM", "LtrimKey2", "5", "-1"},
			expectedValue: []string{"value6", "value7", "value8"},
			expectedError: nil,
		},
		{
			name:          "3. Return error when end index is smaller than start index but greater than -1",
			key:           "LtrimKey3",
			presetValue:   []string{"value1", "value2", "value3", "value4"},
			command:       []string{"LTRIM", "LtrimKey3", "3", "1"},
			expectedValue: nil,
			expectedError: errors.New("end index must be greater than start index or -1"),
		},
		{
			name:          "4. If key does not exist, return error",
			key:           "LtrimKey4",
			presetValue:   nil,
			command:       []string{"LTRIM", "LtrimKey4", "0", "2"},
			expectedValue: nil,
			expectedError: errors.New("LTRIM command on non-list item"),
		},
		{
			name:          "5. Command too short",
			key:           "LtrimKey5",
			presetValue:   nil,
			command:       []string{"LTRIM", "LtrimKey5"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "6. Command too long",
			key:           "LtrimKey6",
			presetValue:   nil,
			command:       []string{"LTRIM", "LtrimKey6", "0", "element", "element"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "7. Trying to get element by index on a non-list returns error",
			key:           "LtrimKey5",
			presetValue:   "Default value",
			command:       []string{"LTRIM", "LtrimKey5", "0", "3"},
			expectedValue: nil,
			expectedError: errors.New("LTRIM command on non-list item"),
		},
		{
			name:          "8. Error when start index is less than 0",
			key:           "LtrimKey7",
			presetValue:   []string{"value1", "value2", "value3", "value4"},
			command:       []string{"LTRIM", "LtrimKey7", "-1", "3"},
			expectedValue: nil,
			expectedError: errors.New("start index must be within list boundary"),
		},
		{
			name:          "9. Error when start index is higher than the length of the list",
			key:           "LtrimKey8",
			presetValue:   []string{"value1", "value2", "value3"},
			command:       []string{"LTRIM", "LtrimKey8", "10", "11"},
			expectedValue: nil,
			expectedError: errors.New("start index must be within list boundary"),
		},
		{
			name:          "10. Return error when start index is not an integer",
			key:           "LtrimKey9",
			presetValue:   []string{"value1", "value2", "value3"},
			command:       []string{"LTRIM", "LtrimKey9", "start", "7"},
			expectedValue: nil,
			expectedError: errors.New("start and end indices must be integers"),
		},
		{
			name:          "11. Return error when end index is not an integer",
			key:           "LtrimKey10",
			presetValue:   []string{"value1", "value2", "value3"},
			command:       []string{"LTRIM", "LtrimKey10", "0", "end"},
			expectedValue: nil,
			expectedError: errors.New("start and end indices must be integers"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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

			if !strings.EqualFold(res.String(), "ok") {
				t.Errorf("expected response OK, got \"%s\"", res.String())
			}

			if err = client.WriteArray([]resp.Value{
				resp.StringValue("LRANGE"),
				resp.StringValue(test.key),
				resp.StringValue("0"),
				resp.StringValue("-1"),
			}); err != nil {
				t.Error(err)
			}

			res, _, err = client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if len(res.Array()) != len(test.expectedValue) {
				t.Errorf("expected list at key \"%s\" to be length %d, got %d",
					test.key, len(test.expectedValue), len(res.Array()))
			}

			for _, item := range res.Array() {
				if !slices.Contains(test.expectedValue, item.String()) {
					t.Errorf("unexpected value \"%s\" in updated list", item.String())
				}
			}
		})
	}
}

func Test_HandleLREM(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name          string
		key           string
		presetValue   interface{}
		command       []string
		expectedValue []string
		expectedError error
	}{
		{
			name:          "1. Remove the first 3 elements that appear in the list",
			key:           "LremKey1",
			presetValue:   []string{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			command:       []string{"LREM", "LremKey1", "3", "4"},
			expectedValue: []string{"1", "2", "5", "6", "7", "8", "4", "9", "10", "5", "4"},
			expectedError: nil,
		},
		{
			name:          "2. Remove the last 3 elements that appear in the list",
			key:           "LremKey2",
			presetValue:   []string{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			command:       []string{"LREM", "LremKey2", "-3", "4"},
			expectedValue: []string{"1", "2", "4", "4", "5", "6", "7", "8", "9", "10", "5"},
			expectedError: nil,
		},
		{
			name:          "3. Command too short",
			key:           "LremKey3",
			presetValue:   nil,
			command:       []string{"LREM", "LremKey3"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "4. Command too long",
			key:           "LremKey4",
			presetValue:   nil,
			command:       []string{"LREM", "LremKey4", "0", "element", "element"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "5. Throw error when count is not an integer",
			key:           "LremKey5",
			presetValue:   nil,
			command:       []string{"LREM", "LremKey5", "count", "value1"},
			expectedValue: nil,
			expectedError: errors.New("count must be an integer"),
		},
		{
			name:          "6. Throw error on non-list item",
			key:           "LremKey6",
			presetValue:   "Default value",
			command:       []string{"LREM", "LremKey6", "0", "value1"},
			expectedValue: nil,
			expectedError: errors.New("LREM command on non-list item"),
		},
		{
			name:          "7. Throw error on non-existent item",
			key:           "LremKey7",
			presetValue:   "Default value",
			command:       []string{"LREM", "LremKey7", "0", "value1"},
			expectedValue: nil,
			expectedError: errors.New("LREM command on non-list item"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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

			if !strings.EqualFold(res.String(), "ok") {
				t.Errorf("expected response OK, got \"%s\"", res.String())
			}

			if err = client.WriteArray([]resp.Value{
				resp.StringValue("LRANGE"),
				resp.StringValue(test.key),
				resp.StringValue("0"),
				resp.StringValue("-1"),
			}); err != nil {
				t.Error(err)
			}

			res, _, err = client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if len(res.Array()) != len(test.expectedValue) {
				t.Errorf("expected list at key \"%s\" to be length %d, got %d",
					test.key, len(test.expectedValue), len(res.Array()))
			}

			for _, item := range res.Array() {
				if !slices.Contains(test.expectedValue, item.String()) {
					t.Errorf("unexpected value \"%s\" in updated list", item.String())
				}
			}
		})
	}
}

func Test_HandleLMOVE(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name          string
		presetValue   map[string]interface{}
		command       []string
		expectedValue map[string][]string
		expectedError error
	}{
		{
			name: "1. Move element from LEFT of left list to LEFT of right list",
			presetValue: map[string]interface{}{
				"source1":      []string{"one", "two", "three"},
				"destination1": []string{"one", "two", "three"},
			},
			command: []string{"LMOVE", "source1", "destination1", "LEFT", "LEFT"},
			expectedValue: map[string][]string{
				"source1":      {"two", "three"},
				"destination1": {"one", "one", "two", "three"},
			},
			expectedError: nil,
		},
		{
			name: "2. Move element from LEFT of left list to RIGHT of right list",
			presetValue: map[string]interface{}{
				"source2":      []string{"one", "two", "three"},
				"destination2": []string{"one", "two", "three"},
			},
			command: []string{"LMOVE", "source2", "destination2", "LEFT", "RIGHT"},
			expectedValue: map[string][]string{
				"source2":      {"two", "three"},
				"destination2": {"one", "two", "three", "one"},
			},
			expectedError: nil,
		},
		{
			name: "3. Move element from RIGHT of left list to LEFT of right list",
			presetValue: map[string]interface{}{
				"source3":      []string{"one", "two", "three"},
				"destination3": []string{"one", "two", "three"},
			},
			command: []string{"LMOVE", "source3", "destination3", "RIGHT", "LEFT"},
			expectedValue: map[string][]string{
				"source3":      {"one", "two"},
				"destination3": {"three", "one", "two", "three"},
			},
			expectedError: nil,
		},
		{
			name: "4. Move element from RIGHT of left list to RIGHT of right list",
			presetValue: map[string]interface{}{
				"source4":      []string{"one", "two", "three"},
				"destination4": []string{"one", "two", "three"},
			},
			command: []string{"LMOVE", "source4", "destination4", "RIGHT", "RIGHT"},
			expectedValue: map[string][]string{
				"source4":      {"one", "two"},
				"destination4": {"one", "two", "three", "three"},
			},
			expectedError: nil,
		},
		{
			name: "5. Throw error when the right list is non-existent",
			presetValue: map[string]interface{}{
				"source5": []string{"one", "two", "three"},
			},
			command:       []string{"LMOVE", "source5", "destination5", "LEFT", "LEFT"},
			expectedValue: nil,
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name: "6. Throw error when right list in not a list",
			presetValue: map[string]interface{}{
				"source6":      []string{"one", "two", "tree"},
				"destination6": "Default value",
			},
			command:       []string{"LMOVE", "source6", "destination6", "LEFT", "LEFT"},
			expectedValue: nil,
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name: "7. Throw error when left list is non-existent",
			presetValue: map[string]interface{}{
				"destination7": []string{"one", "two", "three"},
			},
			command:       []string{"LMOVE", "source7", "destination7", "LEFT", "LEFT"},
			expectedValue: nil,
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name: "8. Throw error when left list is not a list",
			presetValue: map[string]interface{}{
				"source8":      "Default value",
				"destination8": []string{"one", "two", "three"},
			},
			command:       []string{"LMOVE", "source8", "destination8", "LEFT", "LEFT"},
			expectedValue: nil,
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name:          "9. Throw error when command is too short",
			presetValue:   map[string]interface{}{},
			command:       []string{"LMOVE", "source9", "destination9"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "10. Throw error when command is too long",
			presetValue:   map[string]interface{}{},
			command:       []string{"LMOVE", "source10", "destination10", "LEFT", "LEFT", "RIGHT"},
			expectedValue: nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "11. Throw error when WHEREFROM argument is not LEFT/RIGHT",
			presetValue:   map[string]interface{}{},
			command:       []string{"LMOVE", "source11", "destination11", "UP", "RIGHT"},
			expectedValue: nil,
			expectedError: errors.New("wherefrom and whereto arguments must be either LEFT or RIGHT"),
		},
		{
			name:          "12. Throw error when WHERETO argument is not LEFT/RIGHT",
			presetValue:   map[string]interface{}{},
			command:       []string{"LMOVE", "source11", "destination11", "LEFT", "DOWN"},
			expectedValue: nil,
			expectedError: errors.New("wherefrom and whereto arguments must be either LEFT or RIGHT"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.presetValue != nil {
				for key, value := range test.presetValue {

					var command []resp.Value
					var expected string

					switch value.(type) {
					case string:
						command = []resp.Value{
							resp.StringValue("SET"),
							resp.StringValue(key),
							resp.StringValue(value.(string)),
						}
						expected = "ok"
					case []string:
						command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(key)}
						for _, element := range value.([]string) {
							command = append(command, []resp.Value{resp.StringValue(element)}...)
						}
						expected = strconv.Itoa(len(value.([]string)))
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

			if !strings.EqualFold(res.String(), "ok") {
				t.Errorf("expected response OK, got \"%s\"", res.String())
			}

			for key, list := range test.expectedValue {
				if err = client.WriteArray([]resp.Value{
					resp.StringValue("LRANGE"),
					resp.StringValue(key),
					resp.StringValue("0"),
					resp.StringValue("-1"),
				}); err != nil {
					t.Error(err)
				}

				res, _, err = client.ReadValue()
				if err != nil {
					t.Error(err)
				}

				if len(res.Array()) != len(list) {
					t.Errorf("expected list at key \"%s\" to be length %d, got %d",
						key, len(test.expectedValue), len(res.Array()))
				}

				for _, item := range res.Array() {
					if !slices.Contains(list, item.String()) {
						t.Errorf("unexpected value \"%s\" in updated list %s", item.String(), key)
					}
				}
			}
		})
	}
}

func Test_HandleLPUSH(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name             string
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse int
		expectedValue    []string
		expectedError    error
	}{
		{
			name:             "1. LPUSHX to existing list prepends the element to the list",
			key:              "LpushKey1",
			presetValue:      []string{"1", "2", "4", "5"},
			command:          []string{"LPUSHX", "LpushKey1", "value1", "value2"},
			expectedResponse: 6,
			expectedValue:    []string{"value1", "value2", "1", "2", "4", "5"},
			expectedError:    nil,
		},
		{
			name:             "2. LPUSH on existing list prepends the elements to the list",
			key:              "LpushKey2",
			presetValue:      []string{"1", "2", "4", "5"},
			command:          []string{"LPUSH", "LpushKey2", "value1", "value2"},
			expectedResponse: 6,
			expectedValue:    []string{"value1", "value2", "1", "2", "4", "5"},
			expectedError:    nil,
		},
		{
			name:             "3. LPUSH on non-existent list creates the list",
			key:              "LpushKey3",
			presetValue:      nil,
			command:          []string{"LPUSH", "LpushKey3", "value1", "value2"},
			expectedResponse: 2,
			expectedValue:    []string{"value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "4. Command too short",
			key:              "LpushKey5",
			presetValue:      nil,
			command:          []string{"LPUSH", "LpushKey5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. LPUSHX command returns error on non-existent list",
			key:              "LpushKey6",
			presetValue:      nil,
			command:          []string{"LPUSHX", "LpushKey7", "count", "value1"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LPUSHX command on non-existent key"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), res.Error().Error())
				}
				return
			}

			if res.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, res.Integer())
			}

			if err = client.WriteArray([]resp.Value{
				resp.StringValue("LRANGE"),
				resp.StringValue(test.key),
				resp.StringValue("0"),
				resp.StringValue("-1"),
			}); err != nil {
				t.Error(err)
			}

			res, _, err = client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if len(res.Array()) != len(test.expectedValue) {
				t.Errorf("expected list at key \"%s\" to be length %d, got %d",
					test.key, len(test.expectedValue), len(res.Array()))
			}

			for _, item := range res.Array() {
				if !slices.Contains(test.expectedValue, item.String()) {
					t.Errorf("unexpected value \"%s\" in updated list", item.String())
				}
			}
		})
	}
}

func Test_HandleRPUSH(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name             string
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse int
		expectedValue    []string
		expectedError    error
	}{
		{
			name:             "1. RPUSHX to existing list prepends the element to the list",
			key:              "RpushKey1",
			presetValue:      []string{"1", "2", "4", "5"},
			command:          []string{"RPUSHX", "RpushKey1", "value1", "value2"},
			expectedResponse: 6,
			expectedValue:    []string{"1", "2", "4", "5", "value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "2. RPUSH on existing list prepends the elements to the list",
			key:              "RpushKey2",
			presetValue:      []string{"1", "2", "4", "5"},
			command:          []string{"RPUSH", "RpushKey2", "value1", "value2"},
			expectedResponse: 6,
			expectedValue:    []string{"1", "2", "4", "5", "value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "3. RPUSH on non-existent list creates the list",
			key:              "RpushKey3",
			presetValue:      nil,
			command:          []string{"RPUSH", "RpushKey3", "value1", "value2"},
			expectedResponse: 2,
			expectedValue:    []string{"value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "4. Command too short",
			key:              "RpushKey5",
			presetValue:      nil,
			command:          []string{"RPUSH", "RpushKey5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. RPUSHX command returns error on non-existent list",
			key:              "RpushKey6",
			presetValue:      nil,
			command:          []string{"RPUSHX", "RpushKey7", "count", "value1"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("RPUSHX command on non-existent key"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), res.Error().Error())
				}
				return
			}

			if res.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, res.Integer())
			}

			if err = client.WriteArray([]resp.Value{
				resp.StringValue("LRANGE"),
				resp.StringValue(test.key),
				resp.StringValue("0"),
				resp.StringValue("-1"),
			}); err != nil {
				t.Error(err)
			}

			res, _, err = client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if len(res.Array()) != len(test.expectedValue) {
				t.Errorf("expected list at key \"%s\" to be length %d, got %d",
					test.key, len(test.expectedValue), len(res.Array()))
			}

			for _, item := range res.Array() {
				if !slices.Contains(test.expectedValue, item.String()) {
					t.Errorf("unexpected value \"%s\" in updated list", item.String())
				}
			}
		})
	}
}

func Test_HandlePOP(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		t.Error(err)
	}
	client := resp.NewConn(conn)

	tests := []struct {
		name             string
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse string
		expectedValue    []string
		expectedError    error
	}{
		{
			name:             "1. LPOP returns last element and removed first element from the list",
			key:              "PopKey1",
			presetValue:      []string{"value1", "value2", "value3", "value4"},
			command:          []string{"LPOP", "PopKey1"},
			expectedResponse: "value1",
			expectedValue:    []string{"value2", "value3", "value4"},
			expectedError:    nil,
		},
		{
			name:             "2. RPOP returns last element and removed last element from the list",
			key:              "PopKey2",
			presetValue:      []string{"value1", "value2", "value3", "value4"},
			command:          []string{"RPOP", "PopKey2"},
			expectedResponse: "value4",
			expectedValue:    []string{"value1", "value2", "value3"},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			key:              "PopKey3",
			presetValue:      nil,
			command:          []string{"LPOP"},
			expectedResponse: "",
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4.  Command too long",
			key:              "PopKey4",
			presetValue:      nil,
			command:          []string{"LPOP", "PopKey4", "PopKey4"},
			expectedResponse: "",
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to execute LPOP from a non-list item return an error",
			key:              "PopKey5",
			presetValue:      "Default value",
			command:          []string{"LPOP", "PopKey5"},
			expectedResponse: "",
			expectedValue:    nil,
			expectedError:    errors.New("LPOP command on non-list item"),
		},
		{
			name:             "6. Trying to execute RPOP from a non-list item return an error",
			key:              "PopKey6",
			presetValue:      "Default value",
			command:          []string{"RPOP", "PopKey6"},
			expectedResponse: "",
			expectedValue:    nil,
			expectedError:    errors.New("RPOP command on non-list item"),
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
				case []string:
					command = []resp.Value{resp.StringValue("LPUSH"), resp.StringValue(test.key)}
					for _, element := range test.presetValue.([]string) {
						command = append(command, []resp.Value{resp.StringValue(element)}...)
					}
					expected = strconv.Itoa(len(test.presetValue.([]string)))
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
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), res.Error().Error())
				}
				return
			}

			if res.String() != test.expectedResponse {
				t.Errorf("expected response %s, got %s", test.expectedResponse, res.String())
			}

			if err = client.WriteArray([]resp.Value{
				resp.StringValue("LRANGE"),
				resp.StringValue(test.key),
				resp.StringValue("0"),
				resp.StringValue("-1"),
			}); err != nil {
				t.Error(err)
			}

			res, _, err = client.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if len(res.Array()) != len(test.expectedValue) {
				t.Errorf("expected list at key \"%s\" to be length %d, got %d",
					test.key, len(test.expectedValue), len(res.Array()))
			}

			for _, item := range res.Array() {
				if !slices.Contains(test.expectedValue, item.String()) {
					t.Errorf("unexpected value \"%s\" in updated list", item.String())
				}
			}
		})
	}
}
