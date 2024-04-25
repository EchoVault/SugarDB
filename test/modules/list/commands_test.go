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

package list

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/echovault"
	"github.com/tidwall/resp"
	"net"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

var mockServer *echovault.EchoVault

func init() {
	mockServer, _ = echovault.NewEchoVault(
		echovault.WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)
}

func getUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

func getHandler(commands ...string) internal.HandlerFunc {
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

func getHandlerFuncParams(ctx context.Context, cmd []string, conn *net.Conn) internal.HandlerFuncParams {
	return internal.HandlerFuncParams{
		Context:          ctx,
		Command:          cmd,
		Connection:       conn,
		KeyExists:        mockServer.KeyExists,
		CreateKeyAndLock: mockServer.CreateKeyAndLock,
		KeyLock:          mockServer.KeyLock,
		KeyRLock:         mockServer.KeyRLock,
		KeyUnlock:        mockServer.KeyUnlock,
		KeyRUnlock:       mockServer.KeyRUnlock,
		GetValue:         mockServer.GetValue,
		SetValue:         mockServer.SetValue,
	}
}

func Test_HandleLLEN(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			name:             "1. If key exists and is a list, return the lists length",
			preset:           true,
			key:              "LlenKey1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LLEN", "LlenKey1"},
			expectedResponse: 4,
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "2. If key does not exist, return 0",
			preset:           false,
			key:              "LlenKey2",
			presetValue:      nil,
			command:          []string{"LLEN", "LlenKey2"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "LlenKey3",
			presetValue:      nil,
			command:          []string{"LLEN"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			preset:           false,
			key:              "LlenKey4",
			presetValue:      nil,
			command:          []string{"LLEN", "LlenKey4", "LlenKey4"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non-list returns error",
			preset:           true,
			key:              "LlenKey5",
			presetValue:      "Default value",
			command:          []string{"LLEN", "LlenKey5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LLEN command on non-list item"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LLEN, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.Integer() != test.expectedResponse {
				t.Errorf("expected integer response \"%d\", got \"%d\"", test.expectedResponse, rv.Integer())
			}
		})
	}
}

func Test_HandleLINDEX(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			name:             "1. Return last element within range",
			preset:           true,
			key:              "LindexKey1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "LindexKey1", "3"},
			expectedResponse: "value4",
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "2. Return first element within range",
			preset:           true,
			key:              "LindexKey2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "LindexKey1", "0"},
			expectedResponse: "value1",
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "3. Return middle element within range",
			preset:           true,
			key:              "LindexKey3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "LindexKey1", "1"},
			expectedResponse: "value2",
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "4. If key does not exist, return error",
			preset:           false,
			key:              "LindexKey4",
			presetValue:      nil,
			command:          []string{"LINDEX", "LindexKey4", "0"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LINDEX command on non-list item"),
		},
		{
			name:             "5. Command too short",
			preset:           false,
			key:              "LindexKey3",
			presetValue:      nil,
			command:          []string{"LINDEX", "LindexKey3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             " 6. Command too long",
			preset:           false,
			key:              "LindexKey4",
			presetValue:      nil,
			command:          []string{"LINDEX", "LindexKey4", "0", "20"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "7. Trying to get element by index on a non-list returns error",
			preset:           true,
			key:              "LindexKey5",
			presetValue:      "Default value",
			command:          []string{"LINDEX", "LindexKey5", "0"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LINDEX command on non-list item"),
		},
		{
			name:             "8. Trying to get index out of range index beyond last index",
			preset:           true,
			key:              "LindexKey6",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "LindexKey6", "3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{
			name:             "9. Trying to get index out of range with negative index",
			preset:           true,
			key:              "LindexKey7",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "LindexKey7", "-1"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{
			name:             " 10. Return error when index is not an integer",
			preset:           false,
			key:              "LindexKey8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "LindexKey8", "index"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be an integer"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LINDEX, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected response \"%s\", got \"%s\"", test.expectedResponse, rv.String())
			}
		})
	}
}

func Test_HandleLRANGE(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse []interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			// Return sub-list within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			name:             "1. Return sub-list within range.",
			preset:           true,
			key:              "LrangeKey1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "LrangeKey1", "3", "6"},
			expectedResponse: []interface{}{"value4", "value5", "value6", "value7"},
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "2. Return sub-list from start index to the end of the list when end index is -1",
			preset:           true,
			key:              "LrangeKey2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "LrangeKey2", "3", "-1"},
			expectedResponse: []interface{}{"value4", "value5", "value6", "value7", "value8"},
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "3. Return the reversed sub-list when the end index is greater than -1 but less than start index",
			preset:           true,
			key:              "LrangeKey3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "LrangeKey3", "3", "0"},
			expectedResponse: []interface{}{"value4", "value3", "value2", "value1"},
			expectedValue:    nil,
			expectedError:    nil,
		},
		{
			name:             "4. If key does not exist, return error",
			preset:           false,
			key:              "LrangeKey4",
			presetValue:      nil,
			command:          []string{"LRANGE", "LrangeKey4", "0", "2"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LRANGE command on non-list item"),
		},
		{
			name:             "5. Command too short",
			preset:           false,
			key:              "LrangeKey5",
			presetValue:      nil,
			command:          []string{"LRANGE", "LrangeKey5"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "6. Command too long",
			preset:           false,
			key:              "LrangeKey6",
			presetValue:      nil,
			command:          []string{"LRANGE", "LrangeKey6", "0", "element", "element"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "7. Error when executing command on non-list command",
			preset:           true,
			key:              "LrangeKey5",
			presetValue:      "Default value",
			command:          []string{"LRANGE", "LrangeKey5", "0", "3"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LRANGE command on non-list item"),
		},
		{
			name:             "8. Error when start index is less than 0",
			preset:           true,
			key:              "LrangeKey7",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LRANGE", "LrangeKey7", "-1", "3"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{
			name:             "9. Error when start index is higher than the length of the list",
			preset:           true,
			key:              "LrangeKey8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey8", "10", "11"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{
			name:             "10. Return error when start index is not an integer",
			preset:           false,
			key:              "LrangeKey9",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey9", "start", "7"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{
			name:             "11. Return error when end index is not an integer",
			preset:           false,
			key:              "LrangeKey10",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey10", "0", "end"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{
			name:             "12. Error when start and end indices are equal",
			preset:           true,
			key:              "LrangeKey11",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "LrangeKey11", "1", "1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices cannot be equal"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LRANGE, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			responseArray := rv.Array()
			if len(responseArray) != len(test.expectedResponse) {
				t.Errorf("expected response of length \"%d\", got \"%d\"", len(test.expectedResponse), len(responseArray))
			}
			for i := 0; i < len(responseArray); i++ {
				if responseArray[i].String() != test.expectedResponse[i] {
					t.Errorf("expected value \"%s\" at index %d, got \"%s\"", test.expectedResponse[i], i, responseArray[i].String())
				}
			}
		})
	}
}

func Test_HandleLSET(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			name:             "1. Return last element within range",
			preset:           true,
			key:              "LsetKey1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LSET", "LsetKey1", "3", "new-value"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2", "value3", "new-value"},
			expectedError:    nil,
		},
		{
			name:             "2. Return first element within range",
			preset:           true,
			key:              "LsetKey2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LSET", "LsetKey2", "0", "new-value"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"new-value", "value2", "value3", "value4"},
			expectedError:    nil,
		},
		{
			name:             "3. Return middle element within range",
			preset:           true,
			key:              "LsetKey3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LSET", "LsetKey3", "1", "new-value"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "new-value", "value3", "value4"},
			expectedError:    nil,
		},
		{
			name:             "4. If key does not exist, return error",
			preset:           false,
			key:              "LsetKey4",
			presetValue:      nil,
			command:          []string{"LSET", "LsetKey4", "0", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LSET command on non-list item"),
		},
		{
			name:             "5. Command too short",
			preset:           false,
			key:              "LsetKey5",
			presetValue:      nil,
			command:          []string{"LSET", "LsetKey5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "6. Command too long",
			preset:           false,
			key:              "LsetKey6",
			presetValue:      nil,
			command:          []string{"LSET", "LsetKey6", "0", "element", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "7. Trying to get element by index on a non-list returns error",
			preset:           true,
			key:              "LsetKey5",
			presetValue:      "Default value",
			command:          []string{"LSET", "LsetKey5", "0", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LSET command on non-list item"),
		},
		{
			name:             "8. Trying to get index out of range index beyond last index",
			preset:           true,
			key:              "LsetKey6",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LSET", "LsetKey6", "3", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{
			name:             "9. Trying to get index out of range with negative index",
			preset:           true,
			key:              "LsetKey7",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LSET", "LsetKey7", "-1", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{
			name:             "10. Return error when index is not an integer",
			preset:           false,
			key:              "LsetKey8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LSET", "LsetKey8", "index", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be an integer"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LSET, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
			}
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			l, ok := mockServer.GetValue(ctx, test.key).([]interface{})
			if !ok {
				t.Error("expected value to be list, got another type")
			}
			if len(l) != len(test.expectedValue) {
				t.Errorf("expected list length to be %d, got %d", len(test.expectedValue), len(l))
			}
			for i := 0; i < len(l); i++ {
				if l[i] != test.expectedValue[i] {
					t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedValue[i], l[i])
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		})
	}
}

func Test_HandleLTRIM(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			// Return trim within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			name:             "1. Return trim within range.",
			preset:           true,
			key:              "LtrimKey1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LTRIM", "LtrimKey1", "3", "6"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value4", "value5", "value6"},
			expectedError:    nil,
		},
		{
			name:             "2. Return element from start index to end index when end index is greater than length of the list",
			preset:           true,
			key:              "LtrimKey2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LTRIM", "LtrimKey2", "5", "-1"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value6", "value7", "value8"},
			expectedError:    nil,
		},
		{
			name:             "3. Return error when end index is smaller than start index but greater than -1",
			preset:           true,
			key:              "LtrimKey3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LTRIM", "LtrimKey3", "3", "1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("end index must be greater than start index or -1"),
		},
		{
			name:             "4. If key does not exist, return error",
			preset:           false,
			key:              "LtrimKey4",
			presetValue:      nil,
			command:          []string{"LTRIM", "LtrimKey4", "0", "2"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LTRIM command on non-list item"),
		},
		{
			name:             "5. Command too short",
			preset:           false,
			key:              "LtrimKey5",
			presetValue:      nil,
			command:          []string{"LTRIM", "LtrimKey5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "6. Command too long",
			preset:           false,
			key:              "LtrimKey6",
			presetValue:      nil,
			command:          []string{"LTRIM", "LtrimKey6", "0", "element", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "7. Trying to get element by index on a non-list returns error",
			preset:           true,
			key:              "LtrimKey5",
			presetValue:      "Default value",
			command:          []string{"LTRIM", "LtrimKey5", "0", "3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LTRIM command on non-list item"),
		},
		{
			name:             "8. Error when start index is less than 0",
			preset:           true,
			key:              "LtrimKey7",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LTRIM", "LtrimKey7", "-1", "3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{
			name:             "9. Error when start index is higher than the length of the list",
			preset:           true,
			key:              "LtrimKey8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LTRIM", "LtrimKey8", "10", "11"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{
			name:             "10. Return error when start index is not an integer",
			preset:           false,
			key:              "LtrimKey9",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LTRIM", "LtrimKey9", "start", "7"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{
			name:             "11. Return error when end index is not an integer",
			preset:           false,
			key:              "LtrimKey10",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LTRIM", "LtrimKey10", "0", "end"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LTRIM, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
			}
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			l, ok := mockServer.GetValue(ctx, test.key).([]interface{})
			if !ok {
				t.Error("expected value to be list, got another type")
			}
			if len(l) != len(test.expectedValue) {
				t.Errorf("expected list length to be %d, got %d", len(test.expectedValue), len(l))
			}
			for i := 0; i < len(l); i++ {
				if l[i] != test.expectedValue[i] {
					t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedValue[i], l[i])
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		})
	}
}

func Test_HandleLREM(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			name:             "1. Remove the first 3 elements that appear in the list",
			preset:           true,
			key:              "LremKey1",
			presetValue:      []interface{}{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			command:          []string{"LREM", "LremKey1", "3", "4"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"1", "2", "5", "6", "7", "8", "4", "9", "10", "5", "4"},
			expectedError:    nil,
		},
		{
			name:             "2. Remove the last 3 elements that appear in the list",
			preset:           true,
			key:              "LremKey1",
			presetValue:      []interface{}{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			command:          []string{"LREM", "LremKey1", "-3", "4"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"1", "2", "4", "4", "5", "6", "7", "8", "9", "10", "5"},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "LremKey5",
			presetValue:      nil,
			command:          []string{"LREM", "LremKey5"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			preset:           false,
			key:              "LremKey6",
			presetValue:      nil,
			command:          []string{"LREM", "LremKey6", "0", "element", "element"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Throw error when count is not an integer",
			preset:           false,
			key:              "LremKey7",
			presetValue:      nil,
			command:          []string{"LREM", "LremKey7", "count", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("count must be an integer"),
		},
		{
			name:             "6. Throw error on non-list item",
			preset:           true,
			key:              "LremKey8",
			presetValue:      "Default value",
			command:          []string{"LREM", "LremKey8", "0", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LREM command on non-list item"),
		},
		{
			name:             "7. Throw error on non-existent item",
			preset:           false,
			key:              "LremKey9",
			presetValue:      "Default value",
			command:          []string{"LREM", "LremKey9", "0", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LREM command on non-list item"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LREM, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
			}
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			l, ok := mockServer.GetValue(ctx, test.key).([]interface{})
			if !ok {
				t.Error("expected value to be list, got another type")
			}
			if len(l) != len(test.expectedValue) {
				t.Errorf("expected list length to be %d, got %d", len(test.expectedValue), len(l))
			}
			for i := 0; i < len(l); i++ {
				if l[i] != test.expectedValue[i] {
					t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedValue[i], l[i])
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		})
	}
}

func Test_HandleLMOVE(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		presetValue      map[string]interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:   "1. Move element from LEFT of left list to LEFT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source1":      []interface{}{"one", "two", "three"},
				"destination1": []interface{}{"one", "two", "three"},
			},
			command:          []string{"LMOVE", "source1", "destination1", "LEFT", "LEFT"},
			expectedResponse: "OK",
			expectedValue: map[string]interface{}{
				"source1":      []interface{}{"two", "three"},
				"destination1": []interface{}{"one", "one", "two", "three"},
			},
			expectedError: nil,
		},
		{
			name:   "2. Move element from LEFT of left list to RIGHT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source2":      []interface{}{"one", "two", "three"},
				"destination2": []interface{}{"one", "two", "three"},
			},
			command:          []string{"LMOVE", "source2", "destination2", "LEFT", "RIGHT"},
			expectedResponse: "OK",
			expectedValue: map[string]interface{}{
				"source2":      []interface{}{"two", "three"},
				"destination2": []interface{}{"one", "two", "three", "one"},
			},
			expectedError: nil,
		},
		{
			name:   "3. Move element from RIGHT of left list to LEFT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source3":      []interface{}{"one", "two", "three"},
				"destination3": []interface{}{"one", "two", "three"},
			},
			command:          []string{"LMOVE", "source3", "destination3", "RIGHT", "LEFT"},
			expectedResponse: "OK",
			expectedValue: map[string]interface{}{
				"source3":      []interface{}{"one", "two"},
				"destination3": []interface{}{"three", "one", "two", "three"},
			},
			expectedError: nil,
		},
		{
			name:   "4. Move element from RIGHT of left list to RIGHT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source4":      []interface{}{"one", "two", "three"},
				"destination4": []interface{}{"one", "two", "three"},
			},
			command:          []string{"LMOVE", "source4", "destination4", "RIGHT", "RIGHT"},
			expectedResponse: "OK",
			expectedValue: map[string]interface{}{
				"source4":      []interface{}{"one", "two"},
				"destination4": []interface{}{"one", "two", "three", "three"},
			},
			expectedError: nil,
		},
		{
			name:   "5. Throw error when the right list is non-existent",
			preset: true,
			presetValue: map[string]interface{}{
				"source5": []interface{}{"one", "two", "three"},
			},
			command:          []string{"LMOVE", "source5", "destination5", "LEFT", "LEFT"},
			expectedResponse: nil,
			expectedValue: map[string]interface{}{
				"source5": []interface{}{"one", "two", "three"},
			},
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name:   "6. Throw error when right list in not a list",
			preset: true,
			presetValue: map[string]interface{}{
				"source6":      []interface{}{"one", "two", "tree"},
				"destination6": "Default value",
			},
			command:          []string{"LMOVE", "source6", "destination6", "LEFT", "LEFT"},
			expectedResponse: nil,
			expectedValue: map[string]interface{}{
				"source5":      []interface{}{"one", "two", "three"},
				"destination6": "Default value",
			},
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name:   "7. Throw error when left list is non-existent",
			preset: true,
			presetValue: map[string]interface{}{
				"destination7": []interface{}{"one", "two", "three"},
			},
			command:          []string{"LMOVE", "source7", "destination7", "LEFT", "LEFT"},
			expectedResponse: nil,
			expectedValue: map[string]interface{}{
				"destination7": []interface{}{""},
			},
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name:   "8. Throw error when left list is not a list",
			preset: true,
			presetValue: map[string]interface{}{
				"source8":      "Default value",
				"destination8": []interface{}{"one", "two", "three"},
			},
			command:          []string{"LMOVE", "source8", "destination8", "LEFT", "LEFT"},
			expectedResponse: nil,
			expectedValue: map[string]interface{}{
				"source5":      "Default value",
				"destination6": []interface{}{"one", "two", "three"},
			},
			expectedError: errors.New("both source and destination must be lists"),
		},
		{
			name:             "9. Throw error when command is too short",
			preset:           false,
			presetValue:      map[string]interface{}{},
			command:          []string{"LMOVE", "source9", "destination9"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "10. Throw error when command is too long",
			preset:           false,
			presetValue:      map[string]interface{}{},
			command:          []string{"LMOVE", "source10", "destination10", "LEFT", "LEFT", "RIGHT"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "11. Throw error when WHEREFROM argument is not LEFT/RIGHT",
			preset:           false,
			presetValue:      map[string]interface{}{},
			command:          []string{"LMOVE", "source11", "destination11", "UP", "RIGHT"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("wherefrom and whereto arguments must be either LEFT or RIGHT"),
		},
		{
			name:             "12. Throw error when WHERETO argument is not LEFT/RIGHT",
			preset:           false,
			presetValue:      map[string]interface{}{},
			command:          []string{"LMOVE", "source11", "destination11", "LEFT", "DOWN"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("wherefrom and whereto arguments must be either LEFT or RIGHT"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LMOVE, %d", i))

			if test.preset {
				for key, value := range test.presetValue {
					if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, key, value); err != nil {
						t.Error(err)
					}
					mockServer.KeyUnlock(ctx, key)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
			}
			for key, value := range test.expectedValue {
				if _, err = mockServer.KeyRLock(ctx, key); err != nil {
					t.Error(err)
				}
				l, ok := mockServer.GetValue(ctx, key).([]interface{})
				if !ok {
					t.Error("expected value to be list, got another type")
				}
				expectedList, ok := value.([]interface{})
				if !ok {
					t.Error("expected test value to be list, got another type")
				}
				if len(l) != len(expectedList) {
					t.Errorf("expected list length to be %d, got %d", len(expectedList), len(l))
				}
				for i := 0; i < len(l); i++ {
					if l[i] != expectedList[i] {
						t.Errorf("expected element at index %d to be %+v, got %+v", i, expectedList[i], l[i])
					}
				}
				mockServer.KeyRUnlock(ctx, key)
			}
		})
	}
}

func Test_HandleLPUSH(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			name:             "1. LPUSHX to existing list prepends the element to the list",
			preset:           true,
			key:              "LpushKey1",
			presetValue:      []interface{}{"1", "2", "4", "5"},
			command:          []string{"LPUSHX", "LpushKey1", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2", "1", "2", "4", "5"},
			expectedError:    nil,
		},
		{
			name:             "2. LPUSH on existing list prepends the elements to the list",
			preset:           true,
			key:              "LpushKey2",
			presetValue:      []interface{}{"1", "2", "4", "5"},
			command:          []string{"LPUSH", "LpushKey2", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2", "1", "2", "4", "5"},
			expectedError:    nil,
		},
		{
			name:             "3. LPUSH on non-existent list creates the list",
			preset:           false,
			key:              "LpushKey3",
			presetValue:      nil,
			command:          []string{"LPUSH", "LpushKey3", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "4. Command too short",
			preset:           false,
			key:              "LpushKey5",
			presetValue:      nil,
			command:          []string{"LPUSH", "LpushKey5"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. LPUSHX command returns error on non-existent list",
			preset:           false,
			key:              "LpushKey6",
			presetValue:      nil,
			command:          []string{"LPUSHX", "LpushKey7", "count", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LPUSHX command on non-list item"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LPUSH/LPUSHX, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
			}
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			l, ok := mockServer.GetValue(ctx, test.key).([]interface{})
			if !ok {
				t.Error("expected value to be list, got another type")
			}
			if len(l) != len(test.expectedValue) {
				t.Errorf("expected list length to be %d, got %d", len(test.expectedValue), len(l))
			}
			for i := 0; i < len(l); i++ {
				if l[i] != test.expectedValue[i] {
					t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedValue[i], l[i])
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		})
	}
}

func Test_HandleRPUSH(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			name:             "1. RPUSHX to existing list prepends the element to the list",
			preset:           true,
			key:              "RpushKey1",
			presetValue:      []interface{}{"1", "2", "4", "5"},
			command:          []string{"RPUSHX", "RpushKey1", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"1", "2", "4", "5", "value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "2. RPUSH on existing list prepends the elements to the list",
			preset:           true,
			key:              "RpushKey2",
			presetValue:      []interface{}{"1", "2", "4", "5"},
			command:          []string{"RPUSH", "RpushKey2", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"1", "2", "4", "5", "value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "3. RPUSH on non-existent list creates the list",
			preset:           false,
			key:              "RpushKey3",
			presetValue:      nil,
			command:          []string{"RPUSH", "RpushKey3", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2"},
			expectedError:    nil,
		},
		{
			name:             "4. Command too short",
			preset:           false,
			key:              "RpushKey5",
			presetValue:      nil,
			command:          []string{"RPUSH", "RpushKey5"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. RPUSHX command returns error on non-existent list",
			preset:           false,
			key:              "RpushKey6",
			presetValue:      nil,
			command:          []string{"RPUSHX", "RpushKey7", "count", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("RPUSHX command on non-list item"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("RPUSH/RPUSHX, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
			}
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			l, ok := mockServer.GetValue(ctx, test.key).([]interface{})
			if !ok {
				t.Error("expected value to be list, got another type")
			}
			if len(l) != len(test.expectedValue) {
				t.Errorf("expected list length to be %d, got %d", len(test.expectedValue), len(l))
			}
			for i := 0; i < len(l); i++ {
				if l[i] != test.expectedValue[i] {
					t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedValue[i], l[i])
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		})
	}
}

func Test_HandlePOP(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			name:             "1. LPOP returns last element and removed first element from the list",
			preset:           true,
			key:              "PopKey1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LPOP", "PopKey1"},
			expectedResponse: "value1",
			expectedValue:    []interface{}{"value2", "value3", "value4"},
			expectedError:    nil,
		},
		{
			name:             "2. RPOP returns last element and removed last element from the list",
			preset:           true,
			key:              "PopKey2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"RPOP", "PopKey2"},
			expectedResponse: "value4",
			expectedValue:    []interface{}{"value1", "value2", "value3"},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "PopKey3",
			presetValue:      nil,
			command:          []string{"LPOP"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4.  Command too long",
			preset:           false,
			key:              "PopKey4",
			presetValue:      nil,
			command:          []string{"LPOP", "PopKey4", "PopKey4"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to execute LPOP from a non-list item return an error",
			preset:           true,
			key:              "PopKey5",
			presetValue:      "Default value",
			command:          []string{"LPOP", "PopKey5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LPOP command on non-list item"),
		},
		{
			name:             "6. Trying to execute RPOP from a non-list item return an error",
			preset:           true,
			key:              "PopKey6",
			presetValue:      "Default value",
			command:          []string{"RPOP", "PopKey6"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("RPOP command on non-list item"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("LPOP/RPOP, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			rd := resp.NewReader(bytes.NewBuffer(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.String() != test.expectedResponse {
				t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
			}
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			l, ok := mockServer.GetValue(ctx, test.key).([]interface{})
			if !ok {
				t.Error("expected value to be list, got another type")
			}
			if len(l) != len(test.expectedValue) {
				t.Errorf("expected list length to be %d, got %d", len(test.expectedValue), len(l))
			}
			for i := 0; i < len(l); i++ {
				if l[i] != test.expectedValue[i] {
					t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedValue[i], l[i])
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		})
	}
}
