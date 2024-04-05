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

package hash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/echovault"
	"github.com/tidwall/resp"
	"slices"
	"testing"
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

func Test_HandleHSET(t *testing.T) {
	// Tests for both HSET and HSETNX
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse int // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. HSETNX set field on non-existent hash map",
			preset:           false,
			key:              "HsetKey1",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSETNX", "HsetKey1", "field1", "value1"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{"field1": "value1"},
			expectedError:    nil,
		},
		{
			name:             "2. HSETNX set field on existing hash map",
			preset:           true,
			key:              "HsetKey2",
			presetValue:      map[string]interface{}{"field1": "value1"},
			command:          []string{"HSETNX", "HsetKey2", "field2", "value2"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{"field1": "value1", "field2": "value2"},
			expectedError:    nil,
		},
		{
			name:             "3. HSETNX skips operation when setting on existing field",
			preset:           true,
			key:              "HsetKey3",
			presetValue:      map[string]interface{}{"field1": "value1"},
			command:          []string{"HSETNX", "HsetKey3", "field1", "value1-new"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{"field1": "value1"},
			expectedError:    nil,
		},
		{
			name:             "4. Regular HSET command on non-existent hash map",
			preset:           false,
			key:              "HsetKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSET", "HsetKey4", "field1", "value1", "field2", "value2"},
			expectedResponse: 2,
			expectedValue:    map[string]interface{}{"field1": "value1", "field2": "value2"},
			expectedError:    nil,
		},
		{
			name:             "5. Regular HSET update on existing hash map",
			preset:           true,
			key:              "HsetKey5",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": "value2"},
			command:          []string{"HSET", "HsetKey5", "field1", "value1-new", "field2", "value2-ne2", "field3", "value3"},
			expectedResponse: 3,
			expectedValue:    map[string]interface{}{"field1": "value1-new", "field2": "value2-ne2", "field3": "value3"},
			expectedError:    nil,
		},
		{
			name:             "6. HSET returns error when the target key is not a map",
			preset:           true,
			key:              "HsetKey6",
			presetValue:      "Default preset value",
			command:          []string{"HSET", "HsetKey6", "field1", "value1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HsetKey6 is not a hash"),
		},
		{
			name:             "7. HSET returns error when there's a mismatch in key/values",
			preset:           false,
			key:              "HsetKey7",
			presetValue:      nil,
			command:          []string{"HSET", "HsetKey7", "field1", "value1", "field2"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("each field must have a corresponding value"),
		},
		{
			name:             "8. Command too short",
			preset:           true,
			key:              "HsetKey8",
			presetValue:      nil,
			command:          []string{"HSET", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HSET/HSETNX, %d", i))
			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHSET(ctx, test.command, mockServer, nil)
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
				t.Errorf("expected response \"%d\", got \"%d\"", test.expectedResponse, rv.Integer())
			}
			// Check that all the values are what is expected
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			hash, ok := mockServer.GetValue(ctx, test.key).(map[string]interface{})
			if !ok {
				t.Errorf("value at key \"%s\" is not a hash map", test.key)
			}
			for field, value := range hash {
				if value != test.expectedValue[field] {
					t.Errorf("expected value \"%+v\" for field \"%+v\", got \"%+v\"", test.expectedValue[field], field, value)
				}
			}
		})
	}
}

func Test_HandleHINCRBY(t *testing.T) {
	// Tests for both HINCRBY and HINCRBYFLOAT
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{} // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Increment by integer on non-existent hash should create a new one",
			preset:           false,
			key:              "HincrbyKey1",
			presetValue:      nil,
			command:          []string{"HINCRBY", "HincrbyKey1", "field1", "1"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{"field1": 1},
			expectedError:    nil,
		},
		{
			name:             "2. Increment by float on non-existent hash should create one",
			preset:           false,
			key:              "HincrbyKey2",
			presetValue:      nil,
			command:          []string{"HINCRBYFLOAT", "HincrbyKey2", "field1", "3.142"},
			expectedResponse: 3.142,
			expectedValue:    map[string]interface{}{"field1": 3.142},
			expectedError:    nil,
		},
		{
			name:             "3. Increment by integer on existing hash",
			preset:           true,
			key:              "HincrbyKey3",
			presetValue:      map[string]interface{}{"field1": 1},
			command:          []string{"HINCRBY", "HincrbyKey3", "field1", "10"},
			expectedResponse: 11,
			expectedValue:    map[string]interface{}{"field1": 11},
			expectedError:    nil,
		},
		{
			name:             "4. Increment by float on an existing hash",
			preset:           true,
			key:              "HincrbyKey4",
			presetValue:      map[string]interface{}{"field1": 3.142},
			command:          []string{"HINCRBYFLOAT", "HincrbyKey4", "field1", "3.142"},
			expectedResponse: 6.284,
			expectedValue:    map[string]interface{}{"field1": 6.284},
			expectedError:    nil,
		},
		{
			name:             "5. Command too short",
			preset:           false,
			key:              "HincrbyKey5",
			presetValue:      nil,
			command:          []string{"HINCRBY", "HincrbyKey5"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "6. Command too long",
			preset:           false,
			key:              "HincrbyKey6",
			presetValue:      nil,
			command:          []string{"HINCRBY", "HincrbyKey6", "field1", "23", "45"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "7. Error when increment by float does not pass valid float",
			preset:           false,
			key:              "HincrbyKey7",
			presetValue:      nil,
			command:          []string{"HINCRBYFLOAT", "HincrbyKey7", "field1", "three point one four two"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("increment must be a float"),
		},
		{
			name:             "8. Error when increment does not pass valid integer",
			preset:           false,
			key:              "HincrbyKey8",
			presetValue:      nil,
			command:          []string{"HINCRBY", "HincrbyKey8", "field1", "three"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("increment must be an integer"),
		},
		{
			name:             "9. Error when trying to increment on a key that is not a hash",
			preset:           true,
			key:              "HincrbyKey9",
			presetValue:      "Default value",
			command:          []string{"HINCRBY", "HincrbyKey9", "field1", "3"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HincrbyKey9 is not a hash"),
		},
		{
			name:             "10. Error when trying to increment a hash field that is not a number",
			preset:           true,
			key:              "HincrbyKey10",
			presetValue:      map[string]interface{}{"field1": "value1"},
			command:          []string{"HINCRBY", "HincrbyKey10", "field1", "3"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at field field1 is not a number"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HINCRBY, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHINCRBY(ctx, test.command, mockServer, nil)
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
			switch test.expectedResponse.(type) {
			default:
				t.Error("expectedResponse must be an integer or string")
			case int:
				if rv.Integer() != test.expectedResponse {
					t.Errorf("expected response \"%+v\", got \"%d\"", test.expectedResponse, rv.Integer())
				}
			case float64:
				if rv.Float() != test.expectedResponse {
					t.Errorf("expected response \"%+v\", got \"%+v\"", test.expectedResponse, rv.Float())
				}
			}
			// Check that all the values are what is expected
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			hash, ok := mockServer.GetValue(ctx, test.key).(map[string]interface{})
			if !ok {
				t.Errorf("value at key \"%s\" is not a hash map", test.key)
			}
			for field, value := range hash {
				if value != test.expectedValue[field] {
					t.Errorf("expected value \"%+v\" for field \"%+v\", got \"%+v\"", test.expectedValue[field], field, value)
				}
			}
		})
	}
}

func Test_HandleHGET(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{} // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Return nil when attempting to get from non-existed key",
			preset:           true,
			key:              "HgetKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 365, "field3": 3.142},
			command:          []string{"HGET", "HgetKey1", "field1", "field2", "field3", "field4"},
			expectedResponse: []interface{}{"value1", 365, "3.142", nil},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "2. Return nil when attempting to get from non-existed key",
			preset:           false,
			key:              "HgetKey2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGET", "HgetKey2", "field1"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "3. Error when trying to get from a value that is not a hash map",
			preset:           true,
			key:              "HgetKey3",
			presetValue:      "Default Value",
			command:          []string{"HGET", "HgetKey3", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HgetKey3 is not a hash"),
		},
		{
			name:             "4. Command too short",
			preset:           false,
			key:              "HgetKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGET", "HgetKey4"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HINCRBY, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHGET(ctx, test.command, mockServer, nil)
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
			if test.expectedResponse == nil {
				if !rv.IsNull() {
					t.Errorf("expected nil response, got %+v", rv)
				}
				return
			}
			if expectedArr, ok := test.expectedResponse.([]interface{}); ok {
				for i, v := range rv.Array() {
					switch v.Type().String() {
					default:
						t.Error("unexpected type encountered")
					case "Integer":
						if v.Integer() != expectedArr[i] {
							t.Errorf("expected \"%+v\", got \"%d\"", expectedArr[i], v.Integer())
						}
					case "BulkString":
						if len(v.String()) == 0 && expectedArr[i] == nil {
							continue
						}
						if v.String() != expectedArr[i] {
							t.Errorf("expected \"%+v\", got \"%s\"", expectedArr[i], v.String())
						}
					}
				}
			}
		})
	}
}

func Test_HandleHSTRLEN(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{} // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			// Return lengths of field values.
			// If the key does not exist, its length should be 0.
			name:             "1. Return lengths of field values.",
			preset:           true,
			key:              "HstrlenKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HSTRLEN", "HstrlenKey1", "field1", "field2", "field3", "field4"},
			expectedResponse: []int{len("value1"), len("123456789"), len("3.142"), 0},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "2. Nil response when trying to get HSTRLEN non-existent key",
			preset:           false,
			key:              "HstrlenKey2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSTRLEN", "HstrlenKey2", "field1"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "HstrlenKey3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSTRLEN", "HstrlenKey3"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Trying to get lengths on a non hash map returns error",
			preset:           true,
			key:              "HstrlenKey4",
			presetValue:      "Default value",
			command:          []string{"HSTRLEN", "HstrlenKey4", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HstrlenKey4 is not a hash"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HSTRLEN, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHSTRLEN(ctx, test.command, mockServer, nil)
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
			if test.expectedResponse == nil {
				if !rv.IsNull() {
					t.Errorf("expected nil response, got %+v", rv)
				}
				return
			}
			expectedResponse, _ := test.expectedResponse.([]int)
			for i, v := range rv.Array() {
				if v.Integer() != expectedResponse[i] {
					t.Errorf("expected \"%d\", got \"%d\"", expectedResponse[i], v.Integer())
				}
			}
		})
	}
}

func Test_HandleHVALS(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse []interface{}
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Return all the values from a hash",
			preset:           true,
			key:              "HvalsKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HVALS", "HvalsKey1"},
			expectedResponse: []interface{}{"value1", 123456789, "3.142"},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "2. Empty array response when trying to get HSTRLEN non-existent key",
			preset:           false,
			key:              "HvalsKey2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HVALS", "HvalsKey2"},
			expectedResponse: []interface{}{},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "HvalsKey3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HVALS"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			preset:           false,
			key:              "HvalsKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HVALS", "HvalsKey4", "HvalsKey4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non hash map returns error",
			preset:           true,
			key:              "HvalsKey5",
			presetValue:      "Default value",
			command:          []string{"HVALS", "HvalsKey5"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HvalsKey5 is not a hash"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HVALS, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHVALS(ctx, test.command, mockServer, nil)
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
			switch len(test.expectedResponse) {
			case 0:
				if len(rv.Array()) != 0 {
					t.Errorf("expected empty array, got length \"%d\"", len(rv.Array()))
				}
			default:
				for _, v := range rv.Array() {
					switch v.Type().String() {
					default:
						t.Errorf("unexpected error type")
					case "Integer":
						// Value is an integer, check if it is contained in the expected response
						if !slices.ContainsFunc(test.expectedResponse, func(e interface{}) bool {
							expectedValue, ok := e.(int)
							return ok && expectedValue == v.Integer()
						}) {
							t.Errorf("couldn't find response value \"%d\" in expected values", v.Integer())
						}
					case "BulkString":
						// Value is a string, check if it is contained in the expected response
						if !slices.ContainsFunc(test.expectedResponse, func(e interface{}) bool {
							expectedValue, ok := e.(string)
							return ok && expectedValue == v.String()
						}) {
							t.Errorf("couldn't find response value \"%s\" in expected values", v.String())
						}
					}
				}
			}
		})
	}
}

func Test_HandleHRANDFIELD(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		withValues       bool
		expectedCount    int
		expectedResponse []string
		expectedError    error
	}{
		{
			name:             "1. Get a random field",
			preset:           true,
			key:              "HrandfieldKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HRANDFIELD", "HrandfieldKey1"},
			withValues:       false,
			expectedCount:    1,
			expectedResponse: []string{"field1", "field2", "field3"},
			expectedError:    nil,
		},
		{
			name:             "2. Get a random field with a value",
			preset:           true,
			key:              "HrandfieldKey2",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HRANDFIELD", "HrandfieldKey2", "1", "WITHVALUES"},
			withValues:       true,
			expectedCount:    2,
			expectedResponse: []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
			expectedError:    nil,
		},
		{
			name:   "3.  Get several random fields",
			preset: true,
			key:    "HrandfieldKey3",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:          []string{"HRANDFIELD", "HrandfieldKey3", "3"},
			withValues:       false,
			expectedCount:    3,
			expectedResponse: []string{"field1", "field2", "field3", "field4", "field5"},
			expectedError:    nil,
		},
		{
			name:   "4. Get several random fields with their corresponding values",
			preset: true,
			key:    "HrandfieldKey4",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:       []string{"HRANDFIELD", "HrandfieldKey4", "3", "WITHVALUES"},
			withValues:    true,
			expectedCount: 6,
			expectedResponse: []string{
				"field1", "value1", "field2", "123456789", "field3",
				"3.142", "field4", "value4", "field5", "value5",
			},
			expectedError: nil,
		},
		{
			name:   "5. Get the entire hash",
			preset: true,
			key:    "HrandfieldKey5",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:          []string{"HRANDFIELD", "HrandfieldKey5", "5"},
			withValues:       false,
			expectedCount:    5,
			expectedResponse: []string{"field1", "field2", "field3", "field4", "field5"},
			expectedError:    nil,
		},
		{
			name:   "6. Get the entire hash with values",
			preset: true,
			key:    "HrandfieldKey5",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:       []string{"HRANDFIELD", "HrandfieldKey5", "5", "WITHVALUES"},
			withValues:    true,
			expectedCount: 10,
			expectedResponse: []string{
				"field1", "value1", "field2", "123456789", "field3",
				"3.142", "field4", "value4", "field5", "value5",
			},
			expectedError: nil,
		},
		{
			name:          "7. Command too short",
			preset:        false,
			key:           "HrandfieldKey10",
			presetValue:   map[string]interface{}{},
			command:       []string{"HRANDFIELD"},
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "8. Command too long",
			preset:        false,
			key:           "HrandfieldKey11",
			presetValue:   map[string]interface{}{},
			command:       []string{"HRANDFIELD", "HrandfieldKey11", "HrandfieldKey11", "HrandfieldKey11", "HrandfieldKey11"},
			expectedError: errors.New(constants.WrongArgsResponse),
		},
		{
			name:          "9. Trying to get random field on a non hash map returns error",
			preset:        true,
			key:           "HrandfieldKey12",
			presetValue:   "Default value",
			command:       []string{"HRANDFIELD", "HrandfieldKey12"},
			expectedError: errors.New("value at HrandfieldKey12 is not a hash"),
		},
		{
			name:          "10. Throw error when count provided is not an integer",
			preset:        true,
			key:           "HrandfieldKey12",
			presetValue:   "Default value",
			command:       []string{"HRANDFIELD", "HrandfieldKey12", "COUNT"},
			expectedError: errors.New("count must be an integer"),
		},
		{
			name:          "11. If fourth argument is provided, it must be \"WITHVALUES\"",
			preset:        true,
			key:           "HrandfieldKey12",
			presetValue:   "Default value",
			command:       []string{"HRANDFIELD", "HrandfieldKey12", "10", "FLAG"},
			expectedError: errors.New("result modifier must be withvalues"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HRANDFIELD, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHRANDFIELD(ctx, test.command, mockServer, nil)
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
			if len(rv.Array()) != test.expectedCount {
				t.Errorf("expected response array of length \"%d\", got length \"%d\"", test.expectedCount, len(rv.Array()))
			}
			switch test.withValues {
			case false:
				for _, v := range rv.Array() {
					if !slices.ContainsFunc(test.expectedResponse, func(expected string) bool {
						return expected == v.String()
					}) {
						t.Errorf("could not find response element \"%s\" in expected response", v.String())
					}
				}
			case true:
				responseArray := rv.Array()
				for i := 0; i < len(responseArray); i++ {
					if i%2 == 0 {
						field := responseArray[i].String()
						value := responseArray[i+1].String()

						expectedFieldIndex := slices.Index(test.expectedResponse, field)
						if expectedFieldIndex == -1 {
							t.Errorf("could not find response value \"%s\" in expected values", field)
						}
						expectedValue := test.expectedResponse[expectedFieldIndex+1]

						if value != expectedValue {
							t.Errorf("expected value \"%s\", got \"%s\"", expectedValue, value)
						}
					}
				}
			}
		})
	}
}

func Test_HandleHLEN(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{} // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Return the correct length of the hash",
			preset:           true,
			key:              "HlenKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HLEN", "HlenKey1"},
			expectedResponse: 3,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "2. 0 response when trying to call HLEN on non-existent key",
			preset:           false,
			key:              "HlenKey2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HLEN", "HlenKey2"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "HlenKey3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HLEN"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			preset:           false,
			key:              "HlenKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HLEN", "HlenKey4", "HlenKey4"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non hash map returns error",
			preset:           true,
			key:              "HlenKey5",
			presetValue:      "Default value",
			command:          []string{"HLEN", "HlenKey5"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HlenKey5 is not a hash"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HLEN, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHLEN(ctx, test.command, mockServer, nil)
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
			if expectedResponse, ok := test.expectedResponse.(int); ok {
				if rv.Integer() != expectedResponse {
					t.Errorf("expected ineger \"%d\", got \"%d\"", expectedResponse, rv.Integer())
				}
				return
			}
			t.Error("expected integer response, got another type")
		})
	}
}

func Test_HandleHKeys(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{} // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Return an array containing all the keys of the hash",
			preset:           true,
			key:              "HkeysKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HKEYS", "HkeysKey1"},
			expectedResponse: []string{"field1", "field2", "field3"},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "2. Empty array response when trying to call HKEYS on non-existent key",
			preset:           false,
			key:              "HkeysKey2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HKEYS", "HkeysKey2"},
			expectedResponse: []string{},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "HkeysKey3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HKEYS"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			preset:           false,
			key:              "HkeysKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HKEYS", "HkeysKey4", "HkeysKey4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non hash map returns error",
			preset:           true,
			key:              "HkeysKey5",
			presetValue:      "Default value",
			command:          []string{"HKEYS", "HkeysKey5"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HkeysKey5 is not a hash"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HKEYS, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHKEYS(ctx, test.command, mockServer, nil)
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
			if expectedResponse, ok := test.expectedResponse.([]string); ok {
				if len(rv.Array()) != len(expectedResponse) {
					t.Errorf("expected length \"%d\", got \"%d\"", len(expectedResponse), len(rv.Array()))
				}
				for _, field := range expectedResponse {
					if !slices.ContainsFunc(rv.Array(), func(value resp.Value) bool {
						return value.String() == field
					}) {
						t.Errorf("could not find expected to find key \"%s\" in response", field)
					}
				}
				return
			}
			t.Error("expected array response, got another type")
		})
	}
}

func Test_HandleHGETALL(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse []string
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Return an array containing all the fields and values of the hash",
			preset:           true,
			key:              "HGetAllKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HGETALL", "HGetAllKey1"},
			expectedResponse: []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "2. Empty array response when trying to call HGETALL on non-existent key",
			preset:           false,
			key:              "HGetAllKey2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGETALL", "HGetAllKey2"},
			expectedResponse: []string{},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "HGetAllKey3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGETALL"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			preset:           false,
			key:              "HGetAllKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGETALL", "HGetAllKey4", "HGetAllKey4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non hash map returns error",
			preset:           true,
			key:              "HGetAllKey5",
			presetValue:      "Default value",
			command:          []string{"HGETALL", "HGetAllKey5"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HGetAllKey5 is not a hash"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HGETALL, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHGETALL(ctx, test.command, mockServer, nil)
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
			if len(rv.Array()) != len(test.expectedResponse) {
				t.Errorf("expected length \"%d\", got \"%d\"", len(test.expectedResponse), len(rv.Array()))
			}
			// In the response:
			// The order of results is not guaranteed,
			// However, each field in the array will be reliably followed by its corresponding value
			responseArray := rv.Array()
			for i := 0; i < len(responseArray); i++ {
				if i%2 == 0 {
					// We're on a field in the response
					field := responseArray[i].String()
					value := responseArray[i+1].String()

					expectedFieldIndex := slices.Index(test.expectedResponse, field)
					if expectedFieldIndex == -1 {
						t.Errorf("received unexpected field \"%s\" in response", field)
					}
					expectedValue := test.expectedResponse[expectedFieldIndex+1]
					if expectedValue != value {
						t.Errorf("expected entry \"%s\", got \"%s\"", expectedValue, value)
					}
				}

			}
			return
		})
	}
}

func Test_HandleHEXISTS(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Return 1 if the field exists in the hash",
			preset:           true,
			key:              "HexistsKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HEXISTS", "HexistsKey1", "field1"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "2. 0 response when trying to call HEXISTS on non-existent key",
			preset:           false,
			key:              "HexistsKey2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HEXISTS", "HexistsKey2", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "3. Command too short",
			preset:           false,
			key:              "HexistsKey3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HEXISTS", "HexistsKey3"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "4. Command too long",
			preset:           false,
			key:              "HexistsKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HEXISTS", "HexistsKey4", "field1", "field2"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non hash map returns error",
			preset:           true,
			key:              "HexistsKey5",
			presetValue:      "Default value",
			command:          []string{"HEXISTS", "HexistsKey5", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HexistsKey5 is not a hash"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HEXISTS, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHEXISTS(ctx, test.command, mockServer, nil)
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
			if expectedResponse, ok := test.expectedResponse.(int); ok {
				if rv.Integer() != expectedResponse {
					t.Errorf("expected \"%d\", got \"%d\"", expectedResponse, rv.Integer())
				}
				return
			}
			t.Error("expected integer response, got another type")
		})
	}
}

func Test_HandleHDEL(t *testing.T) {
	tests := []struct {
		name             string
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			name:             "1. Return count of deleted fields in the specified hash",
			preset:           true,
			key:              "HdelKey1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142, "field7": "value7"},
			command:          []string{"HDEL", "HdelKey1", "field1", "field2", "field3", "field4", "field5", "field6"},
			expectedResponse: 3,
			expectedValue:    map[string]interface{}{"field1": nil, "field2": nil, "field3": nil, "field7": "value1"},
			expectedError:    nil,
		},
		{
			name:             "2. 0 response when passing delete fields that are non-existent on valid hash",
			preset:           true,
			key:              "HdelKey2",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": "value2", "field3": "value3"},
			command:          []string{"HDEL", "HdelKey2", "field4", "field5", "field6"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{"field1": "value1", "field2": "value2", "field3": "value3"},
			expectedError:    nil,
		},
		{
			name:             "3. 0 response when trying to call HDEL on non-existent key",
			preset:           false,
			key:              "HdelKey3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HDEL", "HdelKey3", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{
			name:             "4. Command too short",
			preset:           false,
			key:              "HdelKey4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HDEL", "HdelKey4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Trying to get lengths on a non hash map returns error",
			preset:           true,
			key:              "HdelKey5",
			presetValue:      "Default value",
			command:          []string{"HDEL", "HdelKey5", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at HdelKey5 is not a hash"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("HDEL, %d", i))

			if test.preset {
				if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, test.key)
			}
			res, err := handleHDEL(ctx, test.command, mockServer, nil)
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
			if expectedResponse, ok := test.expectedResponse.(int); ok {
				if rv.Integer() != expectedResponse {
					t.Errorf("expected \"%d\", got \"%d\"", expectedResponse, rv.Integer())
				}
				return
			}
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if hash, ok := mockServer.GetValue(ctx, test.key).(map[string]interface{}); ok {
				for field, value := range hash {
					if value != test.expectedValue[field] {
						t.Errorf("expected value \"%+v\", got \"%+v\"", test.expectedValue[field], value)
					}
				}
				return
			}
			t.Error("expected hash value but got another type")
		})
	}
}
