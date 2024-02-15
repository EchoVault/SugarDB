package hash

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_HandleHSET(t *testing.T) {
	// Tests for both HSET and HSETNX
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse int // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{ // HSETNX set field on non-existent hash map
			preset:           false,
			key:              "key1",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSETNX", "key1", "field1", "value1"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{"field1": "value1"},
			expectedError:    nil,
		},
		{ // HSETNX set field on existing hash map
			preset:           true,
			key:              "key2",
			presetValue:      map[string]interface{}{"field1": "value1"},
			command:          []string{"HSETNX", "key2", "field2", "value2"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{"field1": "value1", "field2": "value2"},
			expectedError:    nil,
		},
		{ // HSETNX skips operation when setting on existing field
			preset:           true,
			key:              "key3",
			presetValue:      map[string]interface{}{"field1": "value1"},
			command:          []string{"HSETNX", "key3", "field1", "value1-new"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{"field1": "value1"},
			expectedError:    nil,
		},
		{ // Regular HSET command on non-existent hash map
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSET", "key4", "field1", "value1", "field2", "value2"},
			expectedResponse: 2,
			expectedValue:    map[string]interface{}{"field1": "value1", "field2": "value2"},
			expectedError:    nil,
		},
		{ // Regular HSET update on existing hash map
			preset:           true,
			key:              "key5",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": "value2"},
			command:          []string{"HSET", "key5", "field1", "value1-new", "field2", "value2-ne2", "field3", "value3"},
			expectedResponse: 3,
			expectedValue:    map[string]interface{}{"field1": "value1-new", "field2": "value2-ne2", "field3": "value3"},
			expectedError:    nil,
		},
		{ // HSET returns error when the target key is not a map
			preset:           true,
			key:              "key6",
			presetValue:      "Default preset value",
			command:          []string{"HSET", "key6", "field1", "value1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at key6 is not a hash"),
		},
		{ // HSET returns error when there's a mismatch in key/values
			preset:           false,
			key:              "key7",
			presetValue:      nil,
			command:          []string{"HSET", "key7", "field1", "value1", "field2"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("each field must have a corresponding value"),
		},
		{ // Command too short
			preset:           true,
			key:              "key8",
			presetValue:      nil,
			command:          []string{"HSET", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(context.Background(), test.key); err != nil {
				t.Error(err)
			}
			mockServer.SetValue(context.Background(), test.key, test.presetValue)
			mockServer.KeyUnlock(test.key)
		}
		res, err := handleHSET(context.Background(), test.command, mockServer, nil)
		if test.expectedError != nil {
			if err.Error() != test.expectedError.Error() {
				t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
			}
			continue
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
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		hash, ok := mockServer.GetValue(test.key).(map[string]interface{})
		if !ok {
			t.Errorf("value at key \"%s\" is not a hash map", test.key)
		}
		for field, value := range hash {
			if value != test.expectedValue[field] {
				t.Errorf("expected value \"%+v\" for field \"%+v\", got \"%+v\"", test.expectedValue[field], field, value)
			}
		}
	}
}

func Test_HandleHINCRBY(t *testing.T) {
	// Tests for both HINCRBY and HINCRBYFLOAT
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{} // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{ // Increment by integer on non-existent hash should create a new one
			preset:           false,
			key:              "key1",
			presetValue:      nil,
			command:          []string{"HINCRBY", "key1", "field1", "1"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{"field1": 1},
			expectedError:    nil,
		},
		{ // Increment by float on non-existent hash should create one
			preset:           false,
			key:              "key2",
			presetValue:      nil,
			command:          []string{"HINCRBYFLOAT", "key2", "field1", "3.142"},
			expectedResponse: "3.142",
			expectedValue:    map[string]interface{}{"field1": 3.142},
			expectedError:    nil,
		},
		{ // Increment by integer on existing hash
			preset:           true,
			key:              "key3",
			presetValue:      map[string]interface{}{"field1": 1},
			command:          []string{"HINCRBY", "key3", "field1", "10"},
			expectedResponse: 11,
			expectedValue:    map[string]interface{}{"field1": 11},
			expectedError:    nil,
		},
		{ // Increment by float on an existing hash
			preset:           true,
			key:              "key4",
			presetValue:      map[string]interface{}{"field1": 3.142},
			command:          []string{"HINCRBYFLOAT", "key4", "field1", "3.142"},
			expectedResponse: "6.284",
			expectedValue:    map[string]interface{}{"field1": 6.284},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key5",
			presetValue:      nil,
			command:          []string{"HINCRBY", "key5"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key6",
			presetValue:      nil,
			command:          []string{"HINCRBY", "key6", "field1", "23", "45"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Error when increment by float does not pass valid float
			preset:           false,
			key:              "key7",
			presetValue:      nil,
			command:          []string{"HINCRBYFLOAT", "key7", "field1", "three point one four two"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("increment must be a float"),
		},
		{ // Error when increment does not pass valid integer
			preset:           false,
			key:              "key8",
			presetValue:      nil,
			command:          []string{"HINCRBY", "key8", "field1", "three"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("increment must be an integer"),
		},
		{ // Error when trying to increment on a key that is not a hash
			preset:           true,
			key:              "key9",
			presetValue:      "Default value",
			command:          []string{"HINCRBY", "key9", "field1", "3"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at key9 is not a hash"),
		},
		{ // Error when trying to increment a hash field that is not a number
			preset:           true,
			key:              "key10",
			presetValue:      map[string]interface{}{"field1": "value1"},
			command:          []string{"HINCRBY", "key10", "field1", "3"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at field field1 is not a number"),
		},
	}

	for _, test := range tests {
		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(context.Background(), test.key); err != nil {
				t.Error(err)
			}
			mockServer.SetValue(context.Background(), test.key, test.presetValue)
			mockServer.KeyUnlock(test.key)
		}
		res, err := handleHINCRBY(context.Background(), test.command, mockServer, nil)
		if test.expectedError != nil {
			if err.Error() != test.expectedError.Error() {
				t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
			}
			continue
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
		case string:
			if rv.String() != test.expectedResponse {
				t.Errorf("expected response \"%+v\", got \"%s\"", test.expectedResponse, rv.String())
			}
		}
		// Check that all the values are what is expected
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		hash, ok := mockServer.GetValue(test.key).(map[string]interface{})
		if !ok {
			t.Errorf("value at key \"%s\" is not a hash map", test.key)
		}
		for field, value := range hash {
			if value != test.expectedValue[field] {
				t.Errorf("expected value \"%+v\" for field \"%+v\", got \"%+v\"", test.expectedValue[field], field, value)
			}
		}
	}
}

func Test_HandleHGET(t *testing.T) {
	// Tests for both HINCRBY and HINCRBYFLOAT
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{} // Change count
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{ // Return nil when attempting to get from non-existed key
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 365, "field3": 3.142},
			command:          []string{"HGET", "key1", "field1", "field2", "field3", "field4"},
			expectedResponse: []interface{}{"value1", 365, "3.142", nil},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Return nil when attempting to get from non-existed key
			preset:           false,
			key:              "key2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGET", "key2", "field1"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Error when trying to get from a value that is not a hash map
			preset:           true,
			key:              "key3",
			presetValue:      "Default Value",
			command:          []string{"HGET", "key3", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at key3 is not a hash"),
		},
		{ // Command too short
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGET", "key4"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(context.Background(), test.key); err != nil {
				t.Error(err)
			}
			mockServer.SetValue(context.Background(), test.key, test.presetValue)
			mockServer.KeyUnlock(test.key)
		}
		res, err := handleHGET(context.Background(), test.command, mockServer, nil)
		if test.expectedError != nil {
			if err.Error() != test.expectedError.Error() {
				t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
			}
			continue
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
			continue
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
	}
}

func Test_HandleHSTRLEN(t *testing.T) {}

func Test_HandleHVALS(t *testing.T) {}

func Test_HandleHRANDFIELD(t *testing.T) {}

func Test_HandleHLEN(t *testing.T) {}

func Test_HandleHKeys(t *testing.T) {}

func Test_HandleHGETALL(t *testing.T) {}

func Test_HandleHEXISTS(t *testing.T) {}

func Test_HandleHDEL(t *testing.T) {}
