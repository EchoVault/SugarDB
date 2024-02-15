package hash

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"slices"
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

func Test_HandleHSTRLEN(t *testing.T) {
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
		{
			// Return lengths of field values.
			// If the key does not exist, its length should be 0.
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HSTRLEN", "key1", "field1", "field2", "field3", "field4"},
			expectedResponse: []int{len("value1"), len("123456789"), len("3.142"), 0},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Nil response when trying to get HSTRLEN non-existent key
			preset:           false,
			key:              "key2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSTRLEN", "key2", "field1"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HSTRLEN", "key3"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get lengths on a non hasp map returns error
			preset:           true,
			key:              "key4",
			presetValue:      "Default value",
			command:          []string{"HSTRLEN", "key4", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at key4 is not a hash"),
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
		res, err := handleHSTRLEN(context.Background(), test.command, mockServer, nil)
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
		expectedResponse, _ := test.expectedResponse.([]int)
		for i, v := range rv.Array() {
			if v.Integer() != expectedResponse[i] {
				t.Errorf("expected \"%d\", got \"%d\"", expectedResponse[i], v.Integer())
			}
		}
	}
}

func Test_HandleHVALS(t *testing.T) {
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
		{
			// Return all the values from a hash
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HVALS", "key1"},
			expectedResponse: []interface{}{"value1", 123456789, "3.142"},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Empty array response when trying to get HSTRLEN non-existent key
			preset:           false,
			key:              "key2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HVALS", "key2"},
			expectedResponse: []interface{}{},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HVALS"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HVALS", "key4", "key4"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get lengths on a non hasp map returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"HSTRLEN", "key5"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at key5 is not a hash"),
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
		res, err := handleHVALS(context.Background(), test.command, mockServer, nil)
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
		expectedResponse, _ := test.expectedResponse.([]interface{})
		switch len(expectedResponse) {
		case 0:
			if len(rv.Array()) != 0 {
				t.Errorf("expected empty array, got length \"%d\"", len(rv.Array()))
			}
		default:
			for i, v := range rv.Array() {
				switch v.Type().String() {
				default:
					t.Errorf("unexpected error type")
				case "Integer":
					if expected, ok := expectedResponse[i].(int); ok {
						if v.Integer() != expected {
							t.Errorf("expected integer \"%d\", got \"%d\"", expected, v.Integer())
						}
						continue
					}
					t.Error("expected response should be integer")
				case "BulkString":
					if expected, ok := expectedResponse[i].(string); ok {
						if v.String() != expected {
							t.Errorf("expected string \"%s\", got \"%s\"", expected, v.String())
						}
						continue
					}
					t.Errorf("expected response should be string")
				}
			}
		}
	}
}

// TODO: EDIT THIS TEST
// func Test_HandleHRANDFIELD(t *testing.T) {
// 	// TODO: Customise this test plan
// 	mockServer := server.NewServer(server.Opts{})
//
// 	tests := []struct {
// 		preset           bool
// 		key              string
// 		presetValue      interface{}
// 		command          []string
// 		expectedResponse interface{} // Change count
// 		expectedValue    map[string]interface{}
// 		expectedError    error
// 	}{
// 		{
// 			// Return all the values from a hash
// 			preset:           true,
// 			key:              "key1",
// 			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
// 			command:          []string{"HVALS", "key1"},
// 			expectedResponse: []interface{}{"value1", 123456789, "3.142"},
// 			expectedValue:    map[string]interface{}{},
// 			expectedError:    nil,
// 		},
// 		{ // Empty array response when trying to get HSTRLEN non-existent key
// 			preset:           false,
// 			key:              "key2",
// 			presetValue:      map[string]interface{}{},
// 			command:          []string{"HVALS", "key2"},
// 			expectedResponse: []interface{}{},
// 			expectedValue:    map[string]interface{}{},
// 			expectedError:    nil,
// 		},
// 		{ // Command too short
// 			preset:           false,
// 			key:              "key3",
// 			presetValue:      map[string]interface{}{},
// 			command:          []string{"HVALS"},
// 			expectedResponse: 0,
// 			expectedValue:    map[string]interface{}{},
// 			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
// 		},
// 		{ // Command too long
// 			preset:           false,
// 			key:              "key4",
// 			presetValue:      map[string]interface{}{},
// 			command:          []string{"HVALS", "key4", "key4"},
// 			expectedResponse: 0,
// 			expectedValue:    map[string]interface{}{},
// 			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
// 		},
// 		{ // Trying to get lengths on a non hasp map returns error
// 			preset:           true,
// 			key:              "key5",
// 			presetValue:      "Default value",
// 			command:          []string{"HSTRLEN", "key5"},
// 			expectedResponse: 0,
// 			expectedValue:    map[string]interface{}{},
// 			expectedError:    errors.New("value at key5 is not a hash"),
// 		},
// 	}
//
// 	for _, test := range tests {
// 		if test.preset {
// 			if _, err := mockServer.CreateKeyAndLock(context.Background(), test.key); err != nil {
// 				t.Error(err)
// 			}
// 			mockServer.SetValue(context.Background(), test.key, test.presetValue)
// 			mockServer.KeyUnlock(test.key)
// 		}
// 		res, err := handleHVALS(context.Background(), test.command, mockServer, nil)
// 		if test.expectedError != nil {
// 			if err.Error() != test.expectedError.Error() {
// 				t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
// 			}
// 			continue
// 		}
// 		rd := resp.NewReader(bytes.NewBuffer(res))
// 		rv, _, err := rd.ReadValue()
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		expectedResponse, _ := test.expectedResponse.([]interface{})
// 		switch len(expectedResponse) {
// 		case 0:
// 			if len(rv.Array()) != 0 {
// 				t.Errorf("expected empty array, got length \"%d\"", len(rv.Array()))
// 			}
// 		default:
// 			for i, v := range rv.Array() {
// 				switch v.Type().String() {
// 				default:
// 					t.Errorf("unexpected error type")
// 				case "Integer":
// 					if expected, ok := expectedResponse[i].(int); ok {
// 						if v.Integer() != expected {
// 							t.Errorf("expected integer \"%d\", got \"%d\"", expected, v.Integer())
// 						}
// 						continue
// 					}
// 					t.Error("expected response should be integer")
// 				case "BulkString":
// 					if expected, ok := expectedResponse[i].(string); ok {
// 						if v.String() != expected {
// 							t.Errorf("expected string \"%s\", got \"%s\"", expected, v.String())
// 						}
// 						continue
// 					}
// 					t.Errorf("expected response should be string")
// 				}
// 			}
// 		}
// 	}
// }

func Test_HandleHLEN(t *testing.T) {
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
		{
			// Return the correct length of the hash
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HLEN", "key1"},
			expectedResponse: 3,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // 0 response when trying to call HLEN on non-existent key
			preset:           false,
			key:              "key2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HLEN", "key2"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HLEN"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HLEN", "key4", "key4"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get lengths on a non hasp map returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"HLEN", "key5"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at key5 is not a hash"),
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
		res, err := handleHLEN(context.Background(), test.command, mockServer, nil)
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
		if expectedResponse, ok := test.expectedResponse.(int); ok {
			if rv.Integer() != expectedResponse {
				t.Errorf("expected ineger \"%d\", got \"%d\"", expectedResponse, rv.Integer())
			}
			continue
		}
		t.Error("expected integer response, got another type")
	}
}

func Test_HandleHKeys(t *testing.T) {
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
		{
			// Return the correct length of the hash
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HKEYS", "key1"},
			expectedResponse: []string{"field1", "field2", "field3"},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Empty array response when trying to call HLEN on non-existent key
			preset:           false,
			key:              "key2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HKEYS", "key2"},
			expectedResponse: []string{},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HKEYS"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HKEYS", "key4", "key4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get lengths on a non hasp map returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"HKEYS", "key5"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New("value at key5 is not a hash"),
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
		res, err := handleHKEYS(context.Background(), test.command, mockServer, nil)
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
			continue
		}
		t.Error("expected array response, got another type")
	}
}

func Test_HandleHGETALL(t *testing.T) {}

func Test_HandleHEXISTS(t *testing.T) {}

func Test_HandleHDEL(t *testing.T) {}
