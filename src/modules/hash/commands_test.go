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
			expectedError:    errors.New(utils.WrongArgsResponse),
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
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:           false,
			key:              "key6",
			presetValue:      nil,
			command:          []string{"HINCRBY", "key6", "field1", "23", "45"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
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
			expectedError:    errors.New(utils.WrongArgsResponse),
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
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get lengths on a non hash map returns error
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
		expectedResponse []interface{}
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
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HVALS", "key4", "key4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get lengths on a non hash map returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"HVALS", "key5"},
			expectedResponse: nil,
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
	}
}

func Test_HandleHRANDFIELD(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		withValues       bool
		expectedCount    int
		expectedResponse []string
		expectedError    error
	}{
		{ // Get a random field
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HRANDFIELD", "key1"},
			withValues:       false,
			expectedCount:    1,
			expectedResponse: []string{"field1", "field2", "field3"},
			expectedError:    nil,
		},
		{ // Get a random field with a value
			preset:           true,
			key:              "key2",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HRANDFIELD", "key2", "1", "WITHVALUES"},
			withValues:       true,
			expectedCount:    2,
			expectedResponse: []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
			expectedError:    nil,
		},
		{ // Get several random fields
			preset: true,
			key:    "key3",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:          []string{"HRANDFIELD", "key3", "3"},
			withValues:       false,
			expectedCount:    3,
			expectedResponse: []string{"field1", "field2", "field3", "field4", "field5"},
			expectedError:    nil,
		},
		{ // Get several random fields with their corresponding values
			preset: true,
			key:    "key4",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:       []string{"HRANDFIELD", "key4", "3", "WITHVALUES"},
			withValues:    true,
			expectedCount: 6,
			expectedResponse: []string{
				"field1", "value1", "field2", "123456789", "field3",
				"3.142", "field4", "value4", "field5", "value5",
			},
			expectedError: nil,
		},
		{ // Get the entire hash
			preset: true,
			key:    "key5",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:          []string{"HRANDFIELD", "key5", "5"},
			withValues:       false,
			expectedCount:    5,
			expectedResponse: []string{"field1", "field2", "field3", "field4", "field5"},
			expectedError:    nil,
		},
		{ // Get the entire hash with values
			preset: true,
			key:    "key5",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			command:       []string{"HRANDFIELD", "key5", "5", "WITHVALUES"},
			withValues:    true,
			expectedCount: 10,
			expectedResponse: []string{
				"field1", "value1", "field2", "123456789", "field3",
				"3.142", "field4", "value4", "field5", "value5",
			},
			expectedError: nil,
		},
		{ // Command too short
			preset:        false,
			key:           "key10",
			presetValue:   map[string]interface{}{},
			command:       []string{"HRANDFIELD"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:        false,
			key:           "key11",
			presetValue:   map[string]interface{}{},
			command:       []string{"HRANDFIELD", "key11", "key11", "key11", "key11"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get random field on a non hash map returns error
			preset:        true,
			key:           "key12",
			presetValue:   "Default value",
			command:       []string{"HRANDFIELD", "key12"},
			expectedError: errors.New("value at key12 is not a hash"),
		},
		{ // Throw error when count provided is not an integer
			preset:        true,
			key:           "key12",
			presetValue:   "Default value",
			command:       []string{"HRANDFIELD", "key12", "COUNT"},
			expectedError: errors.New("count must be an integer"),
		},
		{ // If fourth argument is provided, it must be "WITHVALUES"
			preset:        true,
			key:           "key12",
			presetValue:   "Default value",
			command:       []string{"HRANDFIELD", "key12", "10", "FLAG"},
			expectedError: errors.New("result modifier must be withvalues"),
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
		res, err := handleHRANDFIELD(context.Background(), test.command, mockServer, nil)
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
	}
}

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
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HLEN", "key4", "key4"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get lengths on a non hash map returns error
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
			// Return an array containing all the keys of the hash
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HKEYS", "key1"},
			expectedResponse: []string{"field1", "field2", "field3"},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Empty array response when trying to call HKEYS on non-existent key
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
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HKEYS", "key4", "key4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get lengths on a non hash map returns error
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

func Test_HandleHGETALL(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse []string
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			// Return an array containing all the fields and values of the hash
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HGETALL", "key1"},
			expectedResponse: []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Empty array response when trying to call HGETALL on non-existent key
			preset:           false,
			key:              "key2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGETALL", "key2"},
			expectedResponse: []string{},
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGETALL"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HGETALL", "key4", "key4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get lengths on a non hash map returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"HGETALL", "key5"},
			expectedResponse: nil,
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
		res, err := handleHGETALL(context.Background(), test.command, mockServer, nil)
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
		continue
	}
}

func Test_HandleHEXISTS(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			// Return 1 if the field exists in the hash
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			command:          []string{"HEXISTS", "key1", "field1"},
			expectedResponse: 1,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // 0 response when trying to call HEXISTS on non-existent key
			preset:           false,
			key:              "key2",
			presetValue:      map[string]interface{}{},
			command:          []string{"HEXISTS", "key2", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HEXISTS", "key3"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HEXISTS", "key4", "field1", "field2"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get lengths on a non hash map returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"HEXISTS", "key5", "field1"},
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
		res, err := handleHEXISTS(context.Background(), test.command, mockServer, nil)
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
				t.Errorf("expected \"%d\", got \"%d\"", expectedResponse, rv.Integer())
			}
			continue
		}
		t.Error("expected integer response, got another type")
	}
}

func Test_HandleHDEL(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    map[string]interface{}
		expectedError    error
	}{
		{
			// Return count of deleted fields in the specified hash
			preset:           true,
			key:              "key1",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142, "field7": "value7"},
			command:          []string{"HDEL", "key1", "field1", "field2", "field3", "field4", "field5", "field6"},
			expectedResponse: 3,
			expectedValue:    map[string]interface{}{"field1": nil, "field2": nil, "field3": nil, "field7": "value1"},
			expectedError:    nil,
		},
		{ // 0 response when passing delete fields that are non-existent on valid hash
			preset:           true,
			key:              "key2",
			presetValue:      map[string]interface{}{"field1": "value1", "field2": "value2", "field3": "value3"},
			command:          []string{"HDEL", "key2", "field4", "field5", "field6"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{"field1": "value1", "field2": "value2", "field3": "value3"},
			expectedError:    nil,
		},
		{ // 0 response when trying to call HDEL on non-existent key
			preset:           false,
			key:              "key3",
			presetValue:      map[string]interface{}{},
			command:          []string{"HDEL", "key3", "field1"},
			expectedResponse: 0,
			expectedValue:    map[string]interface{}{},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key4",
			presetValue:      map[string]interface{}{},
			command:          []string{"HDEL", "key4"},
			expectedResponse: nil,
			expectedValue:    map[string]interface{}{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Trying to get lengths on a non hash map returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"HDEL", "key5", "field1"},
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
		res, err := handleHDEL(context.Background(), test.command, mockServer, nil)
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
				t.Errorf("expected \"%d\", got \"%d\"", expectedResponse, rv.Integer())
			}
			continue
		}
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		if hash, ok := mockServer.GetValue(test.key).(map[string]interface{}); ok {
			for field, value := range hash {
				if value != test.expectedValue[field] {
					t.Errorf("expected value \"%+v\", got \"%+v\"", test.expectedValue[field], value)
				}
			}
			continue
		}
		t.Error("expected hash value but got another type")
	}
}
