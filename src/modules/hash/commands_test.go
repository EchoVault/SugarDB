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
}

func Test_HandleHGET(t *testing.T) {}

func Test_HandleHSTRLEN(t *testing.T) {}

func Test_HandleHVALS(t *testing.T) {}

func Test_HandleHRANDFIELD(t *testing.T) {}

func Test_HandleHLEN(t *testing.T) {}

func Test_HandleHKeys(t *testing.T) {}

func Test_HandleHGETALL(t *testing.T) {}

func Test_HandleHEXISTS(t *testing.T) {}

func Test_HandleHDEL(t *testing.T) {}
