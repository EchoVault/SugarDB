package list

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_HandleLLEN(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{ // If key exists and is a list, return the lists length
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LLEN", "key1"},
			expectedResponse: 4,
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // If key does not exist, return 0
			preset:           false,
			key:              "key2",
			presetValue:      nil,
			command:          []string{"LLEN", "key2"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      nil,
			command:          []string{"LLEN"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      nil,
			command:          []string{"LLEN", "key4", "key4"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get lengths on a non-list returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"LLEN", "key5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LLEN command on non-list item"),
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
		res, err := handleLLen(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected integer response \"%d\", got \"%d\"", test.expectedResponse, rv.Integer())
		}
	}
}

func Test_HandleLINDEX(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{ // Return last element within range
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "key1", "3"},
			expectedResponse: "value4",
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // Return first element within range
			preset:           true,
			key:              "key2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "key1", "0"},
			expectedResponse: "value1",
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // Return middle element within range
			preset:           true,
			key:              "key3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LINDEX", "key1", "1"},
			expectedResponse: "value2",
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // If key does not exist, return error
			preset:           false,
			key:              "key4",
			presetValue:      nil,
			command:          []string{"LINDEX", "key4", "0"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LINDEX command on non-list item"),
		},
		{ // Command too short
			preset:           false,
			key:              "key3",
			presetValue:      nil,
			command:          []string{"LINDEX", "key3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key4",
			presetValue:      nil,
			command:          []string{"LINDEX", "key4", "0", "20"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get element by index on a non-list returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"LINDEX", "key5", "0"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LINDEX command on non-list item"),
		},
		{ // Trying to get index out of range index beyond last index
			preset:           true,
			key:              "key6",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "key6", "3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{ // Trying to get index out of range with negative index
			preset:           true,
			key:              "key7",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "key7", "-1"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{ // Return error when index is not an integer
			preset:           false,
			key:              "key8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LINDEX", "key8", "index"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be an integer"),
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
		res, err := handleLIndex(context.Background(), test.command, mockServer, nil)
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
		if rv.String() != test.expectedResponse {
			t.Errorf("expected response \"%s\", got \"%s\"", test.expectedResponse, rv.String())
		}
	}
}

func Test_HandleLRANGE(t *testing.T) {}

func Test_HandleLSET(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{ // Return last element within range
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LSET", "key1", "3", "new-value"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2", "value3", "new-value"},
			expectedError:    nil,
		},
		{ // Return first element within range
			preset:           true,
			key:              "key2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LSET", "key2", "0", "new-value"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"new-value", "value2", "value3", "value4"},
			expectedError:    nil,
		},
		{ // Return middle element within range
			preset:           true,
			key:              "key3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LSET", "key3", "1", "new-value"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "new-value", "value3", "value4"},
			expectedError:    nil,
		},
		{ // If key does not exist, return error
			preset:           false,
			key:              "key4",
			presetValue:      nil,
			command:          []string{"LSET", "key4", "0", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LSET command on non-list item"),
		},
		{ // Command too short
			preset:           false,
			key:              "key5",
			presetValue:      nil,
			command:          []string{"LSET", "key5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key6",
			presetValue:      nil,
			command:          []string{"LSET", "key6", "0", "element", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get element by index on a non-list returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"LSET", "key5", "0", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LSET command on non-list item"),
		},
		{ // Trying to get index out of range index beyond last index
			preset:           true,
			key:              "key6",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LSET", "key6", "3", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{ // Trying to get index out of range with negative index
			preset:           true,
			key:              "key7",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LSET", "key7", "-1", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
		},
		{ // Return error when index is not an integer
			preset:           false,
			key:              "key8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LSET", "key8", "index", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be an integer"),
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
		res, err := handleLSet(context.Background(), test.command, mockServer, nil)
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
		if rv.String() != test.expectedResponse {
			t.Errorf("expected \"%s\" response, got \"%s\"", test.expectedResponse, rv.String())
		}
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		list, ok := mockServer.GetValue(test.key).([]interface{})
		if !ok {
			t.Error("expected value to be list, got another type")
		}
		if len(list) != len(test.expectedValue) {
			t.Errorf("expected list length to be %d, got %d", len(test.expectedValue), len(list))
		}
		for i := 0; i < len(list); i++ {
			if list[i] != test.expectedValue[i] {
				t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedValue[i], list[i])
			}
		}
		mockServer.KeyRUnlock(test.key)
	}
}

func Test_HandleLTRIM(t *testing.T) {}

func Test_HandleLREM(t *testing.T) {}

func Test_HandleLMOVE(t *testing.T) {}

func Test_HandleLPUSH(t *testing.T) {}

func Test_HandleRPUSH(t *testing.T) {}

func Test_HandlePop(t *testing.T) {}
