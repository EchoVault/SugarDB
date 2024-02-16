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
			command:          []string{"LLEN", "key4", "0"},
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
			command:          []string{"LINDEX", "key6", "-1"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("index must be within list range"),
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

func Test_HandleLSET(t *testing.T) {}

func Test_HandleLTRIM(t *testing.T) {}

func Test_HandleLREM(t *testing.T) {}

func Test_HandleLMOVE(t *testing.T) {}

func Test_HandleLPUSH(t *testing.T) {}

func Test_HandleRPUSH(t *testing.T) {}

func Test_HandlePop(t *testing.T) {}
