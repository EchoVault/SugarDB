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
