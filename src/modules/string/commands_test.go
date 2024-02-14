package str

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"strconv"
	"testing"
)

func Test_HandleSetRange(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      string
		command          []string
		expectedValue    string
		expectedResponse int
		expectedError    error
	}{
		{ // Test that SETRANGE on non-existent string creates new string
			preset:           false,
			key:              "test1",
			presetValue:      "",
			command:          []string{"SETRANGE", "test1", "10", "New String Value"},
			expectedValue:    "New String Value",
			expectedResponse: len("New String Value"),
			expectedError:    nil,
		},
		{ // Test SETRANGE with an offset that leads to a longer resulting string
			preset:           true,
			key:              "test2",
			presetValue:      "Original String Value",
			command:          []string{"SETRANGE", "test2", "16", "Portion Replaced With This New String"},
			expectedValue:    "Original String Portion Replaced With This New String",
			expectedResponse: len("Original String Portion Replaced With This New String"),
			expectedError:    nil,
		},
		{ // SETRANGE with negative offset prepends the string
			preset:           true,
			key:              "test3",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "test3", "-10", "Prepended "},
			expectedValue:    "Prepended This is a preset value",
			expectedResponse: len("Prepended This is a preset value"),
			expectedError:    nil,
		},
		{ // SETRANGE with offset that embeds new string inside the old string
			preset:           true,
			key:              "test4",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "test4", "0", "That"},
			expectedValue:    "That is a preset value",
			expectedResponse: len("That is a preset value"),
			expectedError:    nil,
		},
		{ // SETRANGE with offset longer than original lengths appends the string
			preset:           true,
			key:              "test5",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "test5", "100", " Appended"},
			expectedValue:    "This is a preset value Appended",
			expectedResponse: len("This is a preset value Appended"),
			expectedError:    nil,
		},
		{ // SETRANGE with offset on the last character replaces last character with new string
			preset:           true,
			key:              "test6",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "test6", strconv.Itoa(len("This is a preset value") - 1), " replaced"},
			expectedValue:    "This is a preset valu replaced",
			expectedResponse: len("This is a preset valu replaced"),
			expectedError:    nil,
		},
		{ // Offset not integer
			preset:           false,
			command:          []string{"SETRANGE", "key", "offset", "value"},
			expectedResponse: 0,
			expectedError:    errors.New("offset must be an integer"),
		},
		{ // SETRANGE target is not a string
			preset:           true,
			key:              "test-int",
			presetValue:      "10",
			command:          []string{"SETRANGE", "test-int", "10", "value"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key test-int is not a string"),
		},
		{ // Command too short
			preset:           false,
			command:          []string{"SETRANGE", "key"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			command:          []string{"SETRANGE", "key", "offset", "value", "value1"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		// If there's a preset step, carry it out here
		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(context.Background(), test.key); err != nil {
				t.Error(err)
			}
			mockServer.SetValue(context.Background(), test.key, utils.AdaptType(test.presetValue))
			mockServer.KeyUnlock(test.key)
		}

		res, err := handleSetRange(context.Background(), test.command, mockServer, nil)
		if test.expectedError != nil {
			if err.Error() != test.expectedError.Error() {
				t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
			}
			continue
		}
		if err != nil {
			t.Error(err)
		}
		rd := resp.NewReader(bytes.NewBuffer(res))
		rv, _, err := rd.ReadValue()
		if err != nil {
			t.Error(err)
		}
		if rv.Integer() != test.expectedResponse {
			t.Errorf("expected response \"%d\", got \"%d\"", test.expectedResponse, rv.Integer())
		}

		// Get the value from the server and check against the expected value
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		value, ok := mockServer.GetValue(test.key).(string)
		if !ok {
			t.Error("expected string data type, got another type")
		}
		if value != test.expectedValue {
			t.Errorf("expected value \"%s\", got \"%s\"", test.expectedValue, value)
		}
		mockServer.KeyRUnlock(test.key)
	}
}

func Test_HandleStrLen(t *testing.T) {}

func Test_HandleSubStr(t *testing.T) {}
