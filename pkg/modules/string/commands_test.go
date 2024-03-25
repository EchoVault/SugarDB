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

package str

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/pkg/echovault"
	"github.com/echovault/echovault/pkg/utils"
	"github.com/tidwall/resp"
	"strconv"
	"testing"
)

var mockServer *echovault.EchoVault

func init() {
	mockServer = echovault.NewEchoVault(
		echovault.WithConfig(internal.Config{
			DataDir:        "",
			EvictionPolicy: utils.NoEviction,
		}),
	)
}

func Test_HandleSetRange(t *testing.T) {
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
			key:              "SetRangeKey1",
			presetValue:      "",
			command:          []string{"SETRANGE", "SetRangeKey1", "10", "New String Value"},
			expectedValue:    "New String Value",
			expectedResponse: len("New String Value"),
			expectedError:    nil,
		},
		{ // Test SETRANGE with an offset that leads to a longer resulting string
			preset:           true,
			key:              "SetRangeKey2",
			presetValue:      "Original String Value",
			command:          []string{"SETRANGE", "SetRangeKey2", "16", "Portion Replaced With This New String"},
			expectedValue:    "Original String Portion Replaced With This New String",
			expectedResponse: len("Original String Portion Replaced With This New String"),
			expectedError:    nil,
		},
		{ // SETRANGE with negative offset prepends the string
			preset:           true,
			key:              "SetRangeKey3",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "SetRangeKey3", "-10", "Prepended "},
			expectedValue:    "Prepended This is a preset value",
			expectedResponse: len("Prepended This is a preset value"),
			expectedError:    nil,
		},
		{ // SETRANGE with offset that embeds new string inside the old string
			preset:           true,
			key:              "SetRangeKey4",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "SetRangeKey4", "0", "That"},
			expectedValue:    "That is a preset value",
			expectedResponse: len("That is a preset value"),
			expectedError:    nil,
		},
		{ // SETRANGE with offset longer than original lengths appends the string
			preset:           true,
			key:              "SetRangeKey5",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "SetRangeKey5", "100", " Appended"},
			expectedValue:    "This is a preset value Appended",
			expectedResponse: len("This is a preset value Appended"),
			expectedError:    nil,
		},
		{ // SETRANGE with offset on the last character replaces last character with new string
			preset:           true,
			key:              "SetRangeKey6",
			presetValue:      "This is a preset value",
			command:          []string{"SETRANGE", "SetRangeKey6", strconv.Itoa(len("This is a preset value") - 1), " replaced"},
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
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Command too long
			preset:           false,
			command:          []string{"SETRANGE", "key", "offset", "value", "value1"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SETRANGE, %d", i))

		// If there's a preset step, carry it out here
		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, utils.AdaptType(test.presetValue)); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}

		res, err := handleSetRange(ctx, test.command, mockServer, nil)
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

		// Get the value from the echovault and check against the expected value
		if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
			t.Error(err)
		}
		value, ok := mockServer.GetValue(ctx, test.key).(string)
		if !ok {
			t.Error("expected string data type, got another type")
		}
		if value != test.expectedValue {
			t.Errorf("expected value \"%s\", got \"%s\"", test.expectedValue, value)
		}
		mockServer.KeyRUnlock(ctx, test.key)
	}
}

func Test_HandleStrLen(t *testing.T) {
	tests := []struct {
		preset           bool
		key              string
		presetValue      string
		command          []string
		expectedResponse int
		expectedError    error
	}{
		{ // Return the correct string length for an existing string
			preset:           true,
			key:              "StrLenKey1",
			presetValue:      "Test String",
			command:          []string{"STRLEN", "StrLenKey1"},
			expectedResponse: len("Test String"),
			expectedError:    nil,
		},
		{ // If the string does not exist, return 0
			preset:           false,
			key:              "StrLenKey2",
			presetValue:      "",
			command:          []string{"STRLEN", "StrLenKey2"},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // Too few args
			preset:           false,
			key:              "StrLenKey3",
			presetValue:      "",
			command:          []string{"STRLEN"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // Too many args
			preset:           false,
			key:              "StrLenKey4",
			presetValue:      "",
			command:          []string{"STRLEN", "StrLenKey4", "StrLenKey5"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("STRLEN, %d", i))

		if test.preset {
			_, err := mockServer.CreateKeyAndLock(ctx, test.key)
			if err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleStrLen(ctx, test.command, mockServer, nil)
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
			t.Errorf("expected respons \"%d\", got \"%d\"", test.expectedResponse, rv.Integer())
		}
	}
}

func Test_HandleSubStr(t *testing.T) {
	tests := []struct {
		preset           bool
		key              string
		presetValue      string
		command          []string
		expectedResponse string
		expectedError    error
	}{
		{ // Return substring within the range of the string
			preset:           true,
			key:              "SubStrKey1",
			presetValue:      "Test String One",
			command:          []string{"SUBSTR", "SubStrKey1", "5", "10"},
			expectedResponse: "String",
			expectedError:    nil,
		},
		{ // Return substring at the end of the string with exact end index
			preset:           true,
			key:              "SubStrKey2",
			presetValue:      "Test String Two",
			command:          []string{"SUBSTR", "SubStrKey2", "12", "14"},
			expectedResponse: "Two",
			expectedError:    nil,
		},
		{ // Return substring at the end of the string with end index greater than length
			preset:           true,
			key:              "SubStrKey3",
			presetValue:      "Test String Three",
			command:          []string{"SUBSTR", "SubStrKey3", "12", "75"},
			expectedResponse: "Three",
			expectedError:    nil,
		},
		{ // Return the substring at the start of the string with 0 start index
			preset:           true,
			key:              "SubStrKey4",
			presetValue:      "Test String Four",
			command:          []string{"SUBSTR", "SubStrKey4", "0", "3"},
			expectedResponse: "Test",
			expectedError:    nil,
		},
		{
			// Return the substring with negative start index.
			// Substring should begin abs(start) from the end of the string when start is negative.
			preset:           true,
			key:              "SubStrKey5",
			presetValue:      "Test String Five",
			command:          []string{"SUBSTR", "SubStrKey5", "-11", "10"},
			expectedResponse: "String",
			expectedError:    nil,
		},
		{
			// Return reverse substring with end index smaller than start index.
			// When end index is smaller than start index, the 2 indices are reversed.
			preset:           true,
			key:              "SubStrKey6",
			presetValue:      "Test String Six",
			command:          []string{"SUBSTR", "SubStrKey6", "4", "0"},
			expectedResponse: "tseT",
			expectedError:    nil,
		},
		{ // Command too short
			command:       []string{"SUBSTR", "key", "10"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{
			// Command too long
			command:       []string{"SUBSTR", "key", "10", "15", "20"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // Start index is not an integer
			command:       []string{"SUBSTR", "key", "start", "10"},
			expectedError: errors.New("start and end indices must be integers"),
		},
		{ // End index is not an integer
			command:       []string{"SUBSTR", "key", "0", "end"},
			expectedError: errors.New("start and end indices must be integers"),
		},
		{ // Non-existent key
			command:       []string{"SUBSTR", "non-existent-key", "0", "10"},
			expectedError: errors.New("key non-existent-key does not exist"),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SUBSTR, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSubStr(ctx, test.command, mockServer, nil)
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
