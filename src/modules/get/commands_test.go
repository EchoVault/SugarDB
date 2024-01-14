package get

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/mock/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_HandleGET(t *testing.T) {
	mockServer := server.NewMockServer()

	tests := []struct {
		key   string
		value string
	}{
		{
			key:   "test1",
			value: "value1",
		},
		{
			key:   "test2",
			value: "10",
		},
		{
			key:   "test3",
			value: "3.142",
		},
	}
	// Test successful GET command
	for _, test := range tests {
		func(key, value string) {
			ctx := context.Background()

			_, err := mockServer.CreateKeyAndLock(ctx, key)
			if err != nil {
				t.Error(err)
			}
			mockServer.SetValue(ctx, key, value)
			mockServer.KeyUnlock(key)

			res, err := handleGet(ctx, []string{"GET", key}, mockServer, nil)
			if err != nil {
				t.Error(err)
			}
			if !bytes.Equal(res, []byte(fmt.Sprintf("+%v\r\n\r\n", value))) {
				t.Errorf("expected %s, got: %s", fmt.Sprintf("+%v\r\n\r\n", value), string(res))
			}
		}(test.key, test.value)
	}

	// Test get non-existent key
	res, err := handleGet(context.Background(), []string{"GET", "test4"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(res, []byte("+nil\r\n\r\n")) {
		t.Errorf("expected %+v, got: %+v", "+nil\r\n\r\n", res)
	}

	errorTests := []struct {
		command  []string
		expected string
	}{
		{
			command:  []string{"GET"},
			expected: utils.WRONG_ARGS_RESPONSE,
		},
		{
			command:  []string{"GET", "key", "test"},
			expected: utils.WRONG_ARGS_RESPONSE,
		},
	}
	for _, test := range errorTests {
		res, err = handleGet(context.Background(), test.command, mockServer, nil)
		if res != nil {
			t.Errorf("expected nil response, got: %+v", res)
		}
		if err.Error() != test.expected {
			t.Errorf("expected error '%s', got: %s", test.expected, err.Error())
		}
	}
}

func Test_HandleMGET(t *testing.T) {
	mockServer := server.NewMockServer()

	tests := []struct {
		presetKeys    []string
		presetValues  []string
		command       []string
		expected      []string
		expectedError error
	}{
		{
			presetKeys:    []string{"test1", "test2", "test3", "test4"},
			presetValues:  []string{"value1", "value2", "value3", "value4"},
			command:       []string{"MGET", "test1", "test4", "test2", "test3", "test1"},
			expected:      []string{"value1", "value4", "value2", "value3", "value1"},
			expectedError: nil,
		},
		{
			presetKeys:    []string{"test5", "test6", "test7"},
			presetValues:  []string{"value5", "value6", "value7"},
			command:       []string{"MGET", "test5", "test6", "non-existent", "non-existent", "test7", "non-existent"},
			expected:      []string{"value5", "value6", "nil", "nil", "value7", "nil"},
			expectedError: nil,
		},
		{
			presetKeys:    []string{"test5"},
			presetValues:  []string{"value5"},
			command:       []string{"MGET"},
			expected:      nil,
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		// Set up the values
		for i, key := range test.presetKeys {
			_, err := mockServer.CreateKeyAndLock(context.Background(), key)
			if err != nil {
				t.Error(err)
			}
			mockServer.SetValue(context.Background(), key, test.presetValues[i])
			mockServer.KeyUnlock(key)
		}
		// Test the command and its results
		res, err := handleMGet(context.Background(), test.command, mockServer, nil)
		if test.expectedError != nil {
			// If we expect and error, branch out and check error
			if err.Error() != test.expectedError.Error() {
				t.Errorf("expected error %+v, got: %+v", test.expectedError, err)
			}
			continue
		}
		if err != nil {
			t.Error(err)
		}
		rr := resp.NewReader(bytes.NewBuffer(res))
		rv, _, err := rr.ReadValue()
		if err != nil {
			t.Error(err)
		}
		if rv.Type().String() != "Array" {
			t.Errorf("expected type Array, got: %s", rv.Type().String())
		}
		for i, value := range rv.Array() {
			if value.String() != test.expected[i] {
				t.Errorf("expected value %s, got: %s", test.expected[i], value.String())
			}
		}
	}
}
