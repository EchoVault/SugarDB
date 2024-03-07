package generic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_HandleSET(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		command          []string
		expectedResponse string
		expectedValue    interface{}
		expectedErr      error
	}{
		{
			command:          []string{"SET", "test", "value"},
			expectedResponse: "OK",
			expectedValue:    "value",
			expectedErr:      nil,
		},
		{
			command:          []string{"SET", "integer", "1245678910"},
			expectedResponse: "OK",
			expectedValue:    1245678910,
			expectedErr:      nil,
		},
		{
			command:          []string{"SET", "float", "45782.11341"},
			expectedResponse: "OK",
			expectedValue:    45782.11341,
			expectedErr:      nil,
		},
		{
			command:          []string{"SET"},
			expectedResponse: "",
			expectedValue:    nil,
			expectedErr:      errors.New(utils.WrongArgsResponse),
		},
		{
			command:          []string{"SET", "test", "one", "two", "three", "four", "five", "eight"},
			expectedResponse: "",
			expectedValue:    nil,
			expectedErr:      errors.New(utils.WrongArgsResponse),
		},
	}

	for _, test := range tests {
		res, err := handleSet(context.Background(), test.command, mockServer, nil)
		if test.expectedErr != nil {
			if err.Error() != test.expectedErr.Error() {
				t.Errorf("expected error %s, got %s", test.expectedErr.Error(), err.Error())
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
		if rv.String() != test.expectedResponse {
			t.Errorf("expected response %s, got %s", test.expectedResponse, rv.String())
		}
		value := mockServer.GetValue(context.Background(), test.command[1])
		switch value.(type) {
		default:
			t.Error("unexpected type for expectedValue")
		case int:
			testValue, ok := test.expectedValue.(int)
			if !ok {
				t.Error("expected integer value but got another type")
			}
			if value != testValue {
				t.Errorf("expected value %d, got: %d", testValue, value)
			}
		case float64:
			testValue, ok := test.expectedValue.(float64)
			if !ok {
				t.Error("expected float value but got another type")
			}
			if value != testValue {
				t.Errorf("expected value %f, got: %f", testValue, value)
			}
		case string:
			testValue, ok := test.expectedValue.(string)
			if !ok {
				t.Error("expected string value but got another type")
			}
			if value != testValue {
				t.Errorf("expected value %s, got: %s", testValue, value)
			}
		}
	}
}

func Test_HandleMSET(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		command          []string
		expectedResponse string
		expectedValues   map[string]interface{}
		expectedErr      error
	}{
		{
			command:          []string{"SET", "test1", "value1", "test2", "10", "test3", "3.142"},
			expectedResponse: "OK",
			expectedValues:   map[string]interface{}{"test1": "value1", "test2": 10, "test3": 3.142},
			expectedErr:      nil,
		},
		{
			command:          []string{"SET", "test1", "value1", "test2", "10", "test3"},
			expectedResponse: "",
			expectedValues:   make(map[string]interface{}),
			expectedErr:      errors.New("each key must be paired with a value"),
		},
	}

	for _, test := range tests {
		res, err := handleMSet(context.Background(), test.command, mockServer, nil)
		if test.expectedErr != nil {
			if err.Error() != test.expectedErr.Error() {
				t.Errorf("expected error %s, got %s", test.expectedErr.Error(), err.Error())
			}
			continue
		}
		rd := resp.NewReader(bytes.NewBuffer(res))
		rv, _, err := rd.ReadValue()
		if err != nil {
			t.Error(err)
		}
		if rv.String() != test.expectedResponse {
			t.Errorf("expected response %s, got %s", test.expectedResponse, rv.String())
		}
		for key, expectedValue := range test.expectedValues {
			if _, err = mockServer.KeyRLock(context.Background(), key); err != nil {
				t.Error(err)
			}
			switch expectedValue.(type) {
			default:
				t.Error("unexpected type for expectedValue")
			case int:
				ev, _ := expectedValue.(int)
				value, ok := mockServer.GetValue(context.Background(), key).(int)
				if !ok {
					t.Errorf("expected integer type for key %s, got another type", key)
				}
				if value != ev {
					t.Errorf("expected value %d for key %s, got %d", ev, key, value)
				}
			case float64:
				ev, _ := expectedValue.(float64)
				value, ok := mockServer.GetValue(context.Background(), key).(float64)
				if !ok {
					t.Errorf("expected float type for key %s, got another type", key)
				}
				if value != ev {
					t.Errorf("expected value %f for key %s, got %f", ev, key, value)
				}
			case string:
				ev, _ := expectedValue.(string)
				value, ok := mockServer.GetValue(context.Background(), key).(string)
				if !ok {
					t.Errorf("expected string type for key %s, got another type", key)
				}
				if value != ev {
					t.Errorf("expected value %s for key %s, got %s", ev, key, value)
				}
			}
			mockServer.KeyRUnlock(key)
		}
	}
}

func Test_HandleGET(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

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
			if !bytes.Equal(res, []byte(fmt.Sprintf("+%v\r\n", value))) {
				t.Errorf("expected %s, got: %s", fmt.Sprintf("+%v\r\n", value), string(res))
			}
		}(test.key, test.value)
	}

	// Test get non-existent key
	res, err := handleGet(context.Background(), []string{"GET", "test4"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(res, []byte("$-1\r\n")) {
		t.Errorf("expected %+v, got: %+v", "+nil\r\n", res)
	}

	errorTests := []struct {
		command  []string
		expected string
	}{
		{
			command:  []string{"GET"},
			expected: utils.WrongArgsResponse,
		},
		{
			command:  []string{"GET", "key", "test"},
			expected: utils.WrongArgsResponse,
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
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		presetKeys    []string
		presetValues  []string
		command       []string
		expected      []interface{}
		expectedError error
	}{
		{
			presetKeys:    []string{"test1", "test2", "test3", "test4"},
			presetValues:  []string{"value1", "value2", "value3", "value4"},
			command:       []string{"MGET", "test1", "test4", "test2", "test3", "test1"},
			expected:      []interface{}{"value1", "value4", "value2", "value3", "value1"},
			expectedError: nil,
		},
		{
			presetKeys:    []string{"test5", "test6", "test7"},
			presetValues:  []string{"value5", "value6", "value7"},
			command:       []string{"MGET", "test5", "test6", "non-existent", "non-existent", "test7", "non-existent"},
			expected:      []interface{}{"value5", "value6", nil, nil, "value7", nil},
			expectedError: nil,
		},
		{
			presetKeys:    []string{"test5"},
			presetValues:  []string{"value5"},
			command:       []string{"MGET"},
			expected:      nil,
			expectedError: errors.New(utils.WrongArgsResponse),
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
			if test.expected[i] == nil {
				if !value.IsNull() {
					t.Errorf("expected nil value, got %+v", value)
				}
				continue
			}
			if value.String() != test.expected[i] {
				t.Errorf("expected value %s, got: %s", test.expected[i], value.String())
			}
		}
	}
}
