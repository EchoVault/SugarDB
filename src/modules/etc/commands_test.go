package etc

import (
	"bytes"
	"context"
	"errors"
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
			command:          []string{"SET", "test", "one", "two", "three"},
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
		value := mockServer.GetValue(test.command[1])
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

func Test_HandleSETNX(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	res, err := handleSetNX(context.Background(), []string{"SET", "test", "Test_HandleSETNX"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	// Try to set existing key again
	res, err = handleSetNX(context.Background(), []string{"SET", "test", "Test_HandleSETNX_2"}, mockServer, nil)
	if res != nil {
		t.Errorf("exptected nil response, got: %+v", res)
	}
	if err.Error() != "key test already exists" {
		t.Errorf("expected key test already exists, got %s", err.Error())
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
				value, ok := mockServer.GetValue(key).(int)
				if !ok {
					t.Errorf("expected integer type for key %s, got another type", key)
				}
				if value != ev {
					t.Errorf("expected value %d for key %s, got %d", ev, key, value)
				}
			case float64:
				ev, _ := expectedValue.(float64)
				value, ok := mockServer.GetValue(key).(float64)
				if !ok {
					t.Errorf("expected float type for key %s, got another type", key)
				}
				if value != ev {
					t.Errorf("expected value %f for key %s, got %f", ev, key, value)
				}
			case string:
				ev, _ := expectedValue.(string)
				value, ok := mockServer.GetValue(key).(string)
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
