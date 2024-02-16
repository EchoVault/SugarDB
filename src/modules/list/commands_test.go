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

func Test_HandleLRANGE(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse []interface{}
		expectedValue    []interface{}
		expectedError    error
	}{
		{
			// Return sub-list within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "key1", "3", "6"},
			expectedResponse: []interface{}{"value4", "value5", "value6", "value7"},
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // Return sub-list from start index to the end of the list when end index is -1
			preset:           true,
			key:              "key2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "key2", "3", "-1"},
			expectedResponse: []interface{}{"value4", "value5", "value6", "value7", "value8"},
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // Return the reversed sub-list when the end index is greater than -1 but less than start index
			preset:           true,
			key:              "key3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LRANGE", "key3", "3", "0"},
			expectedResponse: []interface{}{"value4", "value3", "value2", "value1"},
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // If key does not exist, return error
			preset:           false,
			key:              "key4",
			presetValue:      nil,
			command:          []string{"LRANGE", "key4", "0", "2"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LRANGE command on non-list item"),
		},
		{ // Command too short
			preset:           false,
			key:              "key5",
			presetValue:      nil,
			command:          []string{"LRANGE", "key5"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key6",
			presetValue:      nil,
			command:          []string{"LRANGE", "key6", "0", "element", "element"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Error when executing command on non-list command
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"LRANGE", "key5", "0", "3"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LRANGE command on non-list item"),
		},
		{ // Error when start index is less than 0
			preset:           true,
			key:              "key7",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LRANGE", "key7", "-1", "3"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{ // Error when start index is higher than the length of the list
			preset:           true,
			key:              "key8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "key8", "10", "11"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{ // Return error when start index is not an integer
			preset:           false,
			key:              "key9",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "key9", "start", "7"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{ // Return error when end index is not an integer
			preset:           false,
			key:              "key10",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "key10", "0", "end"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{ // Error when start and end indices are equal
			preset:           true,
			key:              "key11",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LRANGE", "key11", "1", "1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices cannot be equal"),
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
		res, err := handleLRange(context.Background(), test.command, mockServer, nil)
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
		responseArray := rv.Array()
		if len(responseArray) != len(test.expectedResponse) {
			t.Errorf("expected response of length \"%d\", got \"%d\"", len(test.expectedResponse), len(responseArray))
		}
		for i := 0; i < len(responseArray); i++ {
			if responseArray[i].String() != test.expectedResponse[i] {
				t.Errorf("expected value \"%s\" at index %d, got \"%s\"", test.expectedResponse[i], i, responseArray[i].String())
			}
		}
	}
}

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

func Test_HandleLTRIM(t *testing.T) {
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
		{
			// Return trim within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LTRIM", "key1", "3", "6"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value4", "value5", "value6"},
			expectedError:    nil,
		},
		{ // Return element from start index to end index when end index is greater than length of the list
			preset:           true,
			key:              "key2",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			command:          []string{"LTRIM", "key2", "5", "-1"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value6", "value7", "value8"},
			expectedError:    nil,
		},
		{ // Return error when end index is smaller than start index but greater than -1
			preset:           true,
			key:              "key3",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LTRIM", "key3", "3", "1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("end index must be greater than start index or -1"),
		},
		{ // If key does not exist, return error
			preset:           false,
			key:              "key4",
			presetValue:      nil,
			command:          []string{"LTRIM", "key4", "0", "2"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LTRIM command on non-list item"),
		},
		{ // Command too short
			preset:           false,
			key:              "key5",
			presetValue:      nil,
			command:          []string{"LTRIM", "key5"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key6",
			presetValue:      nil,
			command:          []string{"LTRIM", "key6", "0", "element", "element"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Trying to get element by index on a non-list returns error
			preset:           true,
			key:              "key5",
			presetValue:      "Default value",
			command:          []string{"LTRIM", "key5", "0", "3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("LTRIM command on non-list item"),
		},
		{ // Error when start index is less than 0
			preset:           true,
			key:              "key7",
			presetValue:      []interface{}{"value1", "value2", "value3", "value4"},
			command:          []string{"LTRIM", "key7", "-1", "3"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{ // Error when start index is higher than the length of the list
			preset:           true,
			key:              "key8",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LTRIM", "key8", "10", "11"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start index must be within list boundary"),
		},
		{ // Return error when start index is not an integer
			preset:           false,
			key:              "key9",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LTRIM", "key9", "start", "7"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
		},
		{ // Return error when end index is not an integer
			preset:           false,
			key:              "key10",
			presetValue:      []interface{}{"value1", "value2", "value3"},
			command:          []string{"LTRIM", "key10", "0", "end"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    errors.New("start and end indices must be integers"),
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
		res, err := handleLTrim(context.Background(), test.command, mockServer, nil)
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

func Test_HandleLREM(t *testing.T) {
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
		{ // Remove the first 3 elements that appear in the list
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			command:          []string{"LREM", "key1", "3", "4"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"1", "2", "5", "6", "7", "8", "4", "9", "10", "5", "4"},
			expectedError:    nil,
		},
		{ // Remove the last 3 elements that appear in the list
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			command:          []string{"LREM", "key1", "-3", "4"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"1", "2", "4", "4", "5", "6", "7", "8", "9", "10", "5"},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key5",
			presetValue:      nil,
			command:          []string{"LREM", "key5"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Command too long
			preset:           false,
			key:              "key6",
			presetValue:      nil,
			command:          []string{"LREM", "key6", "0", "element", "element"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // Throw error when count is not an integer
			preset:           false,
			key:              "key7",
			presetValue:      nil,
			command:          []string{"LREM", "key7", "count", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("count must be an integer"),
		},
		{ // Throw error on non-list item
			preset:           true,
			key:              "key8",
			presetValue:      "Default value",
			command:          []string{"LREM", "key8", "0", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LREM command on non-list item"),
		},
		{ // Throw error on non-existent item
			preset:           false,
			key:              "key9",
			presetValue:      "Default value",
			command:          []string{"LREM", "key9", "0", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LREM command on non-list item"),
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
		res, err := handleLRem(context.Background(), test.command, mockServer, nil)
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

func Test_HandleLMOVE(t *testing.T) {}

func Test_HandleLPUSH(t *testing.T) {
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
		{ // LPUSHX to existing list prepends the element to the list
			preset:           true,
			key:              "key1",
			presetValue:      []interface{}{"1", "2", "4", "5"},
			command:          []string{"LPUSHX", "key1", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2", "1", "2", "4", "5"},
			expectedError:    nil,
		},
		{ // LPUSH on existing list prepends the elements to the list
			preset:           true,
			key:              "key2",
			presetValue:      []interface{}{"1", "2", "4", "5"},
			command:          []string{"LPUSH", "key2", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2", "1", "2", "4", "5"},
			expectedError:    nil,
		},
		{ // LPUSH on non-existent list creates the list
			preset:           false,
			key:              "key3",
			presetValue:      nil,
			command:          []string{"LPUSH", "key3", "value1", "value2"},
			expectedResponse: "OK",
			expectedValue:    []interface{}{"value1", "value2"},
			expectedError:    nil,
		},
		{ // Command too short
			preset:           false,
			key:              "key5",
			presetValue:      nil,
			command:          []string{"LPUSH", "key5"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // LPUSHX command returns error on non-existent list
			preset:           false,
			key:              "key6",
			presetValue:      nil,
			command:          []string{"LPUSHX", "key7", "count", "value1"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedError:    errors.New("LPUSHX command on non-list item"),
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
		res, err := handleLPush(context.Background(), test.command, mockServer, nil)
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

func Test_HandleRPUSH(t *testing.T) {}

func Test_HandlePop(t *testing.T) {}
