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

package generic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal/clock"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/echovault"
	"github.com/echovault/echovault/pkg/modules/generic"
	"github.com/echovault/echovault/pkg/types"
	"github.com/tidwall/resp"
	"net"
	"strings"
	"testing"
	"time"
)

var mockServer *echovault.EchoVault

var mockClock clock.Clock

type KeyData struct {
	Value    interface{}
	ExpireAt time.Time
}

func init() {
	mockClock = clock.NewClock()

	mockServer, _ = echovault.NewEchoVault(
		echovault.WithCommands(generic.Commands()),
		echovault.WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)
}

func getHandler(commands ...string) types.HandlerFunc {
	if len(commands) == 0 {
		return nil
	}
	for _, c := range mockServer.GetAllCommands() {
		if strings.EqualFold(commands[0], c.Command) && len(commands) == 1 {
			// Get command handler
			return c.HandlerFunc
		}
		if strings.EqualFold(commands[0], c.Command) {
			// Get sub-command handler
			for _, sc := range c.SubCommands {
				if strings.EqualFold(commands[1], sc.Command) {
					return sc.HandlerFunc
				}
			}
		}
	}
	return nil
}

func getHandlerFuncParams(ctx context.Context, cmd []string, conn *net.Conn) types.HandlerFuncParams {
	return types.HandlerFuncParams{
		Context:          ctx,
		Command:          cmd,
		Connection:       conn,
		KeyExists:        mockServer.KeyExists,
		CreateKeyAndLock: mockServer.CreateKeyAndLock,
		KeyLock:          mockServer.KeyLock,
		KeyRLock:         mockServer.KeyRLock,
		KeyUnlock:        mockServer.KeyUnlock,
		KeyRUnlock:       mockServer.KeyRUnlock,
		GetValue:         mockServer.GetValue,
		SetValue:         mockServer.SetValue,
		GetClock:         mockServer.GetClock,
		GetExpiry:        mockServer.GetExpiry,
		SetExpiry:        mockServer.SetExpiry,
		DeleteKey:        mockServer.DeleteKey,
	}
}

func Test_HandleSET(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		presetValues     map[string]KeyData
		expectedResponse interface{}
		expectedValue    interface{}
		expectedExpiry   time.Time
		expectedErr      error
	}{
		{
			name:             "1. Set normal string value",
			command:          []string{"SET", "SetKey1", "value1"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value1",
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{
			name:             "2. Set normal integer value",
			command:          []string{"SET", "SetKey2", "1245678910"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    1245678910,
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{
			name:             "3. Set normal float value",
			command:          []string{"SET", "SetKey3", "45782.11341"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    45782.11341,
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{
			name:             "4. Only set the value if the key does not exist",
			command:          []string{"SET", "SetKey4", "value4", "NX"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value4",
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{
			name:    "5. Throw error when value already exists with NX flag passed",
			command: []string{"SET", "SetKey5", "value5", "NX"},
			presetValues: map[string]KeyData{
				"SetKey5": {
					Value:    "preset-value5",
					ExpireAt: time.Time{},
				},
			},
			expectedResponse: nil,
			expectedValue:    "preset-value5",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("key SetKey5 already exists"),
		},
		{
			name:    "6. Set new key value when key exists with XX flag passed",
			command: []string{"SET", "SetKey6", "value6", "XX"},
			presetValues: map[string]KeyData{
				"SetKey6": {
					Value:    "preset-value6",
					ExpireAt: time.Time{},
				},
			},
			expectedResponse: "OK",
			expectedValue:    "value6",
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{
			name:             "7. Return error when setting non-existent key with XX flag",
			command:          []string{"SET", "SetKey7", "value7", "XX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("key SetKey7 does not exist"),
		},
		{
			name:             "8. Return error when NX flag is provided after XX flag",
			command:          []string{"SET", "SetKey8", "value8", "XX", "NX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify NX when XX is already specified"),
		},
		{
			name:             "9. Return error when XX flag is provided after NX flag",
			command:          []string{"SET", "SetKey9", "value9", "NX", "XX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify XX when NX is already specified"),
		},
		{
			name:             "10. Set expiry time on the key to 100 seconds from now",
			command:          []string{"SET", "SetKey10", "value10", "EX", "100"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value10",
			expectedExpiry:   mockClock.Now().Add(100 * time.Second),
			expectedErr:      nil,
		},
		{
			name:             "11. Return error when EX flag is passed without seconds value",
			command:          []string{"SET", "SetKey11", "value11", "EX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value required after EX"),
		},
		{
			name:             "12. Return error when EX flag is passed with invalid (non-integer) value",
			command:          []string{"SET", "SetKey12", "value12", "EX", "seconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value should be an integer"),
		},
		{
			name:             "13. Return error when trying to set expiry seconds when expiry is already set",
			command:          []string{"SET", "SetKey13", "value13", "PX", "100000", "EX", "100"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify EX when expiry time is already set"),
		},
		{
			name:             "14. Set expiry time on the key in unix milliseconds",
			command:          []string{"SET", "SetKey14", "value14", "PX", "4096"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value14",
			expectedExpiry:   mockClock.Now().Add(4096 * time.Millisecond),
			expectedErr:      nil,
		},
		{
			name:             "15. Return error when PX flag is passed without milliseconds value",
			command:          []string{"SET", "SetKey15", "value15", "PX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value required after PX"),
		},
		{
			name:             "16. Return error when PX flag is passed with invalid (non-integer) value",
			command:          []string{"SET", "SetKey16", "value16", "PX", "milliseconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value should be an integer"),
		},
		{
			name:             "17. Return error when trying to set expiry milliseconds when expiry is already provided",
			command:          []string{"SET", "SetKey17", "value17", "EX", "10", "PX", "1000000"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify PX when expiry time is already set"),
		},
		{
			name: "18. Set exact expiry time in seconds from unix epoch",
			command: []string{
				"SET", "SetKey18", "value18",
				"EXAT", fmt.Sprintf("%d", mockClock.Now().Add(200*time.Second).Unix()),
			},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value18",
			expectedExpiry:   mockClock.Now().Add(200 * time.Second),
			expectedErr:      nil,
		},
		{
			name: "19. Return error when trying to set exact seconds expiry time when expiry time is already provided",
			command: []string{
				"SET", "SetKey19", "value19",
				"EX", "10",
				"EXAT", fmt.Sprintf("%d", mockClock.Now().Add(200*time.Second).Unix()),
			},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify EXAT when expiry time is already set"),
		},
		{
			name:             "20. Return error when no seconds value is provided after EXAT flag",
			command:          []string{"SET", "SetKey20", "value20", "EXAT"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value required after EXAT"),
		},
		{
			name:             "21. Return error when invalid (non-integer) value is passed after EXAT flag",
			command:          []string{"SET", "SekKey21", "value21", "EXAT", "seconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value should be an integer"),
		},
		{
			name: "22. Set exact expiry time in milliseconds from unix epoch",
			command: []string{
				"SET", "SetKey22", "value22",
				"PXAT", fmt.Sprintf("%d", mockClock.Now().Add(4096*time.Millisecond).UnixMilli()),
			},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value22",
			expectedExpiry:   mockClock.Now().Add(4096 * time.Millisecond),
			expectedErr:      nil,
		},
		{
			name: "23. Return error when trying to set exact milliseconds expiry time when expiry time is already provided",
			command: []string{
				"SET", "SetKey23", "value23",
				"PX", "1000",
				"PXAT", fmt.Sprintf("%d", mockClock.Now().Add(4096*time.Millisecond).UnixMilli()),
			},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify PXAT when expiry time is already set"),
		},
		{
			name:             "24. Return error when no milliseconds value is provided after PXAT flag",
			command:          []string{"SET", "SetKey24", "value24", "PXAT"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value required after PXAT"),
		},
		{
			name:             "25. Return error when invalid (non-integer) value is passed after EXAT flag",
			command:          []string{"SET", "SetKey25", "value25", "PXAT", "unix-milliseconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value should be an integer"),
		},
		{
			name:    "26. Get the previous value when GET flag is passed",
			command: []string{"SET", "SetKey26", "value26", "GET", "EX", "1000"},
			presetValues: map[string]KeyData{
				"SetKey26": {
					Value:    "previous-value",
					ExpireAt: time.Time{},
				},
			},
			expectedResponse: "previous-value",
			expectedValue:    "value26",
			expectedExpiry:   mockClock.Now().Add(1000 * time.Second),
			expectedErr:      nil,
		},
		{
			name:             "27. Return nil when GET value is passed and no previous value exists",
			command:          []string{"SET", "SetKey27", "value27", "GET", "EX", "1000"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "value27",
			expectedExpiry:   mockClock.Now().Add(1000 * time.Second),
			expectedErr:      nil,
		},
		{
			name:             "28. Throw error when unknown optional flag is passed to SET command.",
			command:          []string{"SET", "SetKey28", "value28", "UNKNOWN-OPTION"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("unknown option UNKNOWN-OPTION for set command"),
		},
		{
			name:             "29. Command too short",
			command:          []string{"SET"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedErr:      errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "30. Command too long",
			command:          []string{"SET", "SetKey30", "value1", "value2", "value3", "value4", "value5", "value6"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedErr:      errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SET, %d", i+1))

			if test.presetValues != nil {
				for k, v := range test.presetValues {
					if _, err := mockServer.CreateKeyAndLock(ctx, k); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, k, v.Value); err != nil {
						t.Error(err)
					}
					mockServer.SetExpiry(ctx, k, v.ExpireAt, false)
					mockServer.KeyUnlock(ctx, k)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedErr != nil {
				if err == nil {
					t.Errorf("expected error \"%s\", got nil", test.expectedErr.Error())
				}
				if test.expectedErr.Error() != err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedErr.Error(), err.Error())
				}
				return
			}
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}

			switch test.expectedResponse.(type) {
			case string:
				if test.expectedResponse != rv.String() {
					t.Errorf("expected response \"%s\", got \"%s\"", test.expectedResponse, rv.String())
				}
			case nil:
				if !rv.IsNull() {
					t.Errorf("expcted nil response, got %+v", rv)
				}
			default:
				t.Error("test expected result should be nil or string")
			}

			// Compare expected value and expected time
			key := test.command[1]
			var value interface{}
			var expireAt time.Time

			if _, err = mockServer.KeyLock(ctx, key); err != nil {
				t.Error(err)
			}
			value = mockServer.GetValue(ctx, key)
			expireAt = mockServer.GetExpiry(ctx, key)
			mockServer.KeyUnlock(ctx, key)

			if value != test.expectedValue {
				t.Errorf("expected value %+v, got %+v", test.expectedValue, value)
			}
			if test.expectedExpiry.Unix() != expireAt.Unix() {
				t.Errorf("expected expiry time %d, got %d, cmd: %+v", test.expectedExpiry.Unix(), expireAt.Unix(), test.command)
			}
		})
	}
}

func Test_HandleMSET(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		expectedResponse string
		expectedValues   map[string]interface{}
		expectedErr      error
	}{
		{
			name:             "1. Set multiple key value pairs",
			command:          []string{"MSET", "MsetKey1", "value1", "MsetKey2", "10", "MsetKey3", "3.142"},
			expectedResponse: "OK",
			expectedValues:   map[string]interface{}{"MsetKey1": "value1", "MsetKey2": 10, "MsetKey3": 3.142},
			expectedErr:      nil,
		},
		{
			name:             "2. Return error when keys and values are not even",
			command:          []string{"MSET", "MsetKey1", "value1", "MsetKey2", "10", "MsetKey3"},
			expectedResponse: "",
			expectedValues:   make(map[string]interface{}),
			expectedErr:      errors.New("each key must be paired with a value"),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("MSET, %d", i))

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedErr != nil {
				if err.Error() != test.expectedErr.Error() {
					t.Errorf("expected error %s, got %s", test.expectedErr.Error(), err.Error())
				}
				return
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
				if _, err = mockServer.KeyRLock(ctx, key); err != nil {
					t.Error(err)
				}
				switch expectedValue.(type) {
				default:
					t.Error("unexpected type for expectedValue")
				case int:
					ev, _ := expectedValue.(int)
					value, ok := mockServer.GetValue(ctx, key).(int)
					if !ok {
						t.Errorf("expected integer type for key %s, got another type", key)
					}
					if value != ev {
						t.Errorf("expected value %d for key %s, got %d", ev, key, value)
					}
				case float64:
					ev, _ := expectedValue.(float64)
					value, ok := mockServer.GetValue(ctx, key).(float64)
					if !ok {
						t.Errorf("expected float type for key %s, got another type", key)
					}
					if value != ev {
						t.Errorf("expected value %f for key %s, got %f", ev, key, value)
					}
				case string:
					ev, _ := expectedValue.(string)
					value, ok := mockServer.GetValue(ctx, key).(string)
					if !ok {
						t.Errorf("expected string type for key %s, got another type", key)
					}
					if value != ev {
						t.Errorf("expected value %s for key %s, got %s", ev, key, value)
					}
				}
				mockServer.KeyRUnlock(ctx, key)
			}
		})
	}
}

func Test_HandleGET(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{
			name:  "1. String",
			key:   "GetKey1",
			value: "value1",
		},
		{
			name:  "2. Integer",
			key:   "GetKey2",
			value: "10",
		},
		{
			name:  "3. Float",
			key:   "GetKey3",
			value: "3.142",
		},
	}
	// Test successful GET command
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("GET, %d", i))
			func(key, value string) {

				_, err := mockServer.CreateKeyAndLock(ctx, key)
				if err != nil {
					t.Error(err)
				}
				if err = mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)

				handler := getHandler("GET")
				if handler == nil {
					t.Error("no handler found for command GET")
					return
				}

				res, err := handler(getHandlerFuncParams(ctx, []string{"GET", key}, nil))

				if err != nil {
					t.Error(err)
				}
				if !bytes.Equal(res, []byte(fmt.Sprintf("+%v\r\n", value))) {
					t.Errorf("expected %s, got: %s", fmt.Sprintf("+%v\r\n", value), string(res))
				}
			}(test.key, test.value)
		})
	}

	// Test get non-existent key
	res, err := getHandler("GET")(getHandlerFuncParams(context.Background(), []string{"GET", "test4"}, nil))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(res, []byte("$-1\r\n")) {
		t.Errorf("expected %+v, got: %+v", "+nil\r\n", res)
	}

	errorTests := []struct {
		name     string
		command  []string
		expected string
	}{
		{
			name:     "1. Return error when no GET key is passed",
			command:  []string{"GET"},
			expected: constants.WrongArgsResponse,
		},
		{
			name:     "2. Return error when too many GET keys are passed",
			command:  []string{"GET", "GetKey1", "test"},
			expected: constants.WrongArgsResponse,
		},
	}
	for _, test := range errorTests {
		t.Run(test.name, func(t *testing.T) {
			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}
			res, err = handler(getHandlerFuncParams(context.Background(), test.command, nil))
			if res != nil {
				t.Errorf("expected nil response, got: %+v", res)
			}
			if err.Error() != test.expected {
				t.Errorf("expected error '%s', got: %s", test.expected, err.Error())
			}
		})
	}
}

func Test_HandleMGET(t *testing.T) {
	tests := []struct {
		name          string
		presetKeys    []string
		presetValues  []string
		command       []string
		expected      []interface{}
		expectedError error
	}{
		{
			name:          "1. MGET multiple existing values",
			presetKeys:    []string{"MgetKey1", "MgetKey2", "MgetKey3", "MgetKey4"},
			presetValues:  []string{"value1", "value2", "value3", "value4"},
			command:       []string{"MGET", "MgetKey1", "MgetKey4", "MgetKey2", "MgetKey3", "MgetKey1"},
			expected:      []interface{}{"value1", "value4", "value2", "value3", "value1"},
			expectedError: nil,
		},
		{
			name:          "2. MGET multiple values with nil values spliced in",
			presetKeys:    []string{"MgetKey5", "MgetKey6", "MgetKey7"},
			presetValues:  []string{"value5", "value6", "value7"},
			command:       []string{"MGET", "MgetKey5", "MgetKey6", "non-existent", "non-existent", "MgetKey7", "non-existent"},
			expected:      []interface{}{"value5", "value6", nil, nil, "value7", nil},
			expectedError: nil,
		},
		{
			name:          "3. Return error when MGET is invoked with no keys",
			presetKeys:    []string{"MgetKey5"},
			presetValues:  []string{"value5"},
			command:       []string{"MGET"},
			expected:      nil,
			expectedError: errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("MGET, %d", i))
			// Set up the values
			for i, key := range test.presetKeys {
				_, err := mockServer.CreateKeyAndLock(ctx, key)
				if err != nil {
					t.Error(err)
				}
				if err = mockServer.SetValue(ctx, key, test.presetValues[i]); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
			// Test the command and its results
			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedError != nil {
				// If we expect and error, branch out and check error
				if err.Error() != test.expectedError.Error() {
					t.Errorf("expected error %+v, got: %+v", test.expectedError, err)
				}
				return
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
		})
	}
}

func Test_HandleDEL(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		presetValues     map[string]KeyData
		expectedResponse int
		expectToExist    map[string]bool
		expectedErr      error
	}{
		{
			name:    "1. Delete multiple keys",
			command: []string{"DEL", "DelKey1", "DelKey2", "DelKey3", "DelKey4", "DelKey5"},
			presetValues: map[string]KeyData{
				"DelKey1": {Value: "value1", ExpireAt: time.Time{}},
				"DelKey2": {Value: "value2", ExpireAt: time.Time{}},
				"DelKey3": {Value: "value3", ExpireAt: time.Time{}},
				"DelKey4": {Value: "value4", ExpireAt: time.Time{}},
			},
			expectedResponse: 4,
			expectToExist: map[string]bool{
				"DelKey1": false,
				"DelKey2": false,
				"DelKey3": false,
				"DelKey4": false,
				"DelKey5": false,
			},
			expectedErr: nil,
		},
		{
			name:             "2. Return error when DEL is called with no keys",
			command:          []string{"DEL"},
			presetValues:     nil,
			expectedResponse: 0,
			expectToExist:    nil,
			expectedErr:      errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("DEL, %d", i))

			if test.presetValues != nil {
				for k, v := range test.presetValues {
					if _, err := mockServer.CreateKeyAndLock(ctx, k); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, k, v.Value); err != nil {
						t.Error(err)
					}
					mockServer.SetExpiry(ctx, k, v.ExpireAt, false)
					mockServer.KeyUnlock(ctx, k)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))
			if test.expectedErr != nil {
				if err == nil {
					t.Errorf("exected error \"%s\", got nil", test.expectedErr.Error())
				}
				if test.expectedErr.Error() != err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedErr.Error(), err.Error())
				}
				return
			}
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}

			if rv.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
			}

			for k, expected := range test.expectToExist {
				exists := mockServer.KeyExists(ctx, k)
				if exists != expected {
					t.Errorf("expected exists status to be %+v, got %+v", expected, exists)
				}
			}
		})
	}
}

func Test_HandlePERSIST(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		presetValues     map[string]KeyData
		expectedResponse int
		expectedValues   map[string]KeyData
		expectedError    error
	}{
		{
			name:    "1. Successfully persist a volatile key",
			command: []string{"PERSIST", "PersistKey1"},
			presetValues: map[string]KeyData{
				"PersistKey1": {Value: "value1", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"PersistKey1": {Value: "value1", ExpireAt: time.Time{}},
			},
			expectedError: nil,
		},
		{
			name:             "2. Return 0 when trying to persist a non-existent key",
			command:          []string{"PERSIST", "PersistKey2"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    nil,
		},
		{
			name:    "3. Return 0 when trying to persist a non-volatile key",
			command: []string{"PERSIST", "PersistKey3"},
			presetValues: map[string]KeyData{
				"PersistKey3": {Value: "value3", ExpireAt: time.Time{}},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"PersistKey3": {Value: "value3", ExpireAt: time.Time{}},
			},
			expectedError: nil,
		},
		{
			name:             "4. Command too short",
			command:          []string{"PERSIST"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "5. Command too long",
			command:          []string{"PERSIST", "PersistKey5", "key6"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("PERSIST, %d", i))

			if test.presetValues != nil {
				for k, v := range test.presetValues {
					if _, err := mockServer.CreateKeyAndLock(ctx, k); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, k, v.Value); err != nil {
						t.Error(err)
					}
					mockServer.SetExpiry(ctx, k, v.ExpireAt, false)
					mockServer.KeyUnlock(ctx, k)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))

			if test.expectedError != nil {
				if err == nil {
					t.Errorf("expected error \"%s\", got nil", test.expectedError.Error())
				}
				if test.expectedError.Error() != err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
			}

			if test.expectedValues == nil {
				return
			}

			for k, expected := range test.expectedValues {
				if _, err = mockServer.KeyLock(ctx, k); err != nil {
					t.Error(err)
				}
				value := mockServer.GetValue(ctx, k)
				expiry := mockServer.GetExpiry(ctx, k)
				if value != expected.Value {
					t.Errorf("expected value %+v, got %+v", expected.Value, value)
				}
				if expiry.UnixMilli() != expected.ExpireAt.UnixMilli() {
					t.Errorf("expected exiry %d, got %d", expected.ExpireAt.UnixMilli(), expiry.UnixMilli())
				}
				mockServer.KeyUnlock(ctx, k)
			}
		})
	}
}

func Test_HandleEXPIRETIME(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		presetValues     map[string]KeyData
		expectedResponse int
		expectedError    error
	}{
		{
			name:    "1. Return expire time in seconds",
			command: []string{"EXPIRETIME", "ExpireTimeKey1"},
			presetValues: map[string]KeyData{
				"ExpireTimeKey1": {Value: "value1", ExpireAt: mockClock.Now().Add(100 * time.Second)},
			},
			expectedResponse: int(mockClock.Now().Add(100 * time.Second).Unix()),
			expectedError:    nil,
		},
		{
			name:    "2. Return expire time in milliseconds",
			command: []string{"PEXPIRETIME", "ExpireTimeKey2"},
			presetValues: map[string]KeyData{
				"ExpireTimeKey2": {Value: "value2", ExpireAt: mockClock.Now().Add(4096 * time.Millisecond)},
			},
			expectedResponse: int(mockClock.Now().Add(4096 * time.Millisecond).UnixMilli()),
			expectedError:    nil,
		},
		{
			name:    "3. If the key is non-volatile, return -1",
			command: []string{"PEXPIRETIME", "ExpireTimeKey3"},
			presetValues: map[string]KeyData{
				"ExpireTimeKey3": {Value: "value3", ExpireAt: time.Time{}},
			},
			expectedResponse: -1,
			expectedError:    nil,
		},
		{
			name:             "4. If the key is non-existent return -2",
			command:          []string{"PEXPIRETIME", "ExpireTimeKey4"},
			presetValues:     nil,
			expectedResponse: -2,
			expectedError:    nil,
		},
		{
			name:             "5. Command too short",
			command:          []string{"PEXPIRETIME"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "6. Command too long",
			command:          []string{"PEXPIRETIME", "ExpireTimeKey5", "ExpireTimeKey6"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("EXPIRETIME/PEXPIRETIME, %d", i))

			if test.presetValues != nil {
				for k, v := range test.presetValues {
					if _, err := mockServer.CreateKeyAndLock(ctx, k); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, k, v.Value); err != nil {
						t.Error(err)
					}
					mockServer.SetExpiry(ctx, k, v.ExpireAt, false)
					mockServer.KeyUnlock(ctx, k)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))

			if test.expectedError != nil {
				if err == nil {
					t.Errorf("expected error \"%s\", got nil", test.expectedError.Error())
				}
				if test.expectedError.Error() != err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
			}
		})
	}
}

func Test_HandleTTL(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		presetValues     map[string]KeyData
		expectedResponse int
		expectedError    error
	}{
		{
			name:    "1. Return TTL time in seconds",
			command: []string{"TTL", "TTLKey1"},
			presetValues: map[string]KeyData{
				"TTLKey1": {Value: "value1", ExpireAt: mockClock.Now().Add(100 * time.Second)},
			},
			expectedResponse: 100,
			expectedError:    nil,
		},
		{
			name:    "2. Return TTL time in milliseconds",
			command: []string{"PTTL", "TTLKey2"},
			presetValues: map[string]KeyData{
				"TTLKey2": {Value: "value2", ExpireAt: mockClock.Now().Add(4096 * time.Millisecond)},
			},
			expectedResponse: 4096,
			expectedError:    nil,
		},
		{
			name:    "3. If the key is non-volatile, return -1",
			command: []string{"TTL", "TTLKey3"},
			presetValues: map[string]KeyData{
				"TTLKey3": {Value: "value3", ExpireAt: time.Time{}},
			},
			expectedResponse: -1,
			expectedError:    nil,
		},
		{
			name:             "4. If the key is non-existent return -2",
			command:          []string{"TTL", "TTLKey4"},
			presetValues:     nil,
			expectedResponse: -2,
			expectedError:    nil,
		},
		{
			name:             "5. Command too short",
			command:          []string{"TTL"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "6. Command too long",
			command:          []string{"TTL", "TTLKey5", "TTLKey6"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("TTL/PTTL, %d", i))

			if test.presetValues != nil {
				for k, v := range test.presetValues {
					if _, err := mockServer.CreateKeyAndLock(ctx, k); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, k, v.Value); err != nil {
						t.Error(err)
					}
					mockServer.SetExpiry(ctx, k, v.ExpireAt, false)
					mockServer.KeyUnlock(ctx, k)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))

			if test.expectedError != nil {
				if err == nil {
					t.Errorf("expected error \"%s\", got nil", test.expectedError.Error())
				}
				if test.expectedError.Error() != err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
			}
		})
	}
}

func Test_HandleEXPIRE(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		presetValues     map[string]KeyData
		expectedResponse int
		expectedValues   map[string]KeyData
		expectedError    error
	}{
		{
			name:    "1. Set new expire by seconds",
			command: []string{"EXPIRE", "ExpireKey1", "100"},
			presetValues: map[string]KeyData{
				"ExpireKey1": {Value: "value1", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireKey1": {Value: "value1", ExpireAt: mockClock.Now().Add(100 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "2. Set new expire by milliseconds",
			command: []string{"PEXPIRE", "ExpireKey2", "1000"},
			presetValues: map[string]KeyData{
				"ExpireKey2": {Value: "value2", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireKey2": {Value: "value2", ExpireAt: mockClock.Now().Add(1000 * time.Millisecond)},
			},
			expectedError: nil,
		},
		{
			name:    "3. Set new expire only when key does not have an expiry time with NX flag",
			command: []string{"EXPIRE", "ExpireKey3", "1000", "NX"},
			presetValues: map[string]KeyData{
				"ExpireKey3": {Value: "value3", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireKey3": {Value: "value3", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "4. Return 0, when NX flag is provided and key already has an expiry time",
			command: []string{"EXPIRE", "ExpireKey4", "1000", "NX"},
			presetValues: map[string]KeyData{
				"ExpireKey4": {Value: "value4", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireKey4": {Value: "value4", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "5. Set new expire time from now key only when the key already has an expiry time with XX flag",
			command: []string{"EXPIRE", "ExpireKey5", "1000", "XX"},
			presetValues: map[string]KeyData{
				"ExpireKey5": {Value: "value5", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireKey5": {Value: "value5", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "6. Return 0 when key does not have an expiry and the XX flag is provided",
			command: []string{"EXPIRE", "ExpireKey6", "1000", "XX"},
			presetValues: map[string]KeyData{
				"ExpireKey6": {Value: "value6", ExpireAt: time.Time{}},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireKey6": {Value: "value6", ExpireAt: time.Time{}},
			},
			expectedError: nil,
		},
		{
			name:    "7. Set expiry time when the provided time is after the current expiry time when GT flag is provided",
			command: []string{"EXPIRE", "ExpireKey7", "1000", "GT"},
			presetValues: map[string]KeyData{
				"ExpireKey7": {Value: "value7", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireKey7": {Value: "value7", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "8. Return 0 when GT flag is passed and current expiry time is greater than provided time",
			command: []string{"EXPIRE", "ExpireKey8", "1000", "GT"},
			presetValues: map[string]KeyData{
				"ExpireKey8": {Value: "value8", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireKey8": {Value: "value8", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "9. Return 0 when GT flag is passed and key does not have an expiry time",
			command: []string{"EXPIRE", "ExpireKey9", "1000", "GT"},
			presetValues: map[string]KeyData{
				"ExpireKey9": {Value: "value9", ExpireAt: time.Time{}},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireKey9": {Value: "value9", ExpireAt: time.Time{}},
			},
			expectedError: nil,
		},
		{
			name:    "10. Set expiry time when the provided time is before the current expiry time when LT flag is provided",
			command: []string{"EXPIRE", "ExpireKey10", "1000", "LT"},
			presetValues: map[string]KeyData{
				"ExpireKey10": {Value: "value10", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireKey10": {Value: "value10", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "11. Return 0 when LT flag is passed and current expiry time is less than provided time",
			command: []string{"EXPIRE", "ExpireKey11", "5000", "LT"},
			presetValues: map[string]KeyData{
				"ExpireKey11": {Value: "value11", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireKey11": {Value: "value11", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "12. Return 0 when LT flag is passed and key does not have an expiry time",
			command: []string{"EXPIRE", "ExpireKey12", "1000", "LT"},
			presetValues: map[string]KeyData{
				"ExpireKey12": {Value: "value12", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireKey12": {Value: "value12", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name:    "13. Return error when unknown flag is passed",
			command: []string{"EXPIRE", "ExpireKey13", "1000", "UNKNOWN"},
			presetValues: map[string]KeyData{
				"ExpireKey13": {Value: "value13", ExpireAt: time.Time{}},
			},
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New("unknown option UNKNOWN"),
		},
		{
			name:             "14. Return error when expire time is not a valid integer",
			command:          []string{"EXPIRE", "ExpireKey14", "expire"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New("expire time must be integer"),
		},
		{
			name:             "15. Command too short",
			command:          []string{"EXPIRE"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "16. Command too long",
			command:          []string{"EXPIRE", "ExpireKey16", "10", "NX", "GT"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("PERSIST, %d", i))

			if test.presetValues != nil {
				for k, v := range test.presetValues {
					if _, err := mockServer.CreateKeyAndLock(ctx, k); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, k, v.Value); err != nil {
						t.Error(err)
					}
					mockServer.SetExpiry(ctx, k, v.ExpireAt, false)
					mockServer.KeyUnlock(ctx, k)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))

			if test.expectedError != nil {
				if err == nil {
					t.Errorf("expected error \"%s\", got nil", test.expectedError.Error())
				}
				if test.expectedError.Error() != err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
			}

			if test.expectedValues == nil {
				return
			}

			for k, expected := range test.expectedValues {
				if _, err = mockServer.KeyLock(ctx, k); err != nil {
					t.Error(err)
				}
				value := mockServer.GetValue(ctx, k)
				expiry := mockServer.GetExpiry(ctx, k)
				if value != expected.Value {
					t.Errorf("expected value %+v, got %+v", expected.Value, value)
				}
				if expiry.UnixMilli() != expected.ExpireAt.UnixMilli() {
					t.Errorf("expected expiry %d, got %d", expected.ExpireAt.UnixMilli(), expiry.UnixMilli())
				}
				mockServer.KeyUnlock(ctx, k)
			}
		})
	}
}

func Test_HandleEXPIREAT(t *testing.T) {
	tests := []struct {
		name             string
		command          []string
		presetValues     map[string]KeyData
		expectedResponse int
		expectedValues   map[string]KeyData
		expectedError    error
	}{
		{
			name:    "1. Set new expire by unix seconds",
			command: []string{"EXPIREAT", "ExpireAtKey1", fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix())},
			presetValues: map[string]KeyData{
				"ExpireAtKey1": {Value: "value1", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireAtKey1": {Value: "value1", ExpireAt: time.Unix(mockClock.Now().Add(1000*time.Second).Unix(), 0)},
			},
			expectedError: nil,
		},
		{
			name:    "2. Set new expire by milliseconds",
			command: []string{"PEXPIREAT", "ExpireAtKey2", fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).UnixMilli())},
			presetValues: map[string]KeyData{
				"ExpireAtKey2": {Value: "value2", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireAtKey2": {Value: "value2", ExpireAt: time.UnixMilli(mockClock.Now().Add(1000 * time.Second).UnixMilli())},
			},
			expectedError: nil,
		},
		{
			name:    "3. Set new expire only when key does not have an expiry time with NX flag",
			command: []string{"EXPIREAT", "ExpireAtKey3", fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "NX"},
			presetValues: map[string]KeyData{
				"ExpireAtKey3": {Value: "value3", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireAtKey3": {Value: "value3", ExpireAt: time.Unix(mockClock.Now().Add(1000*time.Second).Unix(), 0)},
			},
			expectedError: nil,
		},
		{
			name:    "4. Return 0, when NX flag is provided and key already has an expiry time",
			command: []string{"EXPIREAT", "ExpireAtKey4", fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "NX"},
			presetValues: map[string]KeyData{
				"ExpireAtKey4": {Value: "value4", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireAtKey4": {Value: "value4", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name: "5. Set new expire time from now key only when the key already has an expiry time with XX flag",
			command: []string{
				"EXPIREAT", "ExpireAtKey5",
				fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "XX",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey5": {Value: "value5", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireAtKey5": {Value: "value5", ExpireAt: time.Unix(mockClock.Now().Add(1000*time.Second).Unix(), 0)},
			},
			expectedError: nil,
		},
		{
			name: "6. Return 0 when key does not have an expiry and the XX flag is provided",
			command: []string{
				"EXPIREAT", "ExpireAtKey6",
				fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "XX",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey6": {Value: "value6", ExpireAt: time.Time{}},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireAtKey6": {Value: "value6", ExpireAt: time.Time{}},
			},
			expectedError: nil,
		},
		{
			name: "7. Set expiry time when the provided time is after the current expiry time when GT flag is provided",
			command: []string{
				"EXPIREAT", "ExpireAtKey7",
				fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "GT",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey7": {Value: "value7", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireAtKey7": {Value: "value7", ExpireAt: time.Unix(mockClock.Now().Add(1000*time.Second).Unix(), 0)},
			},
			expectedError: nil,
		},
		{
			name: "8. Return 0 when GT flag is passed and current expiry time is greater than provided time",
			command: []string{
				"EXPIREAT", "ExpireAtKey8",
				fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "GT",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey8": {Value: "value8", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireAtKey8": {Value: "value8", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name: "9. Return 0 when GT flag is passed and key does not have an expiry time",
			command: []string{
				"EXPIREAT", "ExpireAtKey9",
				fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "GT",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey9": {Value: "value9", ExpireAt: time.Time{}},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireAtKey9": {Value: "value9", ExpireAt: time.Time{}},
			},
			expectedError: nil,
		},
		{
			name: "10. Set expiry time when the provided time is before the current expiry time when LT flag is provided",
			command: []string{
				"EXPIREAT", "ExpireAtKey10",
				fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "LT",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey10": {Value: "value10", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireAtKey10": {Value: "value10", ExpireAt: time.Unix(mockClock.Now().Add(1000*time.Second).Unix(), 0)},
			},
			expectedError: nil,
		},
		{
			name: "11. Return 0 when LT flag is passed and current expiry time is less than provided time",
			command: []string{
				"EXPIREAT", "ExpireAtKey11",
				fmt.Sprintf("%d", mockClock.Now().Add(3000*time.Second).Unix()), "LT",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey11": {Value: "value11", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedResponse: 0,
			expectedValues: map[string]KeyData{
				"ExpireAtKey11": {Value: "value11", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			expectedError: nil,
		},
		{
			name: "12. Return 0 when LT flag is passed and key does not have an expiry time",
			command: []string{
				"EXPIREAT", "ExpireAtKey12",
				fmt.Sprintf("%d", mockClock.Now().Add(1000*time.Second).Unix()), "LT",
			},
			presetValues: map[string]KeyData{
				"ExpireAtKey12": {Value: "value12", ExpireAt: time.Time{}},
			},
			expectedResponse: 1,
			expectedValues: map[string]KeyData{
				"ExpireAtKey12": {Value: "value12", ExpireAt: time.Unix(mockClock.Now().Add(1000*time.Second).Unix(), 0)},
			},
			expectedError: nil,
		},
		{
			name:    "13. Return error when unknown flag is passed",
			command: []string{"EXPIREAT", "ExpireAtKey13", "1000", "UNKNOWN"},
			presetValues: map[string]KeyData{
				"ExpireAtKey13": {Value: "value13", ExpireAt: time.Time{}},
			},
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New("unknown option UNKNOWN"),
		},
		{
			name:             "14. Return error when expire time is not a valid integer",
			command:          []string{"EXPIREAT", "ExpireAtKey14", "expire"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New("expire time must be integer"),
		},
		{
			name:             "15. Command too short",
			command:          []string{"EXPIREAT"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
		{
			name:             "16. Command too long",
			command:          []string{"EXPIREAT", "ExpireAtKey16", "10", "NX", "GT"},
			presetValues:     nil,
			expectedResponse: 0,
			expectedValues:   nil,
			expectedError:    errors.New(constants.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("PERSIST, %d", i))

			if test.presetValues != nil {
				for k, v := range test.presetValues {
					if _, err := mockServer.CreateKeyAndLock(ctx, k); err != nil {
						t.Error(err)
					}
					if err := mockServer.SetValue(ctx, k, v.Value); err != nil {
						t.Error(err)
					}
					mockServer.SetExpiry(ctx, k, v.ExpireAt, false)
					mockServer.KeyUnlock(ctx, k)
				}
			}

			handler := getHandler(test.command[0])
			if handler == nil {
				t.Errorf("no handler found for command %s", test.command[0])
				return
			}

			res, err := handler(getHandlerFuncParams(ctx, test.command, nil))

			if test.expectedError != nil {
				if err == nil {
					t.Errorf("expected error \"%s\", got nil", test.expectedError.Error())
				}
				if test.expectedError.Error() != err.Error() {
					t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
				}
				return
			}
			if err != nil {
				t.Error(err)
			}

			rd := resp.NewReader(bytes.NewReader(res))
			rv, _, err := rd.ReadValue()
			if err != nil {
				t.Error(err)
			}
			if rv.Integer() != test.expectedResponse {
				t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
			}

			if test.expectedValues == nil {
				return
			}

			for k, expected := range test.expectedValues {
				if _, err = mockServer.KeyLock(ctx, k); err != nil {
					t.Error(err)
				}
				value := mockServer.GetValue(ctx, k)
				expiry := mockServer.GetExpiry(ctx, k)
				if value != expected.Value {
					t.Errorf("expected value %+v, got %+v", expected.Value, value)
				}
				if expiry.UnixMilli() != expected.ExpireAt.UnixMilli() {
					t.Errorf("expected expiry %d, got %d", expected.ExpireAt.UnixMilli(), expiry.UnixMilli())
				}
				mockServer.KeyUnlock(ctx, k)
			}
		})
	}
}
