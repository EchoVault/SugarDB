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
	"time"
)

func Test_HandleSET(t *testing.T) {
	mockServer := server.NewServer(server.Opts{
		Config: utils.Config{
			EvictionPolicy: utils.NoEviction,
		},
	})

	tests := []struct {
		command          []string
		presetValues     map[string]utils.KeyData
		expectedResponse interface{}
		expectedValue    interface{}
		expectedExpiry   time.Time
		expectedErr      error
	}{
		{ // 1. Set normal string value
			command:          []string{"SET", "key1", "value1"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value1",
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{ // 2. Set normal integer value
			command:          []string{"SET", "key2", "1245678910"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    1245678910,
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{ // 3. Set normal float value
			command:          []string{"SET", "key3", "45782.11341"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    45782.11341,
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{ // 4. Only set the value if the key does not exist
			command:          []string{"SET", "key4", "value4", "NX"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value4",
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{ // 5. Throw error when value already exists with NX flag passed
			command: []string{"SET", "key5", "value5", "NX"},
			presetValues: map[string]utils.KeyData{
				"key5": {
					Value:    "preset-value5",
					ExpireAt: time.Time{},
				},
			},
			expectedResponse: nil,
			expectedValue:    "preset-value5",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("key key5 already exists"),
		},
		{ // 6. Set new key value when key exists with XX flag passed
			command: []string{"SET", "key6", "value6", "XX"},
			presetValues: map[string]utils.KeyData{
				"key6": {
					Value:    "preset-value6",
					ExpireAt: time.Time{},
				},
			},
			expectedResponse: "OK",
			expectedValue:    "value6",
			expectedExpiry:   time.Time{},
			expectedErr:      nil,
		},
		{ // 7. Return error when setting non-existent key with XX flag
			command:          []string{"SET", "key7", "value7", "XX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("key key7 does not exist"),
		},
		{ // 8. Return error when NX flag is provided after XX flag
			command:          []string{"SET", "key8", "value8", "XX", "NX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify NX when XX is already specified"),
		},
		{ // 9. Return error when XX flag is provided after NX flag
			command:          []string{"SET", "key9", "value9", "NX", "XX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify XX when NX is already specified"),
		},
		{ // 10. Set expiry time on the key to 100 seconds from now
			command:          []string{"SET", "key10", "value10", "EX", "100"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value10",
			expectedExpiry:   timeNow().Add(100 * time.Second),
			expectedErr:      nil,
		},
		{ // 11. Return error when EX flag is passed without seconds value
			command:          []string{"SET", "key11", "value11", "EX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value required after EX"),
		},
		{ // 12. Return error when EX flag is passed with invalid (non-integer) value
			command:          []string{"SET", "key12", "value12", "EX", "seconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value should be an integer"),
		},
		{ // 13. Return error when trying to set expiry seconds when expiry is already set
			command:          []string{"SET", "key13", "value13", "PX", "100000", "EX", "100"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify EX when expiry time is already set"),
		},
		{ // 14. Set expiry time on the key in unix milliseconds
			command:          []string{"SET", "key14", "value14", "PX", "4096"},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value14",
			expectedExpiry:   timeNow().Add(4096 * time.Millisecond),
			expectedErr:      nil,
		},
		{ // 15. Return error when PX flag is passed without milliseconds value
			command:          []string{"SET", "key15", "value15", "PX"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value required after PX"),
		},
		{ // 16. Return error when PX flag is passed with invalid (non-integer) value
			command:          []string{"SET", "key16", "value16", "PX", "milliseconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value should be an integer"),
		},
		{ // 17. Return error when trying to set expiry milliseconds when expiry is already provided
			command:          []string{"SET", "key17", "value17", "EX", "10", "PX", "1000000"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify PX when expiry time is already set"),
		},
		{ // 18. Set exact expiry time in seconds from unix epoch
			command: []string{
				"SET", "key18", "value18",
				"EXAT", fmt.Sprintf("%d", timeNow().Add(200*time.Second).Unix()),
			},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value18",
			expectedExpiry:   timeNow().Add(200 * time.Second),
			expectedErr:      nil,
		},
		{ // 19. Return error when trying to set exact seconds expiry time when expiry time is already provided
			command: []string{
				"SET", "key19", "value19",
				"EX", "10",
				"EXAT", fmt.Sprintf("%d", timeNow().Add(200*time.Second).Unix()),
			},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify EXAT when expiry time is already set"),
		},
		{ // 20. Return error when no seconds value is provided after EXAT flag
			command:          []string{"SET", "key20", "value20", "EXAT"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value required after EXAT"),
		},
		{ // 21. Return error when invalid (non-integer) value is passed after EXAT flag
			command:          []string{"SET", "key21", "value21", "EXAT", "seconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("seconds value should be an integer"),
		},
		{ // 22. Set exact expiry time in milliseconds from unix epoch
			command: []string{
				"SET", "key22", "value22",
				"PXAT", fmt.Sprintf("%d", timeNow().Add(4096*time.Millisecond).UnixMilli()),
			},
			presetValues:     nil,
			expectedResponse: "OK",
			expectedValue:    "value22",
			expectedExpiry:   timeNow().Add(4096 * time.Millisecond),
			expectedErr:      nil,
		},
		{ // 23. Return error when trying to set exact milliseconds expiry time when expiry time is already provided
			command: []string{
				"SET", "key23", "value23",
				"PX", "1000",
				"PXAT", fmt.Sprintf("%d", timeNow().Add(4096*time.Millisecond).UnixMilli()),
			},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("cannot specify PXAT when expiry time is already set"),
		},
		{ // 24. Return error when no milliseconds value is provided after PXAT flag
			command:          []string{"SET", "key24", "value24", "PXAT"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value required after PXAT"),
		},
		{ // 25. Return error when invalid (non-integer) value is passed after EXAT flag
			command:          []string{"SET", "key25", "value25", "PXAT", "unix-milliseconds"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "",
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("milliseconds value should be an integer"),
		},
		{ // 26. Get the previous value when GET flag is passed
			command: []string{"SET", "key26", "value26", "GET", "EX", "1000"},
			presetValues: map[string]utils.KeyData{
				"key26": {
					Value:    "previous-value",
					ExpireAt: time.Time{},
				},
			},
			expectedResponse: "previous-value",
			expectedValue:    "value26",
			expectedExpiry:   timeNow().Add(1000 * time.Second),
			expectedErr:      nil,
		},
		{ // 27. Return nil when GET value is passed and no previous value exists
			command:          []string{"SET", "key27", "value27", "GET", "EX", "1000"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    "value27",
			expectedExpiry:   timeNow().Add(1000 * time.Second),
			expectedErr:      nil,
		},
		{ // 28. Throw error when unknown optional flag is passed to SET command.
			command:          []string{"SET", "key28", "value28", "UNKNOWN-OPTION"},
			presetValues:     nil,
			expectedResponse: nil,
			expectedValue:    nil,
			expectedExpiry:   time.Time{},
			expectedErr:      errors.New("unknown option UNKNOWN-OPTION for set command"),
		},
		{ // 29. Command too short
			command:          []string{"SET"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedErr:      errors.New(utils.WrongArgsResponse),
		},
		{ // 30. Command too long
			command:          []string{"SET", "key", "value1", "value2", "value3", "value4", "value5", "value6"},
			expectedResponse: nil,
			expectedValue:    nil,
			expectedErr:      errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SET, %d", i))

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

		res, err := handleSet(ctx, test.command, mockServer, nil)

		if test.expectedErr != nil {
			if err == nil {
				t.Errorf("expected error \"%s\", got nil", test.expectedErr.Error())
			}
			if test.expectedErr.Error() != err.Error() {
				t.Errorf("expected error \"%s\", got \"%s\"", test.expectedErr.Error(), err.Error())
			}
			continue
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
			t.Errorf("expected expiry time %d, got %d", test.expectedExpiry.Unix(), expireAt.Unix())
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
			mockServer.KeyRUnlock(context.Background(), key)
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
			if err = mockServer.SetValue(ctx, key, value); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, key)

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
			if err = mockServer.SetValue(context.Background(), key, test.presetValues[i]); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(context.Background(), key)
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
