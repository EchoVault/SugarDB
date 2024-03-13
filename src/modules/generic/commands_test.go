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
