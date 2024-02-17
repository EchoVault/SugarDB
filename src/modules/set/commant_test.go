package set

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"slices"
	"testing"
)

func Test_HandleSADD(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedValue    *Set
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Create new set on a non-existent key, return count of added elements
			preset:           false,
			presetValue:      nil,
			key:              "key1",
			command:          []string{"SADD", "key1", "one", "two", "three", "four"},
			expectedValue:    NewSet([]string{"one", "two", "three", "four"}),
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 2. Add members to an exiting set, skip members that already exist in the set, return added count.
			preset:           true,
			presetValue:      NewSet([]string{"one", "two", "three", "four"}),
			key:              "key2",
			command:          []string{"SADD", "key2", "three", "four", "five", "six", "seven"},
			expectedValue:    NewSet([]string{"one", "two", "three", "four", "five", "six", "seven"}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to add to a key that does not hold a set
			preset:           true,
			presetValue:      "Default value",
			key:              "key3",
			command:          []string{"SADD", "key3", "member"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key key3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "key4",
			command:          []string{"SADD", "key4"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
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
		res, err := handleSADD(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected integer response %d, got %d", test.expectedResponse, rv.Integer())
		}
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockServer.GetValue(test.key).(*Set)
		if !ok {
			t.Errorf("expected set value at key \"%s\"", test.key)
		}
		if set.Cardinality() != test.expectedValue.Cardinality() {
			t.Errorf("expected resulting cardinality to be %d, got %d", test.expectedValue.Cardinality(), set.Cardinality())
		}
		for _, member := range set.GetAll() {
			if !test.expectedValue.Contains(member) {
				t.Errorf("could not find member \"%s\" in expected set", member)
			}
		}
		mockServer.KeyRUnlock(test.key)
	}
}

func Test_HandleSCARD(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedValue    *Set
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get cardinality of valid set.
			preset:           true,
			presetValue:      NewSet([]string{"one", "two", "three", "four"}),
			key:              "key1",
			command:          []string{"SCARD", "key1"},
			expectedValue:    nil,
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 2. Return 0 when trying to get cardinality on non-existent key
			preset:           false,
			presetValue:      nil,
			key:              "key2",
			command:          []string{"SCARD", "key2"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to get cardinality of a value that is not a set
			preset:           true,
			presetValue:      "Default value",
			key:              "key3",
			command:          []string{"SCARD", "key3"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key key3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "key4",
			command:          []string{"SCARD"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 5. Command too long
			preset:           false,
			key:              "key5",
			command:          []string{"SCARD", "key5", "key5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
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
		res, err := handleSCARD(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected integer response %d, got %d", test.expectedResponse, rv.Integer())
		}
	}
}

func Test_HandleSDIFF(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Get the difference between 2 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			command:          []string{"SDIFF", "key1", "key2"},
			expectedResponse: []string{"one", "two"},
			expectedError:    nil,
		},
		{ // 2. Get the difference between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key5": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFF", "key3", "key4", "key5"},
			expectedResponse: []string{"three", "four", "five", "six"},
			expectedError:    nil,
		},
		{ // 3. Return base set element if base set is the only valid set
			preset: true,
			presetValues: map[string]interface{}{
				"key6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": 123456789,
			},
			command:          []string{"SDIFF", "key6", "key7", "key8"},
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFF", "key9", "key10", "key11"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key key9 is not a set"),
		},
		{ // 5. Throw error when base set is non-existent.
			preset: true,
			presetValues: map[string]interface{}{
				"key12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFF", "non-existent", "key7", "key8"},
			expectedResponse: nil,
			expectedError:    errors.New("key for base set \"non-existent\" does not exist"),
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SDIFF"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(context.Background(), key); err != nil {
					t.Error(err)
				}
				mockServer.SetValue(context.Background(), key, value)
				mockServer.KeyUnlock(key)
			}
		}
		res, err := handleSDIFF(context.Background(), test.command, mockServer, nil)
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
		for _, responseElement := range rv.Array() {
			if !slices.Contains(test.expectedResponse, responseElement.String()) {
				t.Errorf("could not find response element \"%s\" from expected response array", responseElement.String())
			}
		}
	}
}

func Test_HandleSDIFFSTORE(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		destination      string
		command          []string
		expectedValue    *Set
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get the difference between 2 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination:      "destination1",
			command:          []string{"SDIFFSTORE", "destination1", "key1", "key2"},
			expectedValue:    NewSet([]string{"one", "two"}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 2. Get the difference between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key5": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "destination2",
			command:          []string{"SDIFFSTORE", "destination2", "key3", "key4", "key5"},
			expectedValue:    NewSet([]string{"three", "four", "five", "six"}),
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 3. Return base set element if base set is the only valid set
			preset: true,
			presetValues: map[string]interface{}{
				"key6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": 123456789,
			},
			destination:      "destination3",
			command:          []string{"SDIFFSTORE", "destination3", "key6", "key7", "key8"},
			expectedValue:    NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			expectedResponse: 8,
			expectedError:    nil,
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "destination4",
			command:          []string{"SDIFFSTORE", "destination4", "key9", "key10", "key11"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key key9 is not a set"),
		},
		{ // 5. Throw error when base set is non-existent.
			preset:      true,
			destination: "destination5",
			presetValues: map[string]interface{}{
				"key12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFFSTORE", "destination5", "non-existent", "key7", "key8"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("key for base set \"non-existent\" does not exist"),
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SDIFFSTORE", "destination6"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(context.Background(), key); err != nil {
					t.Error(err)
				}
				mockServer.SetValue(context.Background(), key, value)
				mockServer.KeyUnlock(key)
			}
		}
		res, err := handleSDIFFSTORE(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected response integer %d, got %d", test.expectedResponse, rv.Integer())
		}
		if test.expectedValue != nil {
			if _, err = mockServer.KeyRLock(context.Background(), test.destination); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(test.destination).(*Set)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem) {
					t.Errorf("could not find element %s in the expected values", elem)
				}
			}
			mockServer.KeyRUnlock(test.destination)
		}
	}
}

func Test_HandleSINTER(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Get the intersection between 2 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			command:          []string{"SINTER", "key1", "key2"},
			expectedResponse: []string{"three", "four", "five"},
			expectedError:    nil,
		},
		{ // 2. Get the difference between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key5": NewSet([]string{"one", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTER", "key3", "key4", "key5"},
			expectedResponse: []string{"one", "eight"},
			expectedError:    nil,
		},
		{ // 3. Throw an error if any of the provided keys are not sets
			preset: true,
			presetValues: map[string]interface{}{
				"key6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": NewSet([]string{"one"}),
			},
			command:          []string{"SINTER", "key6", "key7", "key8"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key key7 is not a set"),
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTER", "key9", "key10", "key11"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key key9 is not a set"),
		},
		{ // 5. If any of the keys does not exist, return an empty array.
			preset: true,
			presetValues: map[string]interface{}{
				"key12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTER", "non-existent", "key7", "key8"},
			expectedResponse: []string{},
			expectedError:    nil,
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SINTER"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(context.Background(), key); err != nil {
					t.Error(err)
				}
				mockServer.SetValue(context.Background(), key, value)
				mockServer.KeyUnlock(key)
			}
		}
		res, err := handleSINTER(context.Background(), test.command, mockServer, nil)
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
		for _, responseElement := range rv.Array() {
			if !slices.Contains(test.expectedResponse, responseElement.String()) {
				t.Errorf("could not find response element \"%s\" from expected response array", responseElement.String())
			}
		}
	}
}

func Test_HandleSINTERCARD(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get the full intersect cardinality between 2 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			command:          []string{"SINTERCARD", "key1", "key2"},
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. Get an intersect cardinality between 2 sets with a limit
			preset: true,
			presetValues: map[string]interface{}{
				"key3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"}),
				"key4": NewSet([]string{"three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"}),
			},
			command:          []string{"SINTERCARD", "key3", "key4", "LIMIT", "3"},
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 3. Get the full intersect cardinality between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"key5": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key6": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key7": NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERCARD", "key5", "key6", "key7"},
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 4. Get the intersection of 3 sets with a limit
			preset: true,
			presetValues: map[string]interface{}{
				"key8":  NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key9":  NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key10": NewSet([]string{"one", "two", "seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERCARD", "key8", "key9", "key10", "LIMIT", "2"},
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 5. Return 0 if any of the keys does not exist
			preset: true,
			presetValues: map[string]interface{}{
				"key11": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key12": "Default value",
				"key13": NewSet([]string{"one"}),
			},
			command:          []string{"SINTERCARD", "key11", "key12", "key13", "non-existent"},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 6. Throw error when one of the keys is not a valid set.
			preset: true,
			presetValues: map[string]interface{}{
				"key14": "Default value",
				"key15": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key16": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERSTORE", "key14", "key15", "key16"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key key14 is not a set"),
		},
		{ // 7. Command too short
			preset:           false,
			command:          []string{"SINTERSTORE"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(context.Background(), key); err != nil {
					t.Error(err)
				}
				mockServer.SetValue(context.Background(), key, value)
				mockServer.KeyUnlock(key)
			}
		}
		res, err := handleSINTERCARD(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected response integer %d, got %d", test.expectedResponse, rv.Integer())
		}
	}
}

func Test_HandleSINTERSTORE(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		destination      string
		command          []string
		expectedValue    *Set
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get the intersection between 2 sets and store it at the destination.
			preset: true,
			presetValues: map[string]interface{}{
				"key1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination:      "destination1",
			command:          []string{"SINTERSTORE", "destination1", "key1", "key2"},
			expectedValue:    NewSet([]string{"three", "four", "five"}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. Get the intersection between 3 sets and store it at the destination key.
			preset: true,
			presetValues: map[string]interface{}{
				"key3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key5": NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "destination2",
			command:          []string{"SINTERSTORE", "destination2", "key3", "key4", "key5"},
			expectedValue:    NewSet([]string{"one", "eight"}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 3. Throw error when any of the keys is not a set
			preset: true,
			presetValues: map[string]interface{}{
				"key6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": NewSet([]string{"one"}),
			},
			destination:      "destination3",
			command:          []string{"SINTERSTORE", "destination3", "key6", "key7", "key8"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key key7 is not a set"),
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "destination4",
			command:          []string{"SINTERSTORE", "destination4", "key9", "key10", "key11"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key key9 is not a set"),
		},
		{ // 5. Return an empty intersection if one of the keys does not exist.
			preset:      true,
			destination: "destination5",
			presetValues: map[string]interface{}{
				"key12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERSTORE", "destination5", "non-existent", "key7", "key8"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SINTERSTORE", "destination6"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(context.Background(), key); err != nil {
					t.Error(err)
				}
				mockServer.SetValue(context.Background(), key, value)
				mockServer.KeyUnlock(key)
			}
		}
		res, err := handleSINTERSTORE(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected response integer %d, got %d", test.expectedResponse, rv.Integer())
		}
		if test.expectedValue != nil {
			if _, err = mockServer.KeyRLock(context.Background(), test.destination); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(test.destination).(*Set)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem) {
					t.Errorf("could not find element %s in the expected values", elem)
				}
			}
			mockServer.KeyRUnlock(test.destination)
		}
	}
}

func Test_HandleSISMEMBER(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Return 1 when element is a member of the set
			preset:           true,
			presetValue:      NewSet([]string{"one", "two", "three", "four"}),
			key:              "key1",
			command:          []string{"SISMEMBER", "key1", "three"},
			expectedResponse: 1,
			expectedError:    nil,
		},
		{ // 2. Return 0 when element is not a member of the set
			preset:           true,
			presetValue:      NewSet([]string{"one", "two", "three", "four"}),
			key:              "key2",
			command:          []string{"SISMEMBER", "key2", "five"},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to assert membership when the key does not hold a valid set
			preset:           true,
			presetValue:      "Default value",
			key:              "key3",
			command:          []string{"SISMEMBER", "key3", "one"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key key3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "key4",
			command:          []string{"SISMEMBER", "key4"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 5. Command too long
			preset:           false,
			key:              "key5",
			command:          []string{"SISMEMBER", "key5", "one", "two", "three"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
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
		res, err := handleSISMEMBER(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected integer response %d, got %d", test.expectedResponse, rv.Integer())
		}
	}
}

func Test_HandleSMEMBERS(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Return all the members of the set.
			preset:           true,
			key:              "key1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five"}),
			command:          []string{"SMEMBERS", "key1"},
			expectedResponse: []string{"one", "two", "three", "four", "five"},
			expectedError:    nil,
		},
		{ // 2. If the key does not exist, return an empty array.
			preset:           false,
			key:              "key2",
			presetValue:      nil,
			command:          []string{"SMEMBERS", "key2"},
			expectedResponse: []string{},
			expectedError:    nil,
		},
		{ // 3. Throw error when the provided key is not a set.
			preset:           true,
			key:              "key3",
			presetValue:      "Default value",
			command:          []string{"SMEMBERS", "key3"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key key3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			command:          []string{"SMEMBERS"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 5. Command too long
			preset:           false,
			command:          []string{"SMEMBERS", "key5", "key6"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
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
		res, err := handleSMEMBERS(context.Background(), test.command, mockServer, nil)
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
		if len(rv.Array()) != len(test.expectedResponse) {
			t.Errorf("expected response array of length %d, got %d", len(test.expectedResponse), len(rv.Array()))
		}
		for _, responseElement := range rv.Array() {
			if !slices.Contains(test.expectedResponse, responseElement.String()) {
				t.Errorf("could not find response element \"%s\" from expected response array", responseElement.String())
			}
		}
	}
}

func Test_HandleSMISMEMBER(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedResponse []int
		expectedError    error
	}{
		{
			// 1. Return set membership status for multiple elements
			// Return 1 for present and 0 for absent
			// The placement of the membership status flag should me consistent with the order the elements
			// are in within the original command
			preset:           true,
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven"}),
			key:              "key1",
			command:          []string{"SMISMEMBER", "key1", "three", "four", "five", "six", "eight", "nine", "seven"},
			expectedResponse: []int{1, 1, 1, 1, 0, 0, 1},
			expectedError:    nil,
		},
		{ // 2. If the set key does not exist, return an array of zeroes as long as the list of members
			preset:           false,
			presetValue:      nil,
			key:              "key2",
			command:          []string{"SMISMEMBER", "key2", "one", "two", "three", "four"},
			expectedResponse: []int{0, 0, 0, 0},
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to assert membership when the key does not hold a valid set
			preset:           true,
			presetValue:      "Default value",
			key:              "key3",
			command:          []string{"SMISMEMBER", "key3", "one"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key key3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "key4",
			command:          []string{"SMISMEMBER", "key4"},
			expectedResponse: nil,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
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
		res, err := handleSMISMEMBER(context.Background(), test.command, mockServer, nil)
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
		responseArray := rv.Array()
		for i := 0; i < len(responseArray); i++ {
			if responseArray[i].Integer() != test.expectedResponse[i] {
				t.Errorf("expected integer %d at index %d, got %d", test.expectedResponse[i], i, responseArray[i].Integer())
			}
		}
	}
}

func Test_HandleSMOVE(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedValues   map[string]interface{}
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Return 1 after a successful move of a member from source set to destination set
			preset: true,
			presetValues: map[string]interface{}{
				"source1":      NewSet([]string{"one", "two", "three", "four"}),
				"destination1": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			command: []string{"SMOVE", "source1", "destination1", "four"},
			expectedValues: map[string]interface{}{
				"source1":      NewSet([]string{"one", "two", "three"}),
				"destination1": NewSet([]string{"four", "five", "six", "seven", "eight"}),
			},
			expectedResponse: 1,
			expectedError:    nil,
		},
		{ // 2. Return 0 when trying to move a member from source set to destination set when it doesn't exist in source
			preset: true,
			presetValues: map[string]interface{}{
				"source2":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"destination2": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			command: []string{"SMOVE", "source2", "destination2", "six"},
			expectedValues: map[string]interface{}{
				"source2":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"destination2": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error when the source key is not a set
			preset: true,
			presetValues: map[string]interface{}{
				"source3":      "Default value",
				"destination3": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			command: []string{"SMOVE", "source3", "destination3", "five"},
			expectedValues: map[string]interface{}{
				"source3":      "Default value",
				"destination3": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			expectedResponse: 0,
			expectedError:    errors.New("source is not a set"),
		},
		{ // 4. Return error when the destination key is not a set
			preset: true,
			presetValues: map[string]interface{}{
				"source4":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"destination4": "Default value",
			},
			command: []string{"SMOVE", "source4", "destination4", "five"},
			expectedValues: map[string]interface{}{
				"source4":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"destination4": "Default value",
			},
			expectedResponse: 0,
			expectedError:    errors.New("destination is not a set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"SMOVE", "source5", "source6"},
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"SMOVE", "source5", "source6", "member1", "member2"},
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
		},
	}

	for _, test := range tests {
		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(context.Background(), key); err != nil {
					t.Error(err)
				}
				mockServer.SetValue(context.Background(), key, value)
				mockServer.KeyUnlock(key)
			}
		}
		res, err := handleSMOVE(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected response integer %d, got %d", test.expectedResponse, rv.Integer())
		}
		for key, value := range test.expectedValues {
			expectedSet, ok := value.(*Set)
			if !ok {
				t.Errorf("expected value at \"%s\" should be a set", key)
			}
			if _, err = mockServer.KeyRLock(context.Background(), key); err != nil {
				t.Error(key)
			}
			set, ok := mockServer.GetValue(key).(*Set)
			if !ok {
				t.Errorf("expected set \"%s\" to be a set, got another type", key)
			}
			if expectedSet.Cardinality() != set.Cardinality() {
				t.Errorf("expected set to have cardinaltity %d, got %d", expectedSet.Cardinality(), set.Cardinality())
			}
			for _, element := range expectedSet.GetAll() {
				if !set.Contains(element) {
					t.Errorf("could not find element \"%s\" in the expected set", element)
				}
			}
			mockServer.KeyRUnlock(key)
		}
	}
}

func Test_HandleSPOP(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedValue    int // The final cardinality of the resulting set
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Return multiple popped elements and modify the set
			preset:           true,
			key:              "key1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			command:          []string{"SPOP", "key1", "3"},
			expectedValue:    5,
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{ // 2. Return error when the source key is not a set
			preset:           true,
			key:              "key2",
			presetValue:      "Default value",
			command:          []string{"SPOP", "key2"},
			expectedValue:    0,
			expectedResponse: []string{},
			expectedError:    errors.New("value at key2 is not a set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"SPOP"},
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"SPOP", "source5", "source6", "member1", "member2"},
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 7. Throw error when count is not an integer
			preset:        false,
			command:       []string{"SPOP", "key1", "count"},
			expectedError: errors.New("count must be an integer"),
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
		res, err := handleSPOP(context.Background(), test.command, mockServer, nil)
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
		// 1. Check if the response array members are all included in test.expectedResponse.
		for _, element := range rv.Array() {
			if !slices.Contains(test.expectedResponse, element.String()) {
				t.Errorf("expected response array does not contain element \"%s\"", element.String())
			}
		}
		// 2. Fetch the set and check if its cardinality is what we expect.
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockServer.GetValue(test.key).(*Set)
		if !ok {
			t.Errorf("expected value at key \"%s\" to be a set, got another type", test.key)
		}
		if set.Cardinality() != test.expectedValue {
			t.Errorf("expected cardinality of final set to be %d, got %d", test.expectedValue, set.Cardinality())
		}
		// 3. Check if all the popped elements we received are no longer in the set.
		for _, element := range rv.Array() {
			if set.Contains(element.String()) {
				t.Errorf("expected element \"%s\" to not be in set but it was found", element.String())
			}
		}
	}
}

func Test_HandleSRANDMEMBER(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedValue    int // The final cardinality of the resulting set
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Return multiple random elements without removing them
			preset:           true,
			key:              "key1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			command:          []string{"SRANDMEMBER", "key1", "3"},
			expectedValue:    8,
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{ // 2. Return error when the source key is not a set
			preset:           true,
			key:              "key2",
			presetValue:      "Default value",
			command:          []string{"SRANDMEMBER", "key2"},
			expectedValue:    0,
			expectedResponse: []string{},
			expectedError:    errors.New("value at key2 is not a set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"SRANDMEMBER"},
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"SRANDMEMBER", "source5", "source6", "member1", "member2"},
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 7. Throw error when count is not an integer
			preset:        false,
			command:       []string{"SRANDMEMBER", "key1", "count"},
			expectedError: errors.New("count must be an integer"),
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
		res, err := handleSRANDMEMBER(context.Background(), test.command, mockServer, nil)
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
		// 1. Check if the response array members are all included in test.expectedResponse.
		for _, element := range rv.Array() {
			if !slices.Contains(test.expectedResponse, element.String()) {
				t.Errorf("expected response array does not contain element \"%s\"", element.String())
			}
		}
		// 2. Fetch the set and check if its cardinality is what we expect.
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockServer.GetValue(test.key).(*Set)
		if !ok {
			t.Errorf("expected value at key \"%s\" to be a set, got another type", test.key)
		}
		if set.Cardinality() != test.expectedValue {
			t.Errorf("expected cardinality of final set to be %d, got %d", test.expectedValue, set.Cardinality())
		}
		// 3. Check if all the returned elements we received are still in the set.
		for _, element := range rv.Array() {
			if !set.Contains(element.String()) {
				t.Errorf("expected element \"%s\" to be in set but it was not found", element.String())
			}
		}
	}
}

func Test_HandleSREM(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedValue    *Set // The final cardinality of the resulting set
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Remove multiple elements and return the number of elements removed
			preset:           true,
			key:              "key1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			command:          []string{"SREM", "key1", "one", "two", "three", "nine"},
			expectedValue:    NewSet([]string{"four", "five", "six", "seven", "eight"}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. If key does not exist, return 0
			preset:           false,
			key:              "key2",
			presetValue:      nil,
			command:          []string{"SREM", "key1", "one", "two", "three", "nine"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error when the source key is not a set
			preset:           true,
			key:              "key3",
			presetValue:      "Default value",
			command:          []string{"SREM", "key3", "one"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key key3 is not a set"),
		},
		{ // 4. Command too short
			preset:        false,
			command:       []string{"SREM", "key"},
			expectedError: errors.New(utils.WRONG_ARGS_RESPONSE),
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
		res, err := handleSREM(context.Background(), test.command, mockServer, nil)
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
			t.Errorf("expected integer response %d, got %d", test.expectedResponse, rv.Integer())
		}
		if test.expectedValue != nil {
			if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(test.key).(*Set)
			if !ok {
				t.Errorf("expected value at key \"%s\" to be a set, got another type", test.key)
			}
			for _, element := range set.GetAll() {
				if !test.expectedValue.Contains(element) {
					t.Errorf("element \"%s\" not found in expected set values but found in set", element)
				}
			}
			mockServer.KeyRUnlock(test.key)
		}
	}
}

func Test_HandleSUNION(t *testing.T) {}

func Test_HandleSUNIONSTORE(t *testing.T) {}
