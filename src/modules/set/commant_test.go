package set

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_HandleSADD(t *testing.T) {
	mockserver := server.NewServer(server.Opts{})

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
			if _, err := mockserver.CreateKeyAndLock(context.Background(), test.key); err != nil {
				t.Error(err)
			}
			mockserver.SetValue(context.Background(), test.key, test.presetValue)
			mockserver.KeyUnlock(test.key)
		}
		res, err := handleSADD(context.Background(), test.command, mockserver, nil)
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
		if _, err = mockserver.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockserver.GetValue(test.key).(*Set)
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
		mockserver.KeyRUnlock(test.key)
	}
}

func Test_HandleSCARD(t *testing.T) {}

func Test_HandleSDIFF(t *testing.T) {}

func Test_HandleSDIFFSTORE(t *testing.T) {}

func Test_HandleSINTER(t *testing.T) {}

func Test_HandleSINTERCARD(t *testing.T) {}

func Test_HandleSINTERSTORE(t *testing.T) {}

func Test_HandleSISMEMBER(t *testing.T) {}

func Test_HandleSMEMBERS(t *testing.T) {}

func Test_HandleSMOVE(t *testing.T) {}

func Test_HandleSPOP(t *testing.T) {}

func Test_HandleSRANDMEMBER(t *testing.T) {}

func Test_HandleSREM(t *testing.T) {}

func Test_HandleSUNION(t *testing.T) {}

func Test_HandleSUNIONSTORE(t *testing.T) {}
