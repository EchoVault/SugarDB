package sorted_set

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_HandleZADD(t *testing.T) {
	mockServer := server.NewServer(server.Opts{})

	tests := []struct {
		preset           bool
		presetValue      *SortedSet
		key              string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Create new sorted set and return the cardinality of the new sorted set.
			preset:      false,
			presetValue: nil,
			key:         "key1",
			command:     []string{"ZADD", "key1", "5.5", "member1", "67.77", "member2", "10", "member3"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. Only add the elements that do not currently exist in the sorted set when NX flag is provided
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key2",
			command: []string{"ZADD", "key2", "NX", "5.5", "member1", "67.77", "member4", "10", "member5"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
				{value: "member4", score: Score(67.77)},
				{value: "member5", score: Score(10)},
			}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 3. Do not add any elements when providing existing members with NX flag
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key3",
			command: []string{"ZADD", "key3", "NX", "5.5", "member1", "67.77", "member2", "10", "member3"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 4. Successfully add elements to an existing set when XX flag is provided with existing elements
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key4",
			command: []string{"ZADD", "key4", "XX", "CH", "55", "member1", "1005", "member2", "15", "member3", "99.75", "member4"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(55)},
				{value: "member2", score: Score(1005)},
				{value: "member3", score: Score(15)},
			}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 5. Fail to add element when providing XX flag with elements that do not exist in the sorted set.
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key5",
			command: []string{"ZADD", "key5", "XX", "5.5", "member4", "100.5", "member5", "15", "member6"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 6. Only update the elements where provided score is greater than current score if GT flag
			// Return only the new elements added by default
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key6",
			command: []string{"ZADD", "key6", "XX", "CH", "GT", "7.5", "member1", "100.5", "member4", "15", "member5"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(7.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			expectedResponse: 1,
			expectedError:    nil,
		},
		{ // 7. Only update the elements where provided score is less than current score if LT flag is provided
			// Return only the new elements added by default.
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key7",
			command: []string{"ZADD", "key7", "XX", "LT", "3.5", "member1", "100.5", "member4", "15", "member5"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(3.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 8. Return all the elements that were updated AND added when CH flag is provided
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key8",
			command: []string{"ZADD", "key8", "XX", "LT", "CH", "3.5", "member1", "100.5", "member4", "15", "member5"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(3.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			expectedResponse: 1,
			expectedError:    nil,
		},
		{ // 9. Increment the member by score
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "key9",
			command: []string{"ZADD", "key9", "INCR", "5.5", "member3"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(15.5)},
			}),
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 10. Fail when GT/LT flag is provided alongside NX flag
			preset:           false,
			presetValue:      nil,
			key:              "key10",
			command:          []string{"ZADD", "key10", "NX", "LT", "CH", "3.5", "member1", "100.5", "member4", "15", "member5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("GT/LT flags not allowed if NX flag is provided"),
		},
		{ // 11. Command is too short
			preset:           false,
			presetValue:      nil,
			key:              "key11",
			command:          []string{"ZADD", "key11"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WRONG_ARGS_RESPONSE),
		},
		{ // 12. Throw error when score/member entries are do not match
			preset:           false,
			presetValue:      nil,
			key:              "key11",
			command:          []string{"ZADD", "key12", "10.5", "member1", "12.5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("score/member pairs must be float/string"),
		},
		{ // 13. Throw error when INCR flag is passed with more than one score/member pair
			preset:           false,
			presetValue:      nil,
			key:              "key13",
			command:          []string{"ZADD", "key13", "INCR", "10.5", "member1", "12.5", "member2"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("cannot pass more than one score/member pair when INCR flag is provided"),
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
		res, err := handleZADD(context.Background(), test.command, mockServer, nil)
		if test.expectedError != nil {
			if err.Error() != test.expectedError.Error() {
				t.Errorf("expected error \"%s\", got \"%s\"", test.expectedError.Error(), err.Error())
			}
			continue
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
			t.Errorf("expected response %d at key \"%s\", got %d", test.expectedResponse, test.key, rv.Integer())
		}
		// Fetch the sorted set from the server and check it against the expected result
		if _, err = mockServer.KeyRLock(context.Background(), test.key); err != nil {
			t.Error(err)
		}
		sortedSet, ok := mockServer.GetValue(test.key).(*SortedSet)
		if !ok {
			t.Errorf("expected the value at key \"%s\" to be a sorted set, got another type", test.key)
		}
		if test.expectedValue == nil {
			continue
		}
		for _, member := range sortedSet.GetAll() {
			expectedMember := test.expectedValue.Get(member.value)
			if !expectedMember.exists {
				t.Errorf("could not find member %+v in expected sorted set, found in stored set", member)
			}
			if member.score != expectedMember.score {
				t.Errorf("expected member \"%s\" to have score %f, got score %f", expectedMember.value, expectedMember.score, member.score)
			}
		}
		mockServer.KeyRUnlock(test.key)
	}
}

func Test_HandleZCARD(t *testing.T) {}

func Test_HandleZCOUNT(t *testing.T) {}

func Test_HandleZLEXCOUNT(t *testing.T) {}

func Test_HandleZDIFF(t *testing.T) {}

func Test_HandleZDIFFSTORE(t *testing.T) {}

func Test_HandleZINCRBY(t *testing.T) {}

func Test_HandleZINTER(t *testing.T) {}

func Test_HandleZINTERSTORE(t *testing.T) {}

func Test_HandleZMPOP(t *testing.T) {}

func Test_HandleZPOP(t *testing.T) {}

func Test_HandleZMSCORE(t *testing.T) {}

func Test_HandleZRANDMEMBER(t *testing.T) {}

func Test_HandleZRANK(t *testing.T) {}

func Test_HandleZREM(t *testing.T) {}

func Test_HandleZSCORE(t *testing.T) {}

func Test_HandleZREMRANGEBYSCORE(t *testing.T) {}

func Test_HandleZREMRANGEBYRANK(t *testing.T) {}

func Test_HandleZREMRANGEBYLEX(t *testing.T) {}

func Test_HandleZRANGE(t *testing.T) {}

func Test_HandleZRANGESTORE(t *testing.T) {}

func Test_HandleZUNION(t *testing.T) {}

func Test_HandleZUNIONSTORE(t *testing.T) {}
