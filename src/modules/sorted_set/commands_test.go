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

package sorted_set

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"math"
	"slices"
	"strconv"
	"testing"
)

var mockServer *server.EchoVault

func init() {
	mockServer = server.NewEchoVault(server.Opts{
		Config: utils.Config{
			DataDir:        "",
			EvictionPolicy: utils.NoEviction,
		},
	})
}

func Test_HandleZADD(t *testing.T) {
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
			key:         "ZaddKey1",
			command:     []string{"ZADD", "ZaddKey1", "5.5", "member1", "67.77", "member2", "10", "member3", "-inf", "member4", "+inf", "member5"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
				{value: "member4", score: Score(math.Inf(-1))},
				{value: "member5", score: Score(math.Inf(1))},
			}),
			expectedResponse: 5,
			expectedError:    nil,
		},
		{ // 2. Only add the elements that do not currently exist in the sorted set when NX flag is provided
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:     "ZaddKey2",
			command: []string{"ZADD", "ZaddKey2", "NX", "5.5", "member1", "67.77", "member4", "10", "member5"},
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
			key:     "ZaddKey3",
			command: []string{"ZADD", "ZaddKey3", "NX", "5.5", "member1", "67.77", "member2", "10", "member3"},
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
			key:     "ZaddKey4",
			command: []string{"ZADD", "ZaddKey4", "XX", "CH", "55", "member1", "1005", "member2", "15", "member3", "99.75", "member4"},
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
			key:     "ZaddKey5",
			command: []string{"ZADD", "ZaddKey5", "XX", "5.5", "member4", "100.5", "member5", "15", "member6"},
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
			key:     "ZaddKey6",
			command: []string{"ZADD", "ZaddKey6", "XX", "CH", "GT", "7.5", "member1", "100.5", "member4", "15", "member5"},
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
			key:     "ZaddKey7",
			command: []string{"ZADD", "ZaddKey7", "XX", "LT", "3.5", "member1", "100.5", "member4", "15", "member5"},
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
			key:     "ZaddKey8",
			command: []string{"ZADD", "ZaddKey8", "XX", "LT", "CH", "3.5", "member1", "100.5", "member4", "15", "member5"},
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
			key:     "ZaddKey9",
			command: []string{"ZADD", "ZaddKey9", "INCR", "5.5", "member3"},
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
			key:              "ZaddKey10",
			command:          []string{"ZADD", "ZaddKey10", "NX", "LT", "CH", "3.5", "member1", "100.5", "member4", "15", "member5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("GT/LT flags not allowed if NX flag is provided"),
		},
		{ // 11. Command is too short
			preset:           false,
			presetValue:      nil,
			key:              "ZaddKey11",
			command:          []string{"ZADD", "ZaddKey11"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 12. Throw error when score/member entries are do not match
			preset:           false,
			presetValue:      nil,
			key:              "ZaddKey11",
			command:          []string{"ZADD", "ZaddKey12", "10.5", "member1", "12.5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("score/member pairs must be float/string"),
		},
		{ // 13. Throw error when INCR flag is passed with more than one score/member pair
			preset:           false,
			presetValue:      nil,
			key:              "ZaddKey13",
			command:          []string{"ZADD", "ZaddKey13", "INCR", "10.5", "member1", "12.5", "member2"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("cannot pass more than one score/member pair when INCR flag is provided"),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZADD, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleZADD(ctx, test.command, mockServer, nil)
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
		if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
			t.Error(err)
		}
		sortedSet, ok := mockServer.GetValue(ctx, test.key).(*SortedSet)
		if !ok {
			t.Errorf("expected the value at key \"%s\" to be a sorted set, got another type", test.key)
		}
		if test.expectedValue == nil {
			continue
		}
		if !sortedSet.Equals(test.expectedValue) {
			t.Errorf("expected sorted set %+v, got %+v", test.expectedValue, sortedSet)
		}
		mockServer.KeyRUnlock(ctx, test.key)
	}
}

func Test_HandleZCARD(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get cardinality of valid sorted set.
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
			}),
			key:              "ZcardKey1",
			command:          []string{"ZCARD", "ZcardKey1"},
			expectedValue:    nil,
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. Return 0 when trying to get cardinality from non-existent key
			preset:           false,
			presetValue:      nil,
			key:              "ZcardKey2",
			command:          []string{"ZCARD", "ZcardKey2"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Command is too short
			preset:           false,
			presetValue:      nil,
			key:              "ZcardKey3",
			command:          []string{"ZCARD"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 4. Command too long
			preset:           false,
			presetValue:      nil,
			key:              "ZcardKey4",
			command:          []string{"ZCARD", "ZcardKey4", "ZcardKey5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Return error when not a sorted set
			preset:           true,
			presetValue:      "Default value",
			key:              "ZcardKey5",
			command:          []string{"ZCARD", "ZcardKey5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at ZcardKey5 is not a sorted set"),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZCARD, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleZCARD(ctx, test.command, mockServer, nil)
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
	}
}

func Test_HandleZCOUNT(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get entire count using infinity boundaries
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
				{value: "member4", score: Score(1083.13)},
				{value: "member5", score: Score(11)},
				{value: "member6", score: Score(math.Inf(-1))},
				{value: "member7", score: Score(math.Inf(1))},
			}),
			key:              "ZcountKey1",
			command:          []string{"ZCOUNT", "ZcountKey1", "-inf", "+inf"},
			expectedValue:    nil,
			expectedResponse: 7,
			expectedError:    nil,
		},
		{ // 2. Get count of sub-set from -inf to limit
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
				{value: "member4", score: Score(1083.13)},
				{value: "member5", score: Score(11)},
				{value: "member6", score: Score(math.Inf(-1))},
				{value: "member7", score: Score(math.Inf(1))},
			}),
			key:              "ZcountKey2",
			command:          []string{"ZCOUNT", "ZcountKey2", "-inf", "90"},
			expectedValue:    nil,
			expectedResponse: 5,
			expectedError:    nil,
		},
		{ // 3. Get count of sub-set from bottom boundary to +inf limit
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "member1", score: Score(5.5)},
				{value: "member2", score: Score(67.77)},
				{value: "member3", score: Score(10)},
				{value: "member4", score: Score(1083.13)},
				{value: "member5", score: Score(11)},
				{value: "member6", score: Score(math.Inf(-1))},
				{value: "member7", score: Score(math.Inf(1))},
			}),
			key:              "ZcountKey3",
			command:          []string{"ZCOUNT", "ZcountKey3", "1000", "+inf"},
			expectedValue:    nil,
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 4. Return error when bottom boundary is not a valid double/float
			preset:           false,
			presetValue:      nil,
			key:              "ZcountKey4",
			command:          []string{"ZCOUNT", "ZcountKey4", "min", "10"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("min constraint must be a double"),
		},
		{ // 5. Return error when top boundary is not a valid double/float
			preset:           false,
			presetValue:      nil,
			key:              "ZcountKey5",
			command:          []string{"ZCOUNT", "ZcountKey5", "-10", "max"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("max constraint must be a double"),
		},
		{ // 6. Command is too short
			preset:           false,
			presetValue:      nil,
			key:              "ZcountKey6",
			command:          []string{"ZCOUNT"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 7. Command too long
			preset:           false,
			presetValue:      nil,
			key:              "ZcountKey7",
			command:          []string{"ZCOUNT", "ZcountKey4", "min", "max", "count"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 8. Throw error when value at the key is not a sorted set
			preset:           true,
			presetValue:      "Default value",
			key:              "ZcountKey8",
			command:          []string{"ZCOUNT", "ZcountKey8", "1", "10"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at ZcountKey8 is not a sorted set"),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZCARD, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleZCOUNT(ctx, test.command, mockServer, nil)
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
	}
}

func Test_HandleZLEXCOUNT(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get entire count using infinity boundaries
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "e", score: Score(1)},
				{value: "f", score: Score(1)},
				{value: "g", score: Score(1)},
				{value: "h", score: Score(1)},
				{value: "i", score: Score(1)},
				{value: "j", score: Score(1)},
				{value: "k", score: Score(1)},
			}),
			key:              "ZlexCountKey1",
			command:          []string{"ZLEXCOUNT", "ZlexCountKey1", "f", "j"},
			expectedValue:    nil,
			expectedResponse: 5,
			expectedError:    nil,
		},
		{ // 2. Return 0 when the members do not have the same score
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "a", score: Score(5.5)},
				{value: "b", score: Score(67.77)},
				{value: "c", score: Score(10)},
				{value: "d", score: Score(1083.13)},
				{value: "e", score: Score(11)},
				{value: "f", score: Score(math.Inf(-1))},
				{value: "g", score: Score(math.Inf(1))},
			}),
			key:              "ZlexCountKey2",
			command:          []string{"ZLEXCOUNT", "ZlexCountKey2", "a", "b"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return 0 when the key does not exist
			preset:           false,
			presetValue:      nil,
			key:              "ZlexCountKey3",
			command:          []string{"ZLEXCOUNT", "ZlexCountKey3", "a", "z"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 4. Return error when the value at the key is not a sorted set
			preset:           true,
			presetValue:      "Default value",
			key:              "ZlexCountKey4",
			command:          []string{"ZLEXCOUNT", "ZlexCountKey4", "a", "z"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at ZlexCountKey4 is not a sorted set"),
		},
		{ // 5. Command is too short
			preset:           false,
			presetValue:      nil,
			key:              "ZlexCountKey5",
			command:          []string{"ZLEXCOUNT"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 6. Command too long
			preset:           false,
			presetValue:      nil,
			key:              "ZlexCountKey6",
			command:          []string{"ZLEXCOUNT", "ZlexCountKey6", "min", "max", "count"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZLEXCOUNT, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleZLEXCOUNT(ctx, test.command, mockServer, nil)
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
	}
}

func Test_HandleZDIFF(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse [][]string
		expectedError    error
	}{
		{ // 1. Get the difference between 2 sorted sets without scores.
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1},
					{value: "two", score: 2},
					{value: "three", score: 3},
					{value: "four", score: 4},
				}),
				"ZdiffKey2": NewSortedSet([]MemberParam{
					{value: "three", score: 3},
					{value: "four", score: 4},
					{value: "five", score: 5},
					{value: "six", score: 6},
					{value: "seven", score: 7},
					{value: "eight", score: 8},
				}),
			},
			command:          []string{"ZDIFF", "ZdiffKey1", "ZdiffKey2"},
			expectedResponse: [][]string{{"one"}, {"two"}},
			expectedError:    nil,
		},
		{ // 2. Get the difference between 2 sorted sets with scores.
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1},
					{value: "two", score: 2},
					{value: "three", score: 3},
					{value: "four", score: 4},
				}),
				"ZdiffKey2": NewSortedSet([]MemberParam{
					{value: "three", score: 3},
					{value: "four", score: 4},
					{value: "five", score: 5},
					{value: "six", score: 6},
					{value: "seven", score: 7},
					{value: "eight", score: 8},
				}),
			},
			command:          []string{"ZDIFF", "ZdiffKey1", "ZdiffKey2", "WITHSCORES"},
			expectedResponse: [][]string{{"one", "1"}, {"two", "2"}},
			expectedError:    nil,
		},
		{ // 3. Get the difference between 3 sets with scores.
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZdiffKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZdiffKey5": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZDIFF", "ZdiffKey3", "ZdiffKey4", "ZdiffKey5", "WITHSCORES"},
			expectedResponse: [][]string{{"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
			expectedError:    nil,
		},
		{ // 3. Return sorted set if only one key exists and is a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffKey6": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			command: []string{"ZDIFF", "ZdiffKey6", "ZdiffKey7", "ZdiffKey8", "WITHSCORES"},
			expectedResponse: [][]string{
				{"one", "1"}, {"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"},
				{"six", "6"}, {"seven", "7"}, {"eight", "8"},
			},
			expectedError: nil,
		},
		{ // 4. Throw error when one of the keys is not a sorted set.
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffKey9": "Default value",
				"ZdiffKey10": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZdiffKey11": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZDIFF", "ZdiffKey9", "ZdiffKey10", "ZdiffKey11"},
			expectedResponse: nil,
			expectedError:    errors.New("value at ZdiffKey9 is not a sorted set"),
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"ZDIFF"},
			expectedResponse: [][]string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZDIFF, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZDIFF(ctx, test.command, mockServer, nil)
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
		for _, element := range rv.Array() {
			if !slices.ContainsFunc(test.expectedResponse, func(expected []string) bool {
				// The current sub-slice is a different length, return false because they're not equal
				if len(element.Array()) != len(expected) {
					return false
				}
				for i := 0; i < len(expected); i++ {
					if element.Array()[i].String() != expected[i] {
						return false
					}
				}
				return true
			}) {
				t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
			}
		}
	}
}

func Test_HandleZDIFFSTORE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		destination      string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get the difference between 2 sorted sets.
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffStoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
				"ZdiffStoreKey2": NewSortedSet([]MemberParam{
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination:      "ZdiffStoreDestinationKey1",
			command:          []string{"ZDIFFSTORE", "ZdiffStoreDestinationKey1", "ZdiffStoreKey1", "ZdiffStoreKey2"},
			expectedValue:    NewSortedSet([]MemberParam{{value: "one", score: 1}, {value: "two", score: 2}}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 2. Get the difference between 3 sorted sets.
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffStoreKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZdiffStoreKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZdiffStoreKey5": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZdiffStoreDestinationKey2",
			command:     []string{"ZDIFFSTORE", "ZdiffStoreDestinationKey2", "ZdiffStoreKey3", "ZdiffStoreKey4", "ZdiffStoreKey5"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6},
			}),
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 3. Return base sorted set element if base set is the only existing key provided and is a valid sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffStoreKey6": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination: "ZdiffStoreDestinationKey3",
			command:     []string{"ZDIFFSTORE", "ZdiffStoreDestinationKey3", "ZdiffStoreKey6", "ZdiffStoreKey7", "ZdiffStoreKey8"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6},
				{value: "seven", score: 7}, {value: "eight", score: 8},
			}),
			expectedResponse: 8,
			expectedError:    nil,
		},
		{ // 4. Throw error when base sorted set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"ZdiffStoreKey9": "Default value",
				"ZdiffStoreKey10": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZdiffStoreKey11": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination:      "ZdiffStoreDestinationKey4",
			command:          []string{"ZDIFFSTORE", "ZdiffStoreDestinationKey4", "ZdiffStoreKey9", "ZdiffStoreKey10", "ZdiffStoreKey11"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at ZdiffStoreKey9 is not a sorted set"),
		},
		{ // 5. Throw error when base set is non-existent.
			preset:      true,
			destination: "ZdiffStoreDestinationKey5",
			presetValues: map[string]interface{}{
				"ZdiffStoreKey12": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZdiffStoreKey13": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZDIFFSTORE", "ZdiffStoreDestinationKey5", "non-existent", "ZdiffStoreKey12", "ZdiffStoreKey13"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"ZDIFFSTORE", "ZdiffStoreDestinationKey6"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZDIFFSTORE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZDIFFSTORE(ctx, test.command, mockServer, nil)
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
			if _, err = mockServer.KeyRLock(ctx, test.destination); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, test.destination).(*SortedSet)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem.value) {
					t.Errorf("could not find element %s in the expected values", elem.value)
				}
			}
			mockServer.KeyRUnlock(ctx, test.destination)
		}
	}
}

func Test_HandleZINCRBY(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValue      interface{}
		key              string
		command          []string
		expectedValue    *SortedSet
		expectedResponse string
		expectedError    error
	}{
		{ // 1. Successfully increment by int. Return the new score
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			key:     "ZincrbyKey1",
			command: []string{"ZINCRBY", "ZincrbyKey1", "5", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 6}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			expectedResponse: "6",
			expectedError:    nil,
		},
		{ // 2. Successfully increment by float. Return new score
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			key:     "ZincrbyKey2",
			command: []string{"ZINCRBY", "ZincrbyKey2", "346.785", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 347.785}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			expectedResponse: "347.785",
			expectedError:    nil,
		},
		{ // 3. Increment on non-existent sorted set will create the set with the member and increment as its score
			preset:      false,
			presetValue: nil,
			key:         "ZincrbyKey3",
			command:     []string{"ZINCRBY", "ZincrbyKey3", "346.785", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 346.785},
			}),
			expectedResponse: "346.785",
			expectedError:    nil,
		},
		{ // 4. Increment score to +inf
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			key:     "ZincrbyKey4",
			command: []string{"ZINCRBY", "ZincrbyKey4", "+inf", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: Score(math.Inf(1))}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			expectedResponse: "+Inf",
			expectedError:    nil,
		},
		{ // 5. Increment score to -inf
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			key:     "ZincrbyKey5",
			command: []string{"ZINCRBY", "ZincrbyKey5", "-inf", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: Score(math.Inf(-1))}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			expectedResponse: "-Inf",
			expectedError:    nil,
		},
		{ // 6. Incrementing score by negative increment should lower the score
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			key:     "ZincrbyKey6",
			command: []string{"ZINCRBY", "ZincrbyKey6", "-2.5", "five"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 2.5},
			}),
			expectedResponse: "2.5",
			expectedError:    nil,
		},
		{ // 7. Return error when attempting to increment on a value that is not a valid sorted set
			preset:           true,
			presetValue:      "Default value",
			key:              "ZincrbyKey7",
			command:          []string{"ZINCRBY", "ZincrbyKey7", "-2.5", "five"},
			expectedValue:    nil,
			expectedResponse: "",
			expectedError:    errors.New("value at ZincrbyKey7 is not a sorted set"),
		},
		{ // 8. Return error when trying to increment a member that already has score -inf
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: Score(math.Inf(-1))},
			}),
			key:     "ZincrbyKey8",
			command: []string{"ZINCRBY", "ZincrbyKey8", "2.5", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: Score(math.Inf(-1))},
			}),
			expectedResponse: "",
			expectedError:    errors.New("cannot increment -inf or +inf"),
		},
		{ // 9. Return error when trying to increment a member that already has score +inf
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: Score(math.Inf(1))},
			}),
			key:     "ZincrbyKey9",
			command: []string{"ZINCRBY", "ZincrbyKey9", "2.5", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: Score(math.Inf(-1))},
			}),
			expectedResponse: "",
			expectedError:    errors.New("cannot increment -inf or +inf"),
		},
		{ // 10. Return error when increment is not a valid number
			preset: true,
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1},
			}),
			key:     "ZincrbyKey10",
			command: []string{"ZINCRBY", "ZincrbyKey10", "increment", "one"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1},
			}),
			expectedResponse: "",
			expectedError:    errors.New("increment must be a double"),
		},
		{ // 11. Command too short
			key:              "ZincrbyKey11",
			command:          []string{"ZINCRBY", "ZincrbyKey11", "one"},
			expectedResponse: "",
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 12. Command too long
			key:              "ZincrbyKey12",
			command:          []string{"ZINCRBY", "ZincrbyKey12", "one", "1", "2"},
			expectedResponse: "",
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZINCRBY, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleZINCRBY(ctx, test.command, mockServer, nil)
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
		if rv.String() != test.expectedResponse {
			t.Errorf("expected response integer %s, got %s", test.expectedResponse, rv.String())
		}
		if test.expectedValue != nil {
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, test.key).(*SortedSet)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.key)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem.value) {
					t.Errorf("could not find element %s in the expected values", elem.value)
				}
				if test.expectedValue.Get(elem.value).score != elem.score {
					t.Errorf("expected score of element \"%s\" from set at key \"%s\" to be %s, got %s",
						elem.value, test.key,
						strconv.FormatFloat(float64(test.expectedValue.Get(elem.value).score), 'f', -1, 64),
						strconv.FormatFloat(float64(elem.score), 'f', -1, 64),
					)
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		}
	}
}

func Test_HandleZMPOP(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedValues   map[string]*SortedSet
		expectedResponse [][]string
		expectedError    error
	}{
		{ // 1. Successfully pop one min element by default
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			command: []string{"ZMPOP", "ZmpopKey1"},
			expectedValues: map[string]*SortedSet{
				"ZmpopKey1": NewSortedSet([]MemberParam{
					{value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			expectedResponse: [][]string{
				{"one", "1"},
			},
			expectedError: nil,
		},
		{ // 2. Successfully pop one min element by specifying MIN
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopKey2": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			command: []string{"ZMPOP", "ZmpopKey2", "MIN"},
			expectedValues: map[string]*SortedSet{
				"ZmpopKey2": NewSortedSet([]MemberParam{
					{value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			expectedResponse: [][]string{
				{"one", "1"},
			},
			expectedError: nil,
		},
		{ // 3. Successfully pop one max element by specifying MAX modifier
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			command: []string{"ZMPOP", "ZmpopKey3", "MAX"},
			expectedValues: map[string]*SortedSet{
				"ZmpopKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
				}),
			},
			expectedResponse: [][]string{
				{"five", "5"},
			},
			expectedError: nil,
		},
		{ // 4. Successfully pop multiple min elements
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
				}),
			},
			command: []string{"ZMPOP", "ZmpopKey4", "MIN", "COUNT", "5"},
			expectedValues: map[string]*SortedSet{
				"ZmpopKey4": NewSortedSet([]MemberParam{
					{value: "six", score: 6},
				}),
			},
			expectedResponse: [][]string{
				{"one", "1"}, {"two", "2"}, {"three", "3"},
				{"four", "4"}, {"five", "5"},
			},
			expectedError: nil,
		},
		{ // 5. Successfully pop multiple max elements
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopKey5": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
				}),
			},
			command: []string{"ZMPOP", "ZmpopKey5", "MAX", "COUNT", "5"},
			expectedValues: map[string]*SortedSet{
				"ZmpopKey5": NewSortedSet([]MemberParam{
					{value: "one", score: 1},
				}),
			},
			expectedResponse: [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
			expectedError:    nil,
		},
		{ // 6. Successfully pop elements from the first set which is non-empty
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopKey6": NewSortedSet([]MemberParam{}),
				"ZmpopKey7": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
				}),
			},
			command: []string{"ZMPOP", "ZmpopKey6", "ZmpopKey7", "MAX", "COUNT", "5"},
			expectedValues: map[string]*SortedSet{
				"ZmpopKey6": NewSortedSet([]MemberParam{}),
				"ZmpopKey7": NewSortedSet([]MemberParam{
					{value: "one", score: 1},
				}),
			},
			expectedResponse: [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
			expectedError:    nil,
		},
		{ // 7. Skip the non-set items and pop elements from the first non-empty sorted set found
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopKey8":  "Default value",
				"ZmpopKey9":  56,
				"ZmpopKey10": NewSortedSet([]MemberParam{}),
				"ZmpopKey11": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
				}),
			},
			command: []string{"ZMPOP", "ZmpopKey8", "ZmpopKey9", "ZmpopKey10", "ZmpopKey11", "MIN", "COUNT", "5"},
			expectedValues: map[string]*SortedSet{
				"ZmpopKey10": NewSortedSet([]MemberParam{}),
				"ZmpopKey11": NewSortedSet([]MemberParam{
					{value: "six", score: 6},
				}),
			},
			expectedResponse: [][]string{{"one", "1"}, {"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}},
			expectedError:    nil,
		},
		{ // 9. Return error when count is a negative integer
			preset:        false,
			command:       []string{"ZMPOP", "ZmpopKey8", "MAX", "COUNT", "-20"},
			expectedError: errors.New("count must be a positive integer"),
		},
		{ // 9. Command too short
			preset:        false,
			command:       []string{"ZMPOP"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZMPOP, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZMPOP(ctx, test.command, mockServer, nil)
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
		for _, element := range rv.Array() {
			if !slices.ContainsFunc(test.expectedResponse, func(expected []string) bool {
				// The current sub-slice is a different length, return false because they're not equal
				if len(element.Array()) != len(expected) {
					return false
				}
				for i := 0; i < len(expected); i++ {
					if element.Array()[i].String() != expected[i] {
						return false
					}
				}
				return true
			}) {
				t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
			}
		}
		for key, expectedSortedSet := range test.expectedValues {
			if _, err = mockServer.KeyRLock(ctx, key); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, key).(*SortedSet)
			if !ok {
				t.Errorf("expected key \"%s\" to be a sorted set, got another type", key)
			}
			if !set.Equals(expectedSortedSet) {
				t.Errorf("expected sorted set at key \"%s\" %+v, got %+v", key, expectedSortedSet, set)
			}
		}
	}
}

func Test_HandleZPOP(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedValues   map[string]*SortedSet
		expectedResponse [][]string
		expectedError    error
	}{
		{ // 1. Successfully pop one min element by default
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopMinKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			command: []string{"ZPOPMIN", "ZmpopMinKey1"},
			expectedValues: map[string]*SortedSet{
				"ZmpopMinKey1": NewSortedSet([]MemberParam{
					{value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			expectedResponse: [][]string{
				{"one", "1"},
			},
			expectedError: nil,
		},
		{ // 2. Successfully pop one max element by default
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopMaxKey2": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			command: []string{"ZPOPMAX", "ZmpopMaxKey2"},
			expectedValues: map[string]*SortedSet{
				"ZmpopMaxKey2": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
				}),
			},
			expectedResponse: [][]string{
				{"five", "5"},
			},
			expectedError: nil,
		},
		{ // 3. Successfully pop multiple min elements
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopMinKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
				}),
			},
			command: []string{"ZPOPMIN", "ZmpopMinKey3", "5"},
			expectedValues: map[string]*SortedSet{
				"ZmpopMinKey3": NewSortedSet([]MemberParam{
					{value: "six", score: 6},
				}),
			},
			expectedResponse: [][]string{
				{"one", "1"}, {"two", "2"}, {"three", "3"},
				{"four", "4"}, {"five", "5"},
			},
			expectedError: nil,
		},
		{ // 4. Successfully pop multiple max elements
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopMaxKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
				}),
			},
			command: []string{"ZPOPMAX", "ZmpopMaxKey4", "5"},
			expectedValues: map[string]*SortedSet{
				"ZmpopMaxKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1},
				}),
			},
			expectedResponse: [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
			expectedError:    nil,
		},
		{ // 5. Throw an error when trying to pop from an element that's not a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZmpopMinKey5": "Default value",
			},
			command:          []string{"ZPOPMIN", "ZmpopMinKey5"},
			expectedValues:   nil,
			expectedResponse: nil,
			expectedError:    errors.New("value at key ZmpopMinKey5 is not a sorted set"),
		},
		{ // 6. Command too short
			preset:        false,
			command:       []string{"ZPOPMAX"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 7. Command too long
			preset:        false,
			command:       []string{"ZPOPMAX", "ZmpopMaxKey7", "6", "3"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZPOPMIN/ZPOPMAX, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZPOP(ctx, test.command, mockServer, nil)
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
		for _, element := range rv.Array() {
			if !slices.ContainsFunc(test.expectedResponse, func(expected []string) bool {
				// The current sub-slice is a different length, return false because they're not equal
				if len(element.Array()) != len(expected) {
					return false
				}
				for i := 0; i < len(expected); i++ {
					if element.Array()[i].String() != expected[i] {
						return false
					}
				}
				return true
			}) {
				t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
			}
		}
		for key, expectedSortedSet := range test.expectedValues {
			if _, err = mockServer.KeyRLock(ctx, key); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, key).(*SortedSet)
			if !ok {
				t.Errorf("expected key \"%s\" to be a sorted set, got another type", key)
			}
			if !set.Equals(expectedSortedSet) {
				t.Errorf("expected sorted set at key \"%s\" %+v, got %+v", key, expectedSortedSet, set)
			}
		}
	}
}

func Test_HandleZMSCORE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse []interface{}
		expectedError    error
	}{
		{ // 1. Return multiple scores from the sorted set.
			// Return nil for elements that do not exist in the sorted set.
			preset: true,
			presetValues: map[string]interface{}{
				"ZmScoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1.1}, {value: "two", score: 245},
					{value: "three", score: 3}, {value: "four", score: 4.055},
					{value: "five", score: 5},
				}),
			},
			command:          []string{"ZMSCORE", "ZmScoreKey1", "one", "none", "two", "one", "three", "four", "none", "five"},
			expectedResponse: []interface{}{"1.1", nil, "245", "1.1", "3", "4.055", nil, "5"},
			expectedError:    nil,
		},
		{ // 2. If key does not exist, return empty array
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZMSCORE", "ZmScoreKey2", "one", "two", "three", "four"},
			expectedResponse: []interface{}{},
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to find scores from elements that are not sorted sets
			preset:        true,
			presetValues:  map[string]interface{}{"ZmScoreKey3": "Default value"},
			command:       []string{"ZMSCORE", "ZmScoreKey3", "one", "two", "three"},
			expectedError: errors.New("value at ZmScoreKey3 is not a sorted set"),
		},
		{ // 9. Command too short
			preset:        false,
			command:       []string{"ZMSCORE"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZMSCORE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZMSCORE(ctx, test.command, mockServer, nil)
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
		for i := 0; i < len(rv.Array()); i++ {
			if rv.Array()[i].IsNull() {
				if test.expectedResponse[i] != nil {
					t.Errorf("expected element at index %d to be %+v, got %+v", i, test.expectedResponse[i], rv.Array()[i])
				}
				continue
			}
			if rv.Array()[i].String() != test.expectedResponse[i] {
				t.Errorf("expected \"%s\" at index %d, got %s", test.expectedResponse[i], i, rv.Array()[i].String())
			}
		}
	}
}

func Test_HandleZSCORE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse interface{}
		expectedError    error
	}{
		{ // 1. Return score from a sorted set.
			preset: true,
			presetValues: map[string]interface{}{
				"ZscoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1.1}, {value: "two", score: 245},
					{value: "three", score: 3}, {value: "four", score: 4.055},
					{value: "five", score: 5},
				}),
			},
			command:          []string{"ZSCORE", "ZscoreKey1", "four"},
			expectedResponse: "4.055",
			expectedError:    nil,
		},
		{ // 2. If key does not exist, return nil value
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZSCORE", "ZscoreKey2", "one"},
			expectedResponse: nil,
			expectedError:    nil,
		},
		{ // 3. If key exists and is a sorted set, but the member does not exist, return nil
			preset: true,
			presetValues: map[string]interface{}{
				"ZscoreKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1.1}, {value: "two", score: 245},
					{value: "three", score: 3}, {value: "four", score: 4.055},
					{value: "five", score: 5},
				}),
			},
			command:          []string{"ZSCORE", "ZscoreKey3", "non-existent"},
			expectedResponse: nil,
			expectedError:    nil,
		},
		{ // 4. Throw error when trying to find scores from elements that are not sorted sets
			preset:        true,
			presetValues:  map[string]interface{}{"ZscoreKey4": "Default value"},
			command:       []string{"ZSCORE", "ZscoreKey4", "one"},
			expectedError: errors.New("value at ZscoreKey4 is not a sorted set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"ZSCORE"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"ZSCORE", "ZscoreKey5", "one", "two"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZSCORE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZSCORE(ctx, test.command, mockServer, nil)
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
		if test.expectedResponse == nil {
			if !rv.IsNull() {
				t.Errorf("expected nil response, got %+v", rv)
			}
			continue
		}
		if rv.String() != test.expectedResponse {
			t.Errorf("expected response \"%s\", got %s", test.expectedResponse, rv.String())
		}
	}
}

func Test_HandleZRANDMEMBER(t *testing.T) {
	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedValue    int // The final cardinality of the resulting set
		allowRepeat      bool
		expectedResponse [][]string
		expectedError    error
	}{
		{ // 1. Return multiple random elements without removing them
			// Count is positive, do not allow repeated elements
			preset: true,
			key:    "ZrandMemberKey1",
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 8},
			}),
			command:       []string{"ZRANDMEMBER", "ZrandMemberKey1", "3"},
			expectedValue: 8,
			allowRepeat:   false,
			expectedResponse: [][]string{
				{"one"}, {"two"}, {"three"}, {"four"},
				{"five"}, {"six"}, {"seven"}, {"eight"},
			},
			expectedError: nil,
		},
		{
			// 2. Return multiple random elements and their scores without removing them.
			// Count is negative, so allow repeated numbers.
			preset: true,
			key:    "ZrandMemberKey2",
			presetValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 8},
			}),
			command:       []string{"ZRANDMEMBER", "ZrandMemberKey2", "-5", "WITHSCORES"},
			expectedValue: 8,
			allowRepeat:   true,
			expectedResponse: [][]string{
				{"one", "1"}, {"two", "2"}, {"three", "3"}, {"four", "4"},
				{"five", "5"}, {"six", "6"}, {"seven", "7"}, {"eight", "8"},
			},
			expectedError: nil,
		},
		{ // 2. Return error when the source key is not a sorted set.
			preset:        true,
			key:           "ZrandMemberKey3",
			presetValue:   "Default value",
			command:       []string{"ZRANDMEMBER", "ZrandMemberKey3"},
			expectedValue: 0,
			expectedError: errors.New("value at ZrandMemberKey3 is not a sorted set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"ZRANDMEMBER"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"ZRANDMEMBER", "source5", "source6", "member1", "member2"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 7. Throw error when count is not an integer
			preset:        false,
			command:       []string{"ZRANDMEMBER", "ZrandMemberKey1", "count"},
			expectedError: errors.New("count must be an integer"),
		},
		{ // 8. Throw error when the fourth argument is not WITHSCORES
			preset:        false,
			command:       []string{"ZRANDMEMBER", "ZrandMemberKey1", "8", "ANOTHER"},
			expectedError: errors.New("last option must be WITHSCORES"),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZRANDMEMBER, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleZRANDMEMBER(ctx, test.command, mockServer, nil)
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
			if !slices.ContainsFunc(test.expectedResponse, func(expected []string) bool {
				// The current sub-slice is a different length, return false because they're not equal
				if len(element.Array()) != len(expected) {
					return false
				}
				for i := 0; i < len(expected); i++ {
					if element.Array()[i].String() != expected[i] {
						return false
					}
				}
				return true
			}) {
				t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
			}
		}
		// 2. Fetch the set and check if its cardinality is what we expect.
		if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockServer.GetValue(ctx, test.key).(*SortedSet)
		if !ok {
			t.Errorf("expected value at key \"%s\" to be a set, got another type", test.key)
		}
		if set.Cardinality() != test.expectedValue {
			t.Errorf("expected cardinality of final set to be %d, got %d", test.expectedValue, set.Cardinality())
		}
		// 3. Check if all the returned elements we received are still in the set.
		for _, element := range rv.Array() {
			if !set.Contains(Value(element.Array()[0].String())) {
				t.Errorf("expected element \"%s\" to be in set but it was not found", element.String())
			}
		}
		// 4. If allowRepeat is false, check that all the elements make a valid set
		if !test.allowRepeat {
			var elems []MemberParam
			for _, e := range rv.Array() {
				if len(e.Array()) == 1 {
					elems = append(elems, MemberParam{
						value: Value(e.Array()[0].String()),
						score: 1,
					})
					continue
				}
				elems = append(elems, MemberParam{
					value: Value(e.Array()[0].String()),
					score: Score(e.Array()[1].Float()),
				})
			}
			s := NewSortedSet(elems)
			if s.Cardinality() != len(elems) {
				t.Errorf("expected non-repeating elements for random elements at key \"%s\"", test.key)
			}
		}
	}
}

func Test_HandleZRANK(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Return element's rank from a sorted set.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrankKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
			},
			command:          []string{"ZRANK", "ZrankKey1", "four"},
			expectedResponse: []string{"3"},
			expectedError:    nil,
		},
		{ // 2. Return element's rank from a sorted set with its score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrankKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 100.1}, {value: "two", score: 245},
					{value: "three", score: 305.43}, {value: "four", score: 411.055},
					{value: "five", score: 500},
				}),
			},
			command:          []string{"ZRANK", "ZrankKey1", "four", "WITHSCORES"},
			expectedResponse: []string{"3", "411.055"},
			expectedError:    nil,
		},
		{ // 3. If key does not exist, return nil value
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANK", "ZrankKey3", "one"},
			expectedResponse: nil,
			expectedError:    nil,
		},
		{ // 4. If key exists and is a sorted set, but the member does not exist, return nil
			preset: true,
			presetValues: map[string]interface{}{
				"ZrankKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1.1}, {value: "two", score: 245},
					{value: "three", score: 3}, {value: "four", score: 4.055},
					{value: "five", score: 5},
				}),
			},
			command:          []string{"ZRANK", "ZrankKey4", "non-existent"},
			expectedResponse: nil,
			expectedError:    nil,
		},
		{ // 5. Throw error when trying to find scores from elements that are not sorted sets
			preset:        true,
			presetValues:  map[string]interface{}{"ZrankKey5": "Default value"},
			command:       []string{"ZRANK", "ZrankKey5", "one"},
			expectedError: errors.New("value at ZrankKey5 is not a sorted set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"ZRANK"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"ZRANK", "ZrankKey5", "one", "WITHSCORES", "two"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZRANK, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZRANK(ctx, test.command, mockServer, nil)
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
		if test.expectedResponse == nil {
			if !rv.IsNull() {
				t.Errorf("expected nil response, got %+v", rv)
			}
			continue
		}
		if len(rv.Array()) != len(test.expectedResponse) {
			t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
		}
		for i := 0; i < len(test.expectedResponse); i++ {
			if rv.Array()[i].String() != test.expectedResponse[i] {
				t.Errorf("expected element at index %d to be %s, got %s", i, test.expectedResponse[i], rv.Array()[i].String())
			}
		}
	}
}

func Test_HandleZREM(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedValues   map[string]*SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Successfully remove multiple elements from sorted set, skipping non-existent members.
			// Return deleted count.
			preset: true,
			presetValues: map[string]interface{}{
				"ZremKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			command: []string{"ZREM", "ZremKey1", "three", "four", "five", "none", "six", "none", "seven"},
			expectedValues: map[string]*SortedSet{
				"ZremKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			expectedResponse: 5,
			expectedError:    nil,
		},
		{ // 2. If key does not exist, return 0
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZREM", "ZremKey2", "member"},
			expectedValues:   nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error key is not a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZremKey3": "Default value",
			},
			command:       []string{"ZREM", "ZremKey3", "member"},
			expectedError: errors.New("value at ZremKey3 is not a sorted set"),
		},
		{ // 9. Command too short
			preset:        false,
			command:       []string{"ZREM"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZREM, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZREM(ctx, test.command, mockServer, nil)
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
			t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
		}
		// Check if the expected sorted set is the same at the current one
		if test.expectedValues != nil {
			for key, expectedSet := range test.expectedValues {
				if _, err = mockServer.KeyRLock(ctx, key); err != nil {
					t.Error(err)
				}
				set, ok := mockServer.GetValue(ctx, key).(*SortedSet)
				if !ok {
					t.Errorf("expected value at key \"%s\" to be a sorted set, got another type", key)
				}
				if !set.Equals(expectedSet) {
					t.Errorf("exptected sorted set %+v, got %+v", expectedSet, set)
				}
			}
		}
	}
}

func Test_HandleZREMRANGEBYSCORE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedValues   map[string]*SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Successfully remove multiple elements with scores inside the provided range
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByScoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			command: []string{"ZREMRANGEBYSCORE", "ZremRangeByScoreKey1", "3", "7"},
			expectedValues: map[string]*SortedSet{
				"ZremRangeByScoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			expectedResponse: 5,
			expectedError:    nil,
		},
		{ // 2. If key does not exist, return 0
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZREMRANGEBYSCORE", "ZremRangeByScoreKey2", "2", "4"},
			expectedValues:   nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error key is not a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByScoreKey3": "Default value",
			},
			command:       []string{"ZREMRANGEBYSCORE", "ZremRangeByScoreKey3", "4", "4"},
			expectedError: errors.New("value at ZremRangeByScoreKey3 is not a sorted set"),
		},
		{ // 4. Command too short
			preset:        false,
			command:       []string{"ZREMRANGEBYSCORE", "ZremRangeByScoreKey4", "3"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Command too long
			preset:        false,
			command:       []string{"ZREMRANGEBYSCORE", "ZremRangeByScoreKey5", "4", "5", "8"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZREMRANGEBYSCORE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZREMRANGEBYSCORE(ctx, test.command, mockServer, nil)
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
			t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
		}
		// Check if the expected values are the same
		if test.expectedValues != nil {
			for key, expectedSet := range test.expectedValues {
				if _, err = mockServer.KeyRLock(ctx, key); err != nil {
					t.Error(err)
				}
				set, ok := mockServer.GetValue(ctx, key).(*SortedSet)
				if !ok {
					t.Errorf("expected value at key \"%s\" to be a sorted set, got another type", key)
				}
				if !set.Equals(expectedSet) {
					t.Errorf("exptected sorted set %+v, got %+v", expectedSet, set)
				}
			}
		}
	}
}

func Test_HandleZREMRANGEBYRANK(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedValues   map[string]*SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Successfully remove multiple elements within range
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByRankKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			command: []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey1", "0", "5"},
			expectedValues: map[string]*SortedSet{
				"ZremRangeByRankKey1": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			expectedResponse: 6,
			expectedError:    nil,
		},
		{ // 2. Establish boundaries from the end of the set when negative boundaries are provided
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByRankKey2": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			command: []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey2", "-6", "-3"},
			expectedValues: map[string]*SortedSet{
				"ZremRangeByRankKey2": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 2. If key does not exist, return 0
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey3", "2", "4"},
			expectedValues:   nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error key is not a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByRankKey3": "Default value",
			},
			command:       []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey3", "4", "4"},
			expectedError: errors.New("value at ZremRangeByRankKey3 is not a sorted set"),
		},
		{ // 4. Command too short
			preset:        false,
			command:       []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey4", "3"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Return error when start index is out of bounds
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByRankKey5": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			command:          []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey5", "-12", "5"},
			expectedValues:   nil,
			expectedResponse: 0,
			expectedError:    errors.New("indices out of bounds"),
		},
		{ // 6. Return error when end index is out of bounds
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByRankKey6": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
				}),
			},
			command:          []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey6", "0", "11"},
			expectedValues:   nil,
			expectedResponse: 0,
			expectedError:    errors.New("indices out of bounds"),
		},
		{ // 7. Command too long
			preset:        false,
			command:       []string{"ZREMRANGEBYRANK", "ZremRangeByRankKey7", "4", "5", "8"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZREMRANGEBYRANK, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZREMRANGEBYRANK(ctx, test.command, mockServer, nil)
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
			t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
		}
		// Check if the expected values are the same
		if test.expectedValues != nil {
			for key, expectedSet := range test.expectedValues {
				if _, err = mockServer.KeyRLock(ctx, key); err != nil {
					t.Error(err)
				}
				set, ok := mockServer.GetValue(ctx, key).(*SortedSet)
				if !ok {
					t.Errorf("expected value at key \"%s\" to be a sorted set, got another type", key)
				}
				if !set.Equals(expectedSet) {
					t.Errorf("exptected sorted set %+v, got %+v", expectedSet, set)
				}
			}
		}
	}
}

func Test_HandleZREMRANGEBYLEX(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedValues   map[string]*SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Successfully remove multiple elements with scores inside the provided range
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByLexKey1": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 1},
					{value: "c", score: 1}, {value: "d", score: 1},
					{value: "e", score: 1}, {value: "f", score: 1},
					{value: "g", score: 1}, {value: "h", score: 1},
					{value: "i", score: 1}, {value: "j", score: 1},
				}),
			},
			command: []string{"ZREMRANGEBYLEX", "ZremRangeByLexKey1", "a", "d"},
			expectedValues: map[string]*SortedSet{
				"ZremRangeByLexKey1": NewSortedSet([]MemberParam{
					{value: "e", score: 1}, {value: "f", score: 1},
					{value: "g", score: 1}, {value: "h", score: 1},
					{value: "i", score: 1}, {value: "j", score: 1},
				}),
			},
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 2. Return 0 if the members do not have the same score
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByLexKey2": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 2},
					{value: "c", score: 3}, {value: "d", score: 4},
					{value: "e", score: 5}, {value: "f", score: 6},
					{value: "g", score: 7}, {value: "h", score: 8},
					{value: "i", score: 9}, {value: "j", score: 10},
				}),
			},
			command: []string{"ZREMRANGEBYLEX", "ZremRangeByLexKey2", "d", "g"},
			expectedValues: map[string]*SortedSet{
				"ZremRangeByLexKey2": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 2},
					{value: "c", score: 3}, {value: "d", score: 4},
					{value: "e", score: 5}, {value: "f", score: 6},
					{value: "g", score: 7}, {value: "h", score: 8},
					{value: "i", score: 9}, {value: "j", score: 10},
				}),
			},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. If key does not exist, return 0
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZREMRANGEBYLEX", "ZremRangeByLexKey3", "2", "4"},
			expectedValues:   nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error key is not a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZremRangeByLexKey3": "Default value",
			},
			command:       []string{"ZREMRANGEBYLEX", "ZremRangeByLexKey3", "a", "d"},
			expectedError: errors.New("value at ZremRangeByLexKey3 is not a sorted set"),
		},
		{ // 4. Command too short
			preset:        false,
			command:       []string{"ZREMRANGEBYLEX", "ZremRangeByLexKey4", "a"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Command too long
			preset:        false,
			command:       []string{"ZREMRANGEBYLEX", "ZremRangeByLexKey5", "a", "b", "c"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZREMRANGEBYLEX, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZREMRANGEBYLEX(ctx, test.command, mockServer, nil)
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
			t.Errorf("expected response %d, got %d", test.expectedResponse, rv.Integer())
		}
		// Check if the expected values are the same
		if test.expectedValues != nil {
			for key, expectedSet := range test.expectedValues {
				if _, err = mockServer.KeyRLock(ctx, key); err != nil {
					t.Error(err)
				}
				set, ok := mockServer.GetValue(ctx, key).(*SortedSet)
				if !ok {
					t.Errorf("expected value at key \"%s\" to be a sorted set, got another type", key)
				}
				if !set.Equals(expectedSet) {
					t.Errorf("exptected sorted set %+v, got %+v", expectedSet, set)
				}
			}
		}
	}
}

func Test_HandleZRANGE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse [][]string
		expectedError    error
	}{
		{ // 1. Get elements withing score range without score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			command:          []string{"ZRANGE", "ZrangeKey1", "3", "7", "BYSCORE"},
			expectedResponse: [][]string{{"three"}, {"four"}, {"five"}, {"six"}, {"seven"}},
			expectedError:    nil,
		},
		{ // 2. Get elements within score range with score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey2": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			command: []string{"ZRANGE", "ZrangeKey2", "3", "7", "BYSCORE", "WITHSCORES"},
			expectedResponse: [][]string{
				{"three", "3"}, {"four", "4"}, {"five", "5"},
				{"six", "6"}, {"seven", "7"}},
			expectedError: nil,
		},
		{ // 3. Get elements within score range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			command:          []string{"ZRANGE", "ZrangeKey3", "3", "7", "BYSCORE", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: [][]string{{"three", "3"}, {"four", "4"}, {"five", "5"}},
			expectedError:    nil,
		},
		{ // 4. Get elements within score range with offset and limit + reverse the results.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			// REV reverses the original set before getting the range.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			command:          []string{"ZRANGE", "ZrangeKey4", "3", "7", "BYSCORE", "WITHSCORES", "LIMIT", "2", "4", "REV"},
			expectedResponse: [][]string{{"six", "6"}, {"five", "5"}, {"four", "4"}},
			expectedError:    nil,
		},
		{ // 5. Get elements within lex range without score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey5": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "e", score: 1},
					{value: "b", score: 1}, {value: "f", score: 1},
					{value: "c", score: 1}, {value: "g", score: 1},
					{value: "d", score: 1}, {value: "h", score: 1},
				}),
			},
			command:          []string{"ZRANGE", "ZrangeKey5", "c", "g", "BYLEX"},
			expectedResponse: [][]string{{"c"}, {"d"}, {"e"}, {"f"}, {"g"}},
			expectedError:    nil,
		},
		{ // 6. Get elements within lex range with score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey6": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "e", score: 1},
					{value: "b", score: 1}, {value: "f", score: 1},
					{value: "c", score: 1}, {value: "g", score: 1},
					{value: "d", score: 1}, {value: "h", score: 1},
				}),
			},
			command: []string{"ZRANGE", "ZrangeKey6", "a", "f", "BYLEX", "WITHSCORES"},
			expectedResponse: [][]string{
				{"a", "1"}, {"b", "1"}, {"c", "1"},
				{"d", "1"}, {"e", "1"}, {"f", "1"}},
			expectedError: nil,
		},
		{ // 7. Get elements within lex range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey7": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 1},
					{value: "c", score: 1}, {value: "d", score: 1},
					{value: "e", score: 1}, {value: "f", score: 1},
					{value: "g", score: 1}, {value: "h", score: 1},
				}),
			},
			command:          []string{"ZRANGE", "ZrangeKey7", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: [][]string{{"c", "1"}, {"d", "1"}, {"e", "1"}},
			expectedError:    nil,
		},
		{ // 8. Get elements within lex range with offset and limit + reverse the results.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			// REV reverses the original set before getting the range.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey8": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 1},
					{value: "c", score: 1}, {value: "d", score: 1},
					{value: "e", score: 1}, {value: "f", score: 1},
					{value: "g", score: 1}, {value: "h", score: 1},
				}),
			},
			command:          []string{"ZRANGE", "ZrangeKey8", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4", "REV"},
			expectedResponse: [][]string{{"f", "1"}, {"e", "1"}, {"d", "1"}},
			expectedError:    nil,
		},
		{ // 9. Return an empty slice when we use BYLEX while elements have different scores
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey9": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 5},
					{value: "c", score: 2}, {value: "d", score: 6},
					{value: "e", score: 3}, {value: "f", score: 7},
					{value: "g", score: 4}, {value: "h", score: 8},
				}),
			},
			command:          []string{"ZRANGE", "ZrangeKey9", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: [][]string{},
			expectedError:    nil,
		},
		{ // 10. Throw error when limit does not provide both offset and limit
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGE", "ZrangeKey10", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2"},
			expectedResponse: [][]string{},
			expectedError:    errors.New("limit should contain offset and count as integers"),
		},
		{ // 11. Throw error when offset is not a valid integer
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGE", "ZrangeKey11", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "offset", "4"},
			expectedResponse: [][]string{},
			expectedError:    errors.New("limit offset must be integer"),
		},
		{ // 12. Throw error when limit is not a valid integer
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGE", "ZrangeKey12", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "4", "limit"},
			expectedResponse: [][]string{},
			expectedError:    errors.New("limit count must be integer"),
		},
		{ // 13. Throw error when offset is negative
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGE", "ZrangeKey13", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "-4", "9"},
			expectedResponse: [][]string{},
			expectedError:    errors.New("limit offset must be >= 0"),
		},
		{ // 14. Throw error when the key does not hold a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeKey14": "Default value",
			},
			command:          []string{"ZRANGE", "ZrangeKey14", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: [][]string{},
			expectedError:    errors.New("value at ZrangeKey14 is not a sorted set"),
		},
		{ // 15. Command too short
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGE", "ZrangeKey15", "1"},
			expectedResponse: [][]string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 16 Command too long
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGE", "ZrangeKey16", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "-4", "9", "REV", "WITHSCORES"},
			expectedResponse: [][]string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZRANGE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZRANGE(ctx, test.command, mockServer, nil)
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
		for _, element := range rv.Array() {
			if !slices.ContainsFunc(test.expectedResponse, func(expected []string) bool {
				// The current sub-slice is a different length, return false because they're not equal
				if len(element.Array()) != len(expected) {
					return false
				}
				for i := 0; i < len(expected); i++ {
					if element.Array()[i].String() != expected[i] {
						return false
					}
				}
				return true
			}) {
				t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
			}
		}
	}
}

func Test_HandleZRANGESTORE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		destination      string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get elements withing score range without score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination:      "ZrangeStoreDestinationKey1",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey1", "ZrangeStoreKey1", "3", "7", "BYSCORE"},
			expectedResponse: 5,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "three", score: 3}, {value: "four", score: 4}, {value: "five", score: 5},
				{value: "six", score: 6}, {value: "seven", score: 7},
			}),
			expectedError: nil,
		},
		{ // 2. Get elements within score range with score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey2": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination:      "ZrangeStoreDestinationKey2",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey2", "ZrangeStoreKey2", "3", "7", "BYSCORE", "WITHSCORES"},
			expectedResponse: 5,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "three", score: 3}, {value: "four", score: 4}, {value: "five", score: 5},
				{value: "six", score: 6}, {value: "seven", score: 7},
			}),
			expectedError: nil,
		},
		{ // 3. Get elements within score range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination:      "ZrangeStoreDestinationKey3",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey3", "ZrangeStoreKey3", "3", "7", "BYSCORE", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: 3,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "three", score: 3}, {value: "four", score: 4}, {value: "five", score: 5},
			}),
			expectedError: nil,
		},
		{ // 4. Get elements within score range with offset and limit + reverse the results.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			// REV reverses the original set before getting the range.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination:      "ZrangeStoreDestinationKey4",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey4", "ZrangeStoreKey4", "3", "7", "BYSCORE", "WITHSCORES", "LIMIT", "2", "4", "REV"},
			expectedResponse: 3,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "six", score: 6}, {value: "five", score: 5}, {value: "four", score: 4},
			}),
			expectedError: nil,
		},
		{ // 5. Get elements within lex range without score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey5": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "e", score: 1},
					{value: "b", score: 1}, {value: "f", score: 1},
					{value: "c", score: 1}, {value: "g", score: 1},
					{value: "d", score: 1}, {value: "h", score: 1},
				}),
			},
			destination:      "ZrangeStoreDestinationKey5",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey5", "ZrangeStoreKey5", "c", "g", "BYLEX"},
			expectedResponse: 5,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "c", score: 1}, {value: "d", score: 1}, {value: "e", score: 1},
				{value: "f", score: 1}, {value: "g", score: 1},
			}),
			expectedError: nil,
		},
		{ // 6. Get elements within lex range with score.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey6": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "e", score: 1},
					{value: "b", score: 1}, {value: "f", score: 1},
					{value: "c", score: 1}, {value: "g", score: 1},
					{value: "d", score: 1}, {value: "h", score: 1},
				}),
			},
			destination:      "ZrangeStoreDestinationKey6",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey6", "ZrangeStoreKey6", "a", "f", "BYLEX", "WITHSCORES"},
			expectedResponse: 6,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "a", score: 1}, {value: "b", score: 1}, {value: "c", score: 1},
				{value: "d", score: 1}, {value: "e", score: 1}, {value: "f", score: 1},
			}),
			expectedError: nil,
		},
		{ // 7. Get elements within lex range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey7": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 1},
					{value: "c", score: 1}, {value: "d", score: 1},
					{value: "e", score: 1}, {value: "f", score: 1},
					{value: "g", score: 1}, {value: "h", score: 1},
				}),
			},
			destination:      "ZrangeStoreDestinationKey7",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey7", "ZrangeStoreKey7", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: 3,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "c", score: 1}, {value: "d", score: 1}, {value: "e", score: 1},
			}),
			expectedError: nil,
		},
		{ // 8. Get elements within lex range with offset and limit + reverse the results.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			// REV reverses the original set before getting the range.
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey8": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 1},
					{value: "c", score: 1}, {value: "d", score: 1},
					{value: "e", score: 1}, {value: "f", score: 1},
					{value: "g", score: 1}, {value: "h", score: 1},
				}),
			},
			destination:      "ZrangeStoreDestinationKey8",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey8", "ZrangeStoreKey8", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4", "REV"},
			expectedResponse: 3,
			expectedValue: NewSortedSet([]MemberParam{
				{value: "f", score: 1}, {value: "e", score: 1}, {value: "d", score: 1},
			}),
			expectedError: nil,
		},
		{ // 9. Return an empty slice when we use BYLEX while elements have different scores
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey9": NewSortedSet([]MemberParam{
					{value: "a", score: 1}, {value: "b", score: 5},
					{value: "c", score: 2}, {value: "d", score: 6},
					{value: "e", score: 3}, {value: "f", score: 7},
					{value: "g", score: 4}, {value: "h", score: 8},
				}),
			},
			destination:      "ZrangeStoreDestinationKey9",
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey9", "ZrangeStoreKey9", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: 0,
			expectedValue:    nil,
			expectedError:    nil,
		},
		{ // 10. Throw error when limit does not provide both offset and limit
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey10", "ZrangeStoreKey10", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2"},
			expectedResponse: 0,
			expectedError:    errors.New("limit should contain offset and count as integers"),
		},
		{ // 11. Throw error when offset is not a valid integer
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey11", "ZrangeStoreKey11", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "offset", "4"},
			expectedResponse: 0,
			expectedError:    errors.New("limit offset must be integer"),
		},
		{ // 12. Throw error when limit is not a valid integer
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey12", "ZrangeStoreKey12", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "4", "limit"},
			expectedResponse: 0,
			expectedError:    errors.New("limit count must be integer"),
		},
		{ // 13. Throw error when offset is negative
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey13", "ZrangeStoreKey13", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "-4", "9"},
			expectedResponse: 0,
			expectedError:    errors.New("limit offset must be >= 0"),
		},
		{ // 14. Throw error when the key does not hold a sorted set
			preset: true,
			presetValues: map[string]interface{}{
				"ZrangeStoreKey14": "Default value",
			},
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey14", "ZrangeStoreKey14", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "2", "4"},
			expectedResponse: 0,
			expectedError:    errors.New("value at ZrangeStoreKey14 is not a sorted set"),
		},
		{ // 15. Command too short
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGESTORE", "ZrangeStoreKey15", "1"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 16 Command too long
			preset:           false,
			presetValues:     nil,
			command:          []string{"ZRANGESTORE", "ZrangeStoreDestinationKey16", "ZrangeStoreKey16", "a", "h", "BYLEX", "WITHSCORES", "LIMIT", "-4", "9", "REV", "WITHSCORES"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZRANGESTORE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZRANGESTORE(ctx, test.command, mockServer, nil)
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
			if _, err = mockServer.KeyRLock(ctx, test.destination); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, test.destination).(*SortedSet)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			if !set.Equals(test.expectedValue) {
				t.Errorf("expected sorted set %+v, got %+v", test.expectedValue, set)
			}
			mockServer.KeyRUnlock(ctx, test.destination)
		}
	}
}

func Test_HandleZINTER(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse [][]string
		expectedError    error
	}{
		{ // 1. Get the intersection between 2 sorted sets.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
				"ZinterKey2": NewSortedSet([]MemberParam{
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			command:          []string{"ZINTER", "ZinterKey1", "ZinterKey2"},
			expectedResponse: [][]string{{"three"}, {"four"}, {"five"}},
			expectedError:    nil,
		},
		{
			// 2. Get the intersection between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 8},
				}),
				"ZinterKey5": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTER", "ZinterKey3", "ZinterKey4", "ZinterKey5", "WITHSCORES"},
			expectedResponse: [][]string{{"one", "3"}, {"eight", "24"}},
			expectedError:    nil,
		},
		{
			// 3. Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey6": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey7": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterKey8": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTER", "ZinterKey6", "ZinterKey7", "ZinterKey8", "WITHSCORES", "AGGREGATE", "MIN"},
			expectedResponse: [][]string{{"one", "1"}, {"eight", "8"}},
			expectedError:    nil,
		},
		{
			// 4. Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey9": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey10": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterKey11": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTER", "ZinterKey9", "ZinterKey10", "ZinterKey11", "WITHSCORES", "AGGREGATE", "MAX"},
			expectedResponse: [][]string{{"one", "1000"}, {"eight", "800"}},
			expectedError:    nil,
		},
		{
			// 5. Get the intersection between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey12": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey13": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterKey14": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTER", "ZinterKey12", "ZinterKey13", "ZinterKey14", "WITHSCORES", "AGGREGATE", "SUM", "WEIGHTS", "1", "5", "3"},
			expectedResponse: [][]string{{"one", "3105"}, {"eight", "2808"}},
			expectedError:    nil,
		},
		{
			// 6. Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey15": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey16": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterKey17": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTER", "ZinterKey15", "ZinterKey16", "ZinterKey17", "WITHSCORES", "AGGREGATE", "MAX", "WEIGHTS", "1", "5", "3"},
			expectedResponse: [][]string{{"one", "3000"}, {"eight", "2400"}},
			expectedError:    nil,
		},
		{
			// 7. Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey18": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey19": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterKey20": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTER", "ZinterKey18", "ZinterKey19", "ZinterKey20", "WITHSCORES", "AGGREGATE", "MIN", "WEIGHTS", "1", "5", "3"},
			expectedResponse: [][]string{{"one", "5"}, {"eight", "8"}},
			expectedError:    nil,
		},
		{ // 8. Throw an error if there are more weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey21": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey22": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTER", "ZinterKey21", "ZinterKey22", "WEIGHTS", "1", "2", "3"},
			expectedResponse: nil,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 9. Throw an error if there are fewer weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey23": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey24": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
				}),
				"ZinterKey25": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTER", "ZinterKey23", "ZinterKey24", "ZinterKey25", "WEIGHTS", "5", "4"},
			expectedResponse: nil,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 10. Throw an error if there are no keys provided
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey26": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZinterKey27": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZinterKey28": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTER", "WEIGHTS", "5", "4"},
			expectedResponse: nil,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 11. Throw an error if any of the provided keys are not sorted sets
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey29": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterKey30": "Default value",
				"ZinterKey31": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTER", "ZinterKey29", "ZinterKey30", "ZinterKey31"},
			expectedResponse: nil,
			expectedError:    errors.New("value at ZinterKey30 is not a sorted set"),
		},
		{ // 12. If any of the keys does not exist, return an empty array.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterKey32": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZinterKey33": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTER", "non-existent", "ZinterKey32", "ZinterKey33"},
			expectedResponse: [][]string{},
			expectedError:    nil,
		},
		{ // 13. Command too short
			preset:           false,
			command:          []string{"ZINTER"},
			expectedResponse: [][]string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZINTER, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZINTER(ctx, test.command, mockServer, nil)
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
		for _, element := range rv.Array() {
			if !slices.ContainsFunc(test.expectedResponse, func(expected []string) bool {
				// The current sub-slice is a different length, return false because they're not equal
				if len(element.Array()) != len(expected) {
					return false
				}
				for i := 0; i < len(expected); i++ {
					if element.Array()[i].String() != expected[i] {
						return false
					}
				}
				return true
			}) {
				t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
			}
		}
	}
}

func Test_HandleZINTERSTORE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		destination      string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get the intersection between 2 sorted sets.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
				"ZinterStoreKey2": NewSortedSet([]MemberParam{
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination: "ZinterStoreDestinationKey1",
			command:     []string{"ZINTERSTORE", "ZinterStoreDestinationKey1", "ZinterStoreKey1", "ZinterStoreKey2"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5},
			}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{
			// 2. Get the intersection between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey5": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZinterStoreDestinationKey2",
			command:     []string{"ZINTERSTORE", "ZinterStoreDestinationKey2", "ZinterStoreKey3", "ZinterStoreKey4", "ZinterStoreKey5", "WITHSCORES"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "eight", score: 24},
			}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{
			// 3. Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey6": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey7": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterStoreKey8": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZinterStoreDestinationKey3",
			command:     []string{"ZINTERSTORE", "ZinterStoreDestinationKey3", "ZinterStoreKey6", "ZinterStoreKey7", "ZinterStoreKey8", "WITHSCORES", "AGGREGATE", "MIN"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "eight", score: 8},
			}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{
			// 4. Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey9": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey10": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterStoreKey11": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZinterStoreDestinationKey4",
			command:     []string{"ZINTERSTORE", "ZinterStoreDestinationKey4", "ZinterStoreKey9", "ZinterStoreKey10", "ZinterStoreKey11", "WITHSCORES", "AGGREGATE", "MAX"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1000}, {value: "eight", score: 800},
			}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{
			// 5. Get the intersection between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey12": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey13": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterStoreKey14": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZinterStoreDestinationKey5",
			command:     []string{"ZINTERSTORE", "ZinterStoreDestinationKey5", "ZinterStoreKey12", "ZinterStoreKey13", "ZinterStoreKey14", "WITHSCORES", "AGGREGATE", "SUM", "WEIGHTS", "1", "5", "3"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "eight", score: 2808},
			}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{
			// 6. Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey15": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey16": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterStoreKey17": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZinterStoreDestinationKey6",
			command:     []string{"ZINTERSTORE", "ZinterStoreDestinationKey6", "ZinterStoreKey15", "ZinterStoreKey16", "ZinterStoreKey17", "WITHSCORES", "AGGREGATE", "MAX", "WEIGHTS", "1", "5", "3"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 3000}, {value: "eight", score: 2400},
			}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{
			// 7. Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey18": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey19": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZinterStoreKey20": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZinterStoreDestinationKey7",
			command:     []string{"ZINTERSTORE", "ZinterStoreDestinationKey7", "ZinterStoreKey18", "ZinterStoreKey19", "ZinterStoreKey20", "WITHSCORES", "AGGREGATE", "MIN", "WEIGHTS", "1", "5", "3"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 5}, {value: "eight", score: 8},
			}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 8. Throw an error if there are more weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey21": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey22": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTERSTORE", "ZinterStoreDestinationKey8", "ZinterStoreKey21", "ZinterStoreKey22", "WEIGHTS", "1", "2", "3"},
			expectedResponse: 0,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 9. Throw an error if there are fewer weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey23": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey24": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
				}),
				"ZinterStoreKey25": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTERSTORE", "ZinterStoreDestinationKey9", "ZinterStoreKey23", "ZinterStoreKey24", "ZinterStoreKey25", "WEIGHTS", "5", "4"},
			expectedResponse: 0,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 10. Throw an error if there are no keys provided
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey26": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZinterStoreKey27": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZinterStoreKey28": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTERSTORE", "WEIGHTS", "5", "4"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 11. Throw an error if any of the provided keys are not sorted sets
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey29": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZinterStoreKey30": "Default value",
				"ZinterStoreKey31": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZINTERSTORE", "ZinterStoreKey29", "ZinterStoreKey30", "ZinterStoreKey31"},
			expectedResponse: 0,
			expectedError:    errors.New("value at ZinterStoreKey30 is not a sorted set"),
		},
		{ // 12. If any of the keys does not exist, return an empty array.
			preset: true,
			presetValues: map[string]interface{}{
				"ZinterStoreKey32": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZinterStoreKey33": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command:          []string{"ZINTERSTORE", "ZinterStoreDestinationKey12", "non-existent", "ZinterStoreKey32", "ZinterStoreKey33"},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 13. Command too short
			preset:           false,
			command:          []string{"ZINTERSTORE"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZINTERSTORE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZINTERSTORE(ctx, test.command, mockServer, nil)
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
			if _, err = mockServer.KeyRLock(ctx, test.destination); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, test.destination).(*SortedSet)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem.value) {
					t.Errorf("could not find element %s in the expected values", elem.value)
				}
			}
			mockServer.KeyRUnlock(ctx, test.destination)
		}
	}
}

func Test_HandleZUNION(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse [][]string
		expectedError    error
	}{
		{ // 1. Get the union between 2 sorted sets.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
				"ZunionKey2": NewSortedSet([]MemberParam{
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			command:          []string{"ZUNION", "ZunionKey1", "ZunionKey2"},
			expectedResponse: [][]string{{"one"}, {"two"}, {"three"}, {"four"}, {"five"}, {"six"}, {"seven"}, {"eight"}},
			expectedError:    nil,
		},
		{
			// 2. Get the union between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 8},
				}),
				"ZunionKey5": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12}, {value: "thirty-six", score: 36},
				}),
			},
			command: []string{"ZUNION", "ZunionKey3", "ZunionKey4", "ZunionKey5", "WITHSCORES"},
			expectedResponse: [][]string{
				{"one", "3"}, {"two", "4"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"},
				{"seven", "7"}, {"eight", "24"}, {"nine", "9"}, {"ten", "10"}, {"eleven", "11"},
				{"twelve", "24"}, {"thirty-six", "72"},
			},
			expectedError: nil,
		},
		{
			// 3. Get the union between 3 sorted sets with scores.
			// Use MIN aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey6": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey7": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionKey8": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12}, {value: "thirty-six", score: 72},
				}),
			},
			command: []string{"ZUNION", "ZunionKey6", "ZunionKey7", "ZunionKey8", "WITHSCORES", "AGGREGATE", "MIN"},
			expectedResponse: [][]string{
				{"one", "1"}, {"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"},
				{"seven", "7"}, {"eight", "8"}, {"nine", "9"}, {"ten", "10"}, {"eleven", "11"},
				{"twelve", "12"}, {"thirty-six", "36"},
			},
			expectedError: nil,
		},
		{
			// 4. Get the union between 3 sorted sets with scores.
			// Use MAX aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey9": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey10": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionKey11": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12}, {value: "thirty-six", score: 72},
				}),
			},
			command: []string{"ZUNION", "ZunionKey9", "ZunionKey10", "ZunionKey11", "WITHSCORES", "AGGREGATE", "MAX"},
			expectedResponse: [][]string{
				{"one", "1000"}, {"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"},
				{"seven", "7"}, {"eight", "800"}, {"nine", "9"}, {"ten", "10"}, {"eleven", "11"},
				{"twelve", "12"}, {"thirty-six", "72"},
			},
			expectedError: nil,
		},
		{
			// 5. Get the union between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey12": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey13": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionKey14": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command: []string{"ZUNION", "ZunionKey12", "ZunionKey13", "ZunionKey14", "WITHSCORES", "AGGREGATE", "SUM", "WEIGHTS", "1", "2", "3"},
			expectedResponse: [][]string{
				{"one", "3102"}, {"two", "6"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"},
				{"seven", "7"}, {"eight", "2568"}, {"nine", "27"}, {"ten", "30"}, {"eleven", "22"},
				{"twelve", "60"}, {"thirty-six", "72"},
			},
			expectedError: nil,
		},
		{
			// 6. Get the union between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey15": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey16": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionKey17": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command: []string{"ZUNION", "ZunionKey15", "ZunionKey16", "ZunionKey17", "WITHSCORES", "AGGREGATE", "MAX", "WEIGHTS", "1", "2", "3"},
			expectedResponse: [][]string{
				{"one", "3000"}, {"two", "4"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"},
				{"seven", "7"}, {"eight", "2400"}, {"nine", "27"}, {"ten", "30"}, {"eleven", "22"},
				{"twelve", "36"}, {"thirty-six", "72"},
			},
			expectedError: nil,
		},
		{
			// 7. Get the union between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey18": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey19": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionKey20": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command: []string{"ZUNION", "ZunionKey18", "ZunionKey19", "ZunionKey20", "WITHSCORES", "AGGREGATE", "MIN", "WEIGHTS", "1", "2", "3"},
			expectedResponse: [][]string{
				{"one", "2"}, {"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}, {"seven", "7"},
				{"eight", "8"}, {"nine", "27"}, {"ten", "30"}, {"eleven", "22"}, {"twelve", "24"}, {"thirty-six", "72"},
			},
			expectedError: nil,
		},
		{ // 8. Throw an error if there are more weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey21": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey22": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZUNION", "ZunionKey21", "ZunionKey22", "WEIGHTS", "1", "2", "3"},
			expectedResponse: nil,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 9. Throw an error if there are fewer weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey23": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey24": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
				}),
				"ZunionKey25": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZUNION", "ZunionKey23", "ZunionKey24", "ZunionKey25", "WEIGHTS", "5", "4"},
			expectedResponse: nil,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 10. Throw an error if there are no keys provided
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey26": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZunionKey27": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZunionKey28": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZUNION", "WEIGHTS", "5", "4"},
			expectedResponse: nil,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 11. Throw an error if any of the provided keys are not sorted sets
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey29": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionKey30": "Default value",
				"ZunionKey31": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZUNION", "ZunionKey29", "ZunionKey30", "ZunionKey31"},
			expectedResponse: nil,
			expectedError:    errors.New("value at ZunionKey30 is not a sorted set"),
		},
		{ // 12. If any of the keys does not exist, skip it.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionKey32": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZunionKey33": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			command: []string{"ZUNION", "non-existent", "ZunionKey32", "ZunionKey33"},
			expectedResponse: [][]string{
				{"one"}, {"two"}, {"thirty-six"}, {"twelve"}, {"eleven"},
				{"seven"}, {"eight"}, {"nine"}, {"ten"},
			},
			expectedError: nil,
		},
		{ // 13. Command too short
			preset:        false,
			command:       []string{"ZUNION"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZUNION, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZUNION(ctx, test.command, mockServer, nil)
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
		for _, element := range rv.Array() {
			if !slices.ContainsFunc(test.expectedResponse, func(expected []string) bool {
				// The current sub-slice is a different length, return false because they're not equal
				if len(element.Array()) != len(expected) {
					return false
				}
				for i := 0; i < len(expected); i++ {
					if element.Array()[i].String() != expected[i] {
						return false
					}
				}
				return true
			}) {
				t.Errorf("expected response %+v, got %+v", test.expectedResponse, rv.Array())
			}
		}
	}
}

func Test_HandleZUNIONSTORE(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		destination      string
		command          []string
		expectedValue    *SortedSet
		expectedResponse int
		expectedError    error
	}{
		{ // 1. Get the union between 2 sorted sets.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey1": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5},
				}),
				"ZunionStoreKey2": NewSortedSet([]MemberParam{
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
			},
			destination: "ZunionStoreDestinationKey1",
			command:     []string{"ZUNIONSTORE", "ZunionStoreDestinationKey1", "ZunionStoreKey1", "ZunionStoreKey2"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2},
				{value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6},
				{value: "seven", score: 7}, {value: "eight", score: 8},
			}),
			expectedResponse: 8,
			expectedError:    nil,
		},
		{
			// 2. Get the union between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey3": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey4": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey5": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12}, {value: "thirty-six", score: 36},
				}),
			},
			destination: "ZunionStoreDestinationKey2",
			command:     []string{"ZUNIONSTORE", "ZunionStoreDestinationKey2", "ZunionStoreKey3", "ZunionStoreKey4", "ZunionStoreKey5", "WITHSCORES"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 3}, {value: "two", score: 4}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 24},
				{value: "nine", score: 9}, {value: "ten", score: 10}, {value: "eleven", score: 11},
				{value: "twelve", score: 24}, {value: "thirty-six", score: 72},
			}),
			expectedResponse: 13,
			expectedError:    nil,
		},
		{
			// 3. Get the union between 3 sorted sets with scores.
			// Use MIN aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey6": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey7": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionStoreKey8": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12}, {value: "thirty-six", score: 72},
				}),
			},
			destination: "ZunionStoreDestinationKey3",
			command:     []string{"ZUNIONSTORE", "ZunionStoreDestinationKey3", "ZunionStoreKey6", "ZunionStoreKey7", "ZunionStoreKey8", "WITHSCORES", "AGGREGATE", "MIN"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 8},
				{value: "nine", score: 9}, {value: "ten", score: 10}, {value: "eleven", score: 11},
				{value: "twelve", score: 12}, {value: "thirty-six", score: 36},
			}),
			expectedResponse: 13,
			expectedError:    nil,
		},
		{
			// 4. Get the union between 3 sorted sets with scores.
			// Use MAX aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey9": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey10": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionStoreKey11": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12}, {value: "thirty-six", score: 72},
				}),
			},
			destination: "ZunionStoreDestinationKey4",
			command: []string{
				"ZUNIONSTORE", "ZunionStoreDestinationKey4", "ZunionStoreKey9", "ZunionStoreKey10", "ZunionStoreKey11", "WITHSCORES", "AGGREGATE", "MAX",
			},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1000}, {value: "two", score: 2}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 800},
				{value: "nine", score: 9}, {value: "ten", score: 10}, {value: "eleven", score: 11},
				{value: "twelve", score: 12}, {value: "thirty-six", score: 72},
			}),
			expectedResponse: 13,
			expectedError:    nil,
		},
		{
			// 5. Get the union between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey12": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey13": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionStoreKey14": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZunionStoreDestinationKey5",
			command: []string{
				"ZUNIONSTORE", "ZunionStoreDestinationKey5", "ZunionStoreKey12", "ZunionStoreKey13", "ZunionStoreKey14",
				"WITHSCORES", "AGGREGATE", "SUM", "WEIGHTS", "1", "2", "3",
			},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 3102}, {value: "two", score: 6}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 2568},
				{value: "nine", score: 27}, {value: "ten", score: 30}, {value: "eleven", score: 22},
				{value: "twelve", score: 60}, {value: "thirty-six", score: 72},
			}),
			expectedResponse: 13,
			expectedError:    nil,
		},
		{
			// 6. Get the union between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey15": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey16": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionStoreKey17": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZunionStoreDestinationKey6",
			command: []string{
				"ZUNIONSTORE", "ZunionStoreDestinationKey6", "ZunionStoreKey15", "ZunionStoreKey16", "ZunionStoreKey17",
				"WITHSCORES", "AGGREGATE", "MAX", "WEIGHTS", "1", "2", "3"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 3000}, {value: "two", score: 4}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 2400},
				{value: "nine", score: 27}, {value: "ten", score: 30}, {value: "eleven", score: 22},
				{value: "twelve", score: 36}, {value: "thirty-six", score: 72},
			}),
			expectedResponse: 13,
			expectedError:    nil,
		},
		{
			// 7. Get the union between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey18": NewSortedSet([]MemberParam{
					{value: "one", score: 100}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey19": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11}, {value: "eight", score: 80},
				}),
				"ZunionStoreKey20": NewSortedSet([]MemberParam{
					{value: "one", score: 1000}, {value: "eight", score: 800},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZunionStoreDestinationKey7",
			command: []string{
				"ZUNIONSTORE", "ZunionStoreDestinationKey7", "ZunionStoreKey18", "ZunionStoreKey19", "ZunionStoreKey20",
				"WITHSCORES", "AGGREGATE", "MIN", "WEIGHTS", "1", "2", "3",
			},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 2}, {value: "two", score: 2}, {value: "three", score: 3}, {value: "four", score: 4},
				{value: "five", score: 5}, {value: "six", score: 6}, {value: "seven", score: 7}, {value: "eight", score: 8},
				{value: "nine", score: 27}, {value: "ten", score: 30}, {value: "eleven", score: 22},
				{value: "twelve", score: 24}, {value: "thirty-six", score: 72},
			}),
			expectedResponse: 13,
			expectedError:    nil,
		},
		{ // 8. Throw an error if there are more weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey21": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey22": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			destination:      "ZunionStoreDestinationKey8",
			command:          []string{"ZUNIONSTORE", "ZunionStoreDestinationKey8", "ZunionStoreKey21", "ZunionStoreKey22", "WEIGHTS", "1", "2", "3"},
			expectedResponse: 0,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 9. Throw an error if there are fewer weights than keys
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey23": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey24": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
				}),
				"ZunionStoreKey25": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			destination:      "ZunionStoreDestinationKey9",
			command:          []string{"ZUNIONSTORE", "ZunionStoreDestinationKey9", "ZunionStoreKey23", "ZunionStoreKey24", "ZunionStoreKey25", "WEIGHTS", "5", "4"},
			expectedResponse: 0,
			expectedError:    errors.New("number of weights should match number of keys"),
		},
		{ // 10. Throw an error if there are no keys provided
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey26": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZunionStoreKey27": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
				"ZunionStoreKey28": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			command:          []string{"ZUNIONSTORE", "WEIGHTS", "5", "4"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 11. Throw an error if any of the provided keys are not sorted sets
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey29": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "three", score: 3}, {value: "four", score: 4},
					{value: "five", score: 5}, {value: "six", score: 6},
					{value: "seven", score: 7}, {value: "eight", score: 8},
				}),
				"ZunionStoreKey30": "Default value",
				"ZunionStoreKey31": NewSortedSet([]MemberParam{{value: "one", score: 1}}),
			},
			destination:      "ZunionStoreDestinationKey11",
			command:          []string{"ZUNIONSTORE", "ZunionStoreDestinationKey11", "ZunionStoreKey29", "ZunionStoreKey30", "ZunionStoreKey31"},
			expectedResponse: 0,
			expectedError:    errors.New("value at ZunionStoreKey30 is not a sorted set"),
		},
		{ // 12. If any of the keys does not exist, skip it.
			preset: true,
			presetValues: map[string]interface{}{
				"ZunionStoreKey32": NewSortedSet([]MemberParam{
					{value: "one", score: 1}, {value: "two", score: 2},
					{value: "thirty-six", score: 36}, {value: "twelve", score: 12},
					{value: "eleven", score: 11},
				}),
				"ZunionStoreKey33": NewSortedSet([]MemberParam{
					{value: "seven", score: 7}, {value: "eight", score: 8},
					{value: "nine", score: 9}, {value: "ten", score: 10},
					{value: "twelve", score: 12},
				}),
			},
			destination: "ZunionStoreDestinationKey12",
			command:     []string{"ZUNIONSTORE", "ZunionStoreDestinationKey12", "non-existent", "ZunionStoreKey32", "ZunionStoreKey33"},
			expectedValue: NewSortedSet([]MemberParam{
				{value: "one", score: 1}, {value: "two", score: 2}, {value: "seven", score: 7}, {value: "eight", score: 8},
				{value: "nine", score: 9}, {value: "ten", score: 10}, {value: "eleven", score: 11}, {value: "twelve", score: 12},
				{value: "thirty-six", score: 36},
			}),
			expectedResponse: 9,
			expectedError:    nil,
		},
		{ // 13. Command too short
			preset:           false,
			command:          []string{"ZUNIONSTORE"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("ZUNIONSTORE, %d", i))

		if test.preset {
			for key, value := range test.presetValues {
				if _, err := mockServer.CreateKeyAndLock(ctx, key); err != nil {
					t.Error(err)
				}
				if err := mockServer.SetValue(ctx, key, value); err != nil {
					t.Error(err)
				}
				mockServer.KeyUnlock(ctx, key)
			}
		}
		res, err := handleZUNIONSTORE(ctx, test.command, mockServer, nil)
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
			if _, err = mockServer.KeyRLock(ctx, test.destination); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, test.destination).(*SortedSet)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem.value) {
					t.Errorf("could not find element %s in the expected values", elem.value)
				}
			}
			mockServer.KeyRUnlock(ctx, test.destination)
		}
	}
}
