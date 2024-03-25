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

package set

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/echovault"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"slices"
	"testing"
)

var mockServer *echovault.EchoVault

func init() {
	mockServer = echovault.NewEchoVault(
		echovault.WithConfig(utils.Config{
			DataDir:        "",
			EvictionPolicy: utils.NoEviction,
		}),
	)
}

func Test_HandleSADD(t *testing.T) {
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
			key:              "SaddKey1",
			command:          []string{"SADD", "SaddKey1", "one", "two", "three", "four"},
			expectedValue:    NewSet([]string{"one", "two", "three", "four"}),
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 2. Add members to an exiting set, skip members that already exist in the set, return added count.
			preset:           true,
			presetValue:      NewSet([]string{"one", "two", "three", "four"}),
			key:              "SaddKey2",
			command:          []string{"SADD", "SaddKey2", "three", "four", "five", "six", "seven"},
			expectedValue:    NewSet([]string{"one", "two", "three", "four", "five", "six", "seven"}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to add to a key that does not hold a set
			preset:           true,
			presetValue:      "Default value",
			key:              "SaddKey3",
			command:          []string{"SADD", "SaddKey3", "member"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key SaddKey3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "SaddKey4",
			command:          []string{"SADD", "SaddKey4"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SADD, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSADD(ctx, test.command, mockServer, nil)
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
		if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockServer.GetValue(ctx, test.key).(*Set)
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
		mockServer.KeyRUnlock(ctx, test.key)
	}
}

func Test_HandleSCARD(t *testing.T) {
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
			key:              "ScardKey1",
			command:          []string{"SCARD", "ScardKey1"},
			expectedValue:    nil,
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 2. Return 0 when trying to get cardinality on non-existent key
			preset:           false,
			presetValue:      nil,
			key:              "ScardKey2",
			command:          []string{"SCARD", "ScardKey2"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to get cardinality of a value that is not a set
			preset:           true,
			presetValue:      "Default value",
			key:              "ScardKey3",
			command:          []string{"SCARD", "ScardKey3"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key ScardKey3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "ScardKey4",
			command:          []string{"SCARD"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Command too long
			preset:           false,
			key:              "ScardKey5",
			command:          []string{"SCARD", "ScardKey5", "ScardKey5"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SCARD, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSCARD(ctx, test.command, mockServer, nil)
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
				"SdiffKey1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"SdiffKey2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			command:          []string{"SDIFF", "SdiffKey1", "SdiffKey2"},
			expectedResponse: []string{"one", "two"},
			expectedError:    nil,
		},
		{ // 2. Get the difference between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"SdiffKey3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SdiffKey4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SdiffKey5": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFF", "SdiffKey3", "SdiffKey4", "SdiffKey5"},
			expectedResponse: []string{"three", "four", "five", "six"},
			expectedError:    nil,
		},
		{ // 3. Return base set element if base set is the only valid set
			preset: true,
			presetValues: map[string]interface{}{
				"SdiffKey6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SdiffKey7": "Default value",
				"SdiffKey8": 123456789,
			},
			command:          []string{"SDIFF", "SdiffKey6", "SdiffKey7", "SdiffKey8"},
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"SdiffKey9":  "Default value",
				"SdiffKey10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SdiffKey11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFF", "SdiffKey9", "SdiffKey10", "SdiffKey11"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key SdiffKey9 is not a set"),
		},
		{ // 5. Throw error when base set is non-existent.
			preset: true,
			presetValues: map[string]interface{}{
				"SdiffKey12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SdiffKey13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFF", "non-existent", "SdiffKey7", "SdiffKey8"},
			expectedResponse: nil,
			expectedError:    errors.New("key for base set \"non-existent\" does not exist"),
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SDIFF"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SDIFF, %d", i))

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
		res, err := handleSDIFF(ctx, test.command, mockServer, nil)
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
				"SdiffStoreKey1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"SdiffStoreKey2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination:      "SdiffStoreDestination1",
			command:          []string{"SDIFFSTORE", "SdiffStoreDestination1", "SdiffStoreKey1", "SdiffStoreKey2"},
			expectedValue:    NewSet([]string{"one", "two"}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 2. Get the difference between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"SdiffStoreKey3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SdiffStoreKey4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SdiffStoreKey5": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "SdiffStoreDestination2",
			command:          []string{"SDIFFSTORE", "SdiffStoreDestination2", "SdiffStoreKey3", "SdiffStoreKey4", "SdiffStoreKey5"},
			expectedValue:    NewSet([]string{"three", "four", "five", "six"}),
			expectedResponse: 4,
			expectedError:    nil,
		},
		{ // 3. Return base set element if base set is the only valid set
			preset: true,
			presetValues: map[string]interface{}{
				"SdiffStoreKey6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SdiffStoreKey7": "Default value",
				"SdiffStoreKey8": 123456789,
			},
			destination:      "SdiffStoreDestination3",
			command:          []string{"SDIFFSTORE", "SdiffStoreDestination3", "SdiffStoreKey6", "SdiffStoreKey7", "SdiffStoreKey8"},
			expectedValue:    NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			expectedResponse: 8,
			expectedError:    nil,
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"SdiffStoreKey9":  "Default value",
				"SdiffStoreKey10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SdiffStoreKey11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "SdiffStoreDestination4",
			command:          []string{"SDIFFSTORE", "SdiffStoreDestination4", "SdiffStoreKey9", "SdiffStoreKey10", "SdiffStoreKey11"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key SdiffStoreKey9 is not a set"),
		},
		{ // 5. Throw error when base set is non-existent.
			preset:      true,
			destination: "SdiffStoreDestination5",
			presetValues: map[string]interface{}{
				"SdiffStoreKey12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SdiffStoreKey13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SDIFFSTORE", "SdiffStoreDestination5", "non-existent", "SdiffStoreKey7", "SdiffStoreKey8"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("key for base set \"non-existent\" does not exist"),
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SDIFFSTORE", "SdiffStoreDestination6"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SDIFFSTORE, %d", i))

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
		res, err := handleSDIFFSTORE(ctx, test.command, mockServer, nil)
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
			set, ok := mockServer.GetValue(ctx, test.destination).(*Set)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem) {
					t.Errorf("could not find element %s in the expected values", elem)
				}
			}
			mockServer.KeyRUnlock(ctx, test.destination)
		}
	}
}

func Test_HandleSINTER(t *testing.T) {
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
				"SinterKey1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"SinterKey2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			command:          []string{"SINTER", "SinterKey1", "SinterKey2"},
			expectedResponse: []string{"three", "four", "five"},
			expectedError:    nil,
		},
		{ // 2. Get the intersection between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"SinterKey3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SinterKey4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"SinterKey5": NewSet([]string{"one", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTER", "SinterKey3", "SinterKey4", "SinterKey5"},
			expectedResponse: []string{"one", "eight"},
			expectedError:    nil,
		},
		{ // 3. Throw an error if any of the provided keys are not sets
			preset: true,
			presetValues: map[string]interface{}{
				"SinterKey6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SinterKey7": "Default value",
				"SinterKey8": NewSet([]string{"one"}),
			},
			command:          []string{"SINTER", "SinterKey6", "SinterKey7", "SinterKey8"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key SinterKey7 is not a set"),
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"SinterKey9":  "Default value",
				"SinterKey10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SinterKey11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTER", "SinterKey9", "SinterKey10", "SinterKey11"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key SinterKey9 is not a set"),
		},
		{ // 5. If any of the keys does not exist, return an empty array.
			preset: true,
			presetValues: map[string]interface{}{
				"SinterKey12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SinterKey13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTER", "non-existent", "SinterKey7", "SinterKey8"},
			expectedResponse: []string{},
			expectedError:    nil,
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SINTER"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SINTER, %d", i))

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
		res, err := handleSINTER(ctx, test.command, mockServer, nil)
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
				"SinterCardKey1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"SinterCardKey2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			command:          []string{"SINTERCARD", "SinterCardKey1", "SinterCardKey2"},
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. Get an intersect cardinality between 2 sets with a limit
			preset: true,
			presetValues: map[string]interface{}{
				"SinterCardKey3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"}),
				"SinterCardKey4": NewSet([]string{"three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"}),
			},
			command:          []string{"SINTERCARD", "SinterCardKey3", "SinterCardKey4", "LIMIT", "3"},
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 3. Get the full intersect cardinality between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"SinterCardKey5": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SinterCardKey6": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"SinterCardKey7": NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERCARD", "SinterCardKey5", "SinterCardKey6", "SinterCardKey7"},
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 4. Get the intersection of 3 sets with a limit
			preset: true,
			presetValues: map[string]interface{}{
				"SinterCardKey8":  NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SinterCardKey9":  NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"SinterCardKey10": NewSet([]string{"one", "two", "seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERCARD", "SinterCardKey8", "SinterCardKey9", "SinterCardKey10", "LIMIT", "2"},
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 5. Return 0 if any of the keys does not exist
			preset: true,
			presetValues: map[string]interface{}{
				"SinterCardKey11": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SinterCardKey12": "Default value",
				"SinterCardKey13": NewSet([]string{"one"}),
			},
			command:          []string{"SINTERCARD", "SinterCardKey11", "SinterCardKey12", "SinterCardKey13", "non-existent"},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 6. Throw error when one of the keys is not a valid set.
			preset: true,
			presetValues: map[string]interface{}{
				"SinterCardKey14": "Default value",
				"SinterCardKey15": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SinterCardKey16": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERCARD", "SinterCardKey14", "SinterCardKey15", "SinterCardKey16"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key SinterCardKey14 is not a set"),
		},
		{ // 7. Command too short
			preset:           false,
			command:          []string{"SINTERCARD"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SINTERCARD, %d", i))

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
		res, err := handleSINTERCARD(ctx, test.command, mockServer, nil)
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
				"SinterStoreKey1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"SinterStoreKey2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination:      "SinterStoreDestination1",
			command:          []string{"SINTERSTORE", "SinterStoreDestination1", "SinterStoreKey1", "SinterStoreKey2"},
			expectedValue:    NewSet([]string{"three", "four", "five"}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. Get the intersection between 3 sets and store it at the destination key.
			preset: true,
			presetValues: map[string]interface{}{
				"SinterStoreKey3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SinterStoreKey4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"SinterStoreKey5": NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "SinterStoreDestination2",
			command:          []string{"SINTERSTORE", "SinterStoreDestination2", "SinterStoreKey3", "SinterStoreKey4", "SinterStoreKey5"},
			expectedValue:    NewSet([]string{"one", "eight"}),
			expectedResponse: 2,
			expectedError:    nil,
		},
		{ // 3. Throw error when any of the keys is not a set
			preset: true,
			presetValues: map[string]interface{}{
				"SinterStoreKey6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SinterStoreKey7": "Default value",
				"SinterStoreKey8": NewSet([]string{"one"}),
			},
			destination:      "SinterStoreDestination3",
			command:          []string{"SINTERSTORE", "SinterStoreDestination3", "SinterStoreKey6", "SinterStoreKey7", "SinterStoreKey8"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key SinterStoreKey7 is not a set"),
		},
		{ // 4. Throw error when base set is not a set.
			preset: true,
			presetValues: map[string]interface{}{
				"SinterStoreKey9":  "Default value",
				"SinterStoreKey10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SinterStoreKey11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination:      "SinterStoreDestination4",
			command:          []string{"SINTERSTORE", "SinterStoreDestination4", "SinterStoreKey9", "SinterStoreKey10", "SinterStoreKey11"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key SinterStoreKey9 is not a set"),
		},
		{ // 5. Return an empty intersection if one of the keys does not exist.
			preset:      true,
			destination: "SinterStoreDestination5",
			presetValues: map[string]interface{}{
				"SinterStoreKey12": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SinterStoreKey13": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SINTERSTORE", "SinterStoreDestination5", "non-existent", "SinterStoreKey7", "SinterStoreKey8"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SINTERSTORE", "SinterStoreDestination6"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SINTERSTORE, %d", i))

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
		res, err := handleSINTERSTORE(ctx, test.command, mockServer, nil)
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
			set, ok := mockServer.GetValue(ctx, test.destination).(*Set)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem) {
					t.Errorf("could not find element %s in the expected values", elem)
				}
			}
			mockServer.KeyRUnlock(ctx, test.destination)
		}
	}
}

func Test_HandleSISMEMBER(t *testing.T) {
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
			key:              "SIsMemberKey1",
			command:          []string{"SISMEMBER", "SIsMemberKey1", "three"},
			expectedResponse: 1,
			expectedError:    nil,
		},
		{ // 2. Return 0 when element is not a member of the set
			preset:           true,
			presetValue:      NewSet([]string{"one", "two", "three", "four"}),
			key:              "SIsMemberKey2",
			command:          []string{"SISMEMBER", "SIsMemberKey2", "five"},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to assert membership when the key does not hold a valid set
			preset:           true,
			presetValue:      "Default value",
			key:              "SIsMemberKey3",
			command:          []string{"SISMEMBER", "SIsMemberKey3", "one"},
			expectedResponse: 0,
			expectedError:    errors.New("value at key SIsMemberKey3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "SIsMemberKey4",
			command:          []string{"SISMEMBER", "SIsMemberKey4"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Command too long
			preset:           false,
			key:              "SIsMemberKey5",
			command:          []string{"SISMEMBER", "SIsMemberKey5", "one", "two", "three"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SISMEMBER, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSISMEMBER(ctx, test.command, mockServer, nil)
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
			key:              "SmembersKey1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five"}),
			command:          []string{"SMEMBERS", "SmembersKey1"},
			expectedResponse: []string{"one", "two", "three", "four", "five"},
			expectedError:    nil,
		},
		{ // 2. If the key does not exist, return an empty array.
			preset:           false,
			key:              "SmembersKey2",
			presetValue:      nil,
			command:          []string{"SMEMBERS", "SmembersKey2"},
			expectedResponse: []string{},
			expectedError:    nil,
		},
		{ // 3. Throw error when the provided key is not a set.
			preset:           true,
			key:              "SmembersKey3",
			presetValue:      "Default value",
			command:          []string{"SMEMBERS", "SmembersKey3"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key SmembersKey3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			command:          []string{"SMEMBERS"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Command too long
			preset:           false,
			command:          []string{"SMEMBERS", "SmembersKey5", "SmembersKey6"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SMEMBERS, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSMEMBERS(ctx, test.command, mockServer, nil)
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
			key:              "SmismemberKey1",
			command:          []string{"SMISMEMBER", "SmismemberKey1", "three", "four", "five", "six", "eight", "nine", "seven"},
			expectedResponse: []int{1, 1, 1, 1, 0, 0, 1},
			expectedError:    nil,
		},
		{ // 2. If the set key does not exist, return an array of zeroes as long as the list of members
			preset:           false,
			presetValue:      nil,
			key:              "SmismemberKey2",
			command:          []string{"SMISMEMBER", "SmismemberKey2", "one", "two", "three", "four"},
			expectedResponse: []int{0, 0, 0, 0},
			expectedError:    nil,
		},
		{ // 3. Throw error when trying to assert membership when the key does not hold a valid set
			preset:           true,
			presetValue:      "Default value",
			key:              "SmismemberKey3",
			command:          []string{"SMISMEMBER", "SmismemberKey3", "one"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key SmismemberKey3 is not a set"),
		},
		{ // 4. Command too short
			preset:           false,
			key:              "SmismemberKey4",
			command:          []string{"SMISMEMBER", "SmismemberKey4"},
			expectedResponse: nil,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SMISMEMBER, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSMISMEMBER(ctx, test.command, mockServer, nil)
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
				"SmoveSource1":      NewSet([]string{"one", "two", "three", "four"}),
				"SmoveDestination1": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			command: []string{"MOVE", "SmoveSource1", "SmoveDestination1", "four"},
			expectedValues: map[string]interface{}{
				"SmoveSource1":      NewSet([]string{"one", "two", "three"}),
				"SmoveDestination1": NewSet([]string{"four", "five", "six", "seven", "eight"}),
			},
			expectedResponse: 1,
			expectedError:    nil,
		},
		{ // 2. Return 0 when trying to move a member from source set to destination set when it doesn't exist in source
			preset: true,
			presetValues: map[string]interface{}{
				"SmoveSource2":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"SmoveDestination2": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			command: []string{"SMOVE", "SmoveSource2", "SmoveDestination2", "six"},
			expectedValues: map[string]interface{}{
				"SmoveSource2":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"SmoveDestination2": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error when the source key is not a set
			preset: true,
			presetValues: map[string]interface{}{
				"SmoveSource3":      "Default value",
				"SmoveDestination3": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			command: []string{"SMOVE", "SmoveSource3", "SmoveDestination3", "five"},
			expectedValues: map[string]interface{}{
				"SmoveSource3":      "Default value",
				"SmoveDestination3": NewSet([]string{"five", "six", "seven", "eight"}),
			},
			expectedResponse: 0,
			expectedError:    errors.New("source is not a set"),
		},
		{ // 4. Return error when the destination key is not a set
			preset: true,
			presetValues: map[string]interface{}{
				"SmoveSource4":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"SmoveDestination4": "Default value",
			},
			command: []string{"SMOVE", "SmoveSource4", "SmoveDestination4", "five"},
			expectedValues: map[string]interface{}{
				"SmoveSource4":      NewSet([]string{"one", "two", "three", "four", "five"}),
				"SmoveDestination4": "Default value",
			},
			expectedResponse: 0,
			expectedError:    errors.New("destination is not a set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"SMOVE", "SmoveSource5", "SmoveSource6"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"SMOVE", "SmoveSource5", "SmoveSource6", "member1", "member2"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SMOVE, %d", i))

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
		res, err := handleSMOVE(ctx, test.command, mockServer, nil)
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
			if _, err = mockServer.KeyRLock(ctx, key); err != nil {
				t.Error(key)
			}
			set, ok := mockServer.GetValue(ctx, key).(*Set)
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
			mockServer.KeyRUnlock(ctx, key)
		}
	}
}

func Test_HandleSPOP(t *testing.T) {
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
			key:              "SpopKey1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			command:          []string{"SPOP", "SpopKey1", "3"},
			expectedValue:    5,
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{ // 2. Return error when the source key is not a set
			preset:           true,
			key:              "SpopKey2",
			presetValue:      "Default value",
			command:          []string{"SPOP", "SpopKey2"},
			expectedValue:    0,
			expectedResponse: []string{},
			expectedError:    errors.New("value at SpopKey2 is not a set"),
		},
		{ // 5. Command too short
			preset:        false,
			command:       []string{"SPOP"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 6. Command too long
			preset:        false,
			command:       []string{"SPOP", "SpopSource5", "SpopSource6", "member1", "member2"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 7. Throw error when count is not an integer
			preset:        false,
			command:       []string{"SPOP", "SpopKey1", "count"},
			expectedError: errors.New("count must be an integer"),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SPOP, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSPOP(ctx, test.command, mockServer, nil)
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
		if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockServer.GetValue(ctx, test.key).(*Set)
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
	tests := []struct {
		preset           bool
		key              string
		presetValue      interface{}
		command          []string
		expectedValue    int // The final cardinality of the resulting set
		allowRepeat      bool
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Return multiple random elements without removing them
			// Count is positive, do not allow repeated elements
			preset:           true,
			key:              "SRandMemberKey1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			command:          []string{"SRANDMEMBER", "SRandMemberKey1", "3"},
			expectedValue:    8,
			allowRepeat:      false,
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{
			// 2. Return multiple random elements without removing them
			// Count is negative, so allow repeated numbers
			preset:           true,
			key:              "SRandMemberKey2",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			command:          []string{"SRANDMEMBER", "SRandMemberKey2", "-5"},
			expectedValue:    8,
			allowRepeat:      true,
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{ // 2. Return error when the source key is not a set
			preset:           true,
			key:              "SRandMemberKey3",
			presetValue:      "Default value",
			command:          []string{"SRANDMEMBER", "SRandMemberKey3"},
			expectedValue:    0,
			expectedResponse: []string{},
			expectedError:    errors.New("value at SRandMemberKey3 is not a set"),
		},
		{ // 3. Command too short
			preset:        false,
			command:       []string{"SRANDMEMBER"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 4. Command too long
			preset:        false,
			command:       []string{"SRANDMEMBER", "SRandMemberSource5", "SRandMemberSource6", "member1", "member2"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
		{ // 5. Throw error when count is not an integer
			preset:        false,
			command:       []string{"SRANDMEMBER", "SRandMemberKey1", "count"},
			expectedError: errors.New("count must be an integer"),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SRANDMEMBER, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSRANDMEMBER(ctx, test.command, mockServer, nil)
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
		if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
			t.Error(err)
		}
		set, ok := mockServer.GetValue(ctx, test.key).(*Set)
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
		// 4. If allowRepeat is false, check that all the elements make a valid set
		if !test.allowRepeat {
			var elems []string
			for _, e := range rv.Array() {
				elems = append(elems, e.String())
			}
			s := NewSet(elems)
			if s.Cardinality() != len(elems) {
				t.Errorf("expected non-repeating elements for random elements at key \"%s\"", test.key)
			}
		}
	}
}

func Test_HandleSREM(t *testing.T) {
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
			key:              "SremKey1",
			presetValue:      NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			command:          []string{"SREM", "SremKey1", "one", "two", "three", "nine"},
			expectedValue:    NewSet([]string{"four", "five", "six", "seven", "eight"}),
			expectedResponse: 3,
			expectedError:    nil,
		},
		{ // 2. If key does not exist, return 0
			preset:           false,
			key:              "SremKey2",
			presetValue:      nil,
			command:          []string{"SREM", "SremKey1", "one", "two", "three", "nine"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    nil,
		},
		{ // 3. Return error when the source key is not a set
			preset:           true,
			key:              "SremKey3",
			presetValue:      "Default value",
			command:          []string{"SREM", "SremKey3", "one"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key SremKey3 is not a set"),
		},
		{ // 4. Command too short
			preset:        false,
			command:       []string{"SREM", "SremKey"},
			expectedError: errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SREM, %d", i))

		if test.preset {
			if _, err := mockServer.CreateKeyAndLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			if err := mockServer.SetValue(ctx, test.key, test.presetValue); err != nil {
				t.Error(err)
			}
			mockServer.KeyUnlock(ctx, test.key)
		}
		res, err := handleSREM(ctx, test.command, mockServer, nil)
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
			if _, err = mockServer.KeyRLock(ctx, test.key); err != nil {
				t.Error(err)
			}
			set, ok := mockServer.GetValue(ctx, test.key).(*Set)
			if !ok {
				t.Errorf("expected value at key \"%s\" to be a set, got another type", test.key)
			}
			for _, element := range set.GetAll() {
				if !test.expectedValue.Contains(element) {
					t.Errorf("element \"%s\" not found in expected set values but found in set", element)
				}
			}
			mockServer.KeyRUnlock(ctx, test.key)
		}
	}
}

func Test_HandleSUNION(t *testing.T) {
	tests := []struct {
		preset           bool
		presetValues     map[string]interface{}
		command          []string
		expectedResponse []string
		expectedError    error
	}{
		{ // 1. Get the union between 2 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"SunionKey1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"SunionKey2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			command:          []string{"SUNION", "SunionKey1", "SunionKey2"},
			expectedResponse: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			expectedError:    nil,
		},
		{ // 2. Get the union between 3 sets.
			preset: true,
			presetValues: map[string]interface{}{
				"SunionKey3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SunionKey4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"SunionKey5": NewSet([]string{"one", "eight", "nine", "ten", "twelve"}),
			},
			command: []string{"SUNION", "SunionKey3", "SunionKey4", "SunionKey5"},
			expectedResponse: []string{
				"one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
				"ten", "eleven", "twelve", "thirty-six",
			},
			expectedError: nil,
		},
		{ // 3. Throw an error if any of the provided keys are not sets
			preset: true,
			presetValues: map[string]interface{}{
				"SunionKey6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SunionKey7": "Default value",
				"SunionKey8": NewSet([]string{"one"}),
			},
			command:          []string{"SUNION", "SunionKey6", "SunionKey7", "SunionKey8"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key SunionKey7 is not a set"),
		},
		{ // 4. Throw error any of the keys does not hold a set.
			preset: true,
			presetValues: map[string]interface{}{
				"SunionKey9":  "Default value",
				"SunionKey10": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"SunionKey11": NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			command:          []string{"SUNION", "SunionKey9", "SunionKey10", "SunionKey11"},
			expectedResponse: nil,
			expectedError:    errors.New("value at key SunionKey9 is not a set"),
		},
		{ // 6. Command too short
			preset:           false,
			command:          []string{"SUNION"},
			expectedResponse: []string{},
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SUNION, %d", i))

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
		res, err := handleSUNION(ctx, test.command, mockServer, nil)
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

func Test_HandleSUNIONSTORE(t *testing.T) {
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
				"SunionStoreKey1": NewSet([]string{"one", "two", "three", "four", "five"}),
				"SunionStoreKey2": NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination:      "SunionStoreDestination1",
			command:          []string{"SUNIONSTORE", "SunionStoreDestination1", "SunionStoreKey1", "SunionStoreKey2"},
			expectedValue:    NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			expectedResponse: 8,
			expectedError:    nil,
		},
		{ // 2. Get the intersection between 3 sets and store it at the destination key.
			preset: true,
			presetValues: map[string]interface{}{
				"SunionStoreKey3": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SunionStoreKey4": NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"SunionStoreKey5": NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			destination: "SunionStoreDestination2",
			command:     []string{"SUNIONSTORE", "SunionStoreDestination2", "SunionStoreKey3", "SunionStoreKey4", "SunionStoreKey5"},
			expectedValue: NewSet([]string{
				"one", "two", "three", "four", "five", "six", "seven", "eight",
				"nine", "ten", "eleven", "twelve", "thirty-six",
			}),
			expectedResponse: 13,
			expectedError:    nil,
		},
		{ // 3. Throw error when any of the keys is not a set
			preset: true,
			presetValues: map[string]interface{}{
				"SunionStoreKey6": NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"SunionStoreKey7": "Default value",
				"SunionStoreKey8": NewSet([]string{"one"}),
			},
			destination:      "SunionStoreDestination3",
			command:          []string{"SUNIONSTORE", "SunionStoreDestination3", "SunionStoreKey6", "SunionStoreKey7", "SunionStoreKey8"},
			expectedValue:    nil,
			expectedResponse: 0,
			expectedError:    errors.New("value at key SunionStoreKey7 is not a set"),
		},
		{ // 5. Command too short
			preset:           false,
			command:          []string{"SUNIONSTORE", "SunionStoreDestination6"},
			expectedResponse: 0,
			expectedError:    errors.New(utils.WrongArgsResponse),
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("SUNIONSTORE, %d", i))

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
		res, err := handleSUNIONSTORE(ctx, test.command, mockServer, nil)
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
			set, ok := mockServer.GetValue(ctx, test.destination).(*Set)
			if !ok {
				t.Errorf("expected vaule at key %s to be set, got another type", test.destination)
			}
			for _, elem := range set.GetAll() {
				if !test.expectedValue.Contains(elem) {
					t.Errorf("could not find element %s in the expected values", elem)
				}
			}
			mockServer.KeyRUnlock(ctx, test.destination)
		}
	}
}
