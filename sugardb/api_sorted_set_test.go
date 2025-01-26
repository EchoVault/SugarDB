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

package sugardb

import (
	"context"
	"github.com/echovault/sugardb/internal"
	ss "github.com/echovault/sugardb/internal/modules/sorted_set"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestSugarDB_SortedSet(t *testing.T) {
	server := createSugarDB()

	t.Cleanup(func() {
		server.ShutDown()
	})

	t.Run("TestSugarDB_ZADD", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue *ss.SortedSet
			key         string
			entries     map[string]float64
			options     ZAddOptions
			want        int
			wantErr     bool
		}{
			{
				name:        "1. Create new sorted set and return the cardinality of the new sorted set",
				preset:      false,
				presetValue: nil,
				key:         "zadd_key1",
				entries: map[string]float64{
					"member1": 5.5,
					"member2": 67.77,
					"member3": 10,
					"member4": math.Inf(-1),
					"member5": math.Inf(1),
				},
				options: ZAddOptions{},
				want:    5,
				wantErr: false,
			},
			{
				name:   "2. Only add the elements that do not currently exist in the sorted set when NX flag is provided",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key2",
				entries: map[string]float64{
					"member1": 5.5,
					"member4": 67.77,
					"member5": 10,
				},
				options: ZAddOptions{NX: true},
				want:    2,
				wantErr: false,
			},
			{
				name:   "3. Do not add any elements when providing existing members with NX flag",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key3",
				entries: map[string]float64{
					"member1": 5.5,
					"member2": 67.77,
					"member3": 10,
				},
				options: ZAddOptions{NX: true},
				want:    0,
				wantErr: false,
			},
			{
				name:   "4. Successfully add elements to an existing set when XX flag is provided with existing elements",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key4",
				entries: map[string]float64{
					"member1": 55,
					"member2": 1005,
					"member3": 15,
					"member4": 99.75,
				},
				options: ZAddOptions{XX: true, CH: true},
				want:    3,
				wantErr: false,
			},
			{
				name:   "5. Fail to add element when providing XX flag with elements that do not exist in the sorted set",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key5",
				entries: map[string]float64{
					"member4": 5.5,
					"member5": 100.5,
					"member6": 15,
				},
				options: ZAddOptions{XX: true},
				want:    0,
				wantErr: false,
			},
			{
				// Only update the elements where provided score is greater than current score if GT flag
				// Return only the new elements added by default
				name:   "6. Only update the elements where provided score is greater than current score if GT flag",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key6",
				entries: map[string]float64{
					"member1": 7.5,
					"member4": 100.5,
					"member5": 15,
				},
				options: ZAddOptions{XX: true, CH: true, GT: true},
				want:    1,
				wantErr: false,
			},
			{
				// Only update the elements where provided score is less than current score if LT flag is provided
				// Return only the new elements added by default.
				name:   "7. Only update the elements where provided score is less than current score if LT flag is provided",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key7",
				entries: map[string]float64{
					"member1": 3.5,
					"member4": 100.5,
					"member5": 15,
				},
				options: ZAddOptions{XX: true, LT: true},
				want:    0,
				wantErr: false,
			},
			{
				name:   "8. Return all the elements that were updated AND added when CH flag is provided",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key8",
				entries: map[string]float64{
					"member1": 3.5,
					"member4": 100.5,
					"member5": 15,
				},
				options: ZAddOptions{XX: true, LT: true, CH: true},
				want:    1,
				wantErr: false,
			},
			{
				name:   "9. Increment the member by score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key: "zadd_key9",
				entries: map[string]float64{
					"member3": 5.5,
				},
				options: ZAddOptions{INCR: true},
				want:    0,
				wantErr: false,
			},
			{
				name:        "10. Fail when GT/LT flag is provided alongside NX flag",
				preset:      false,
				presetValue: nil,
				key:         "zadd_key10",
				entries: map[string]float64{
					"member1": 3.5,
					"member5": 15,
				},
				options: ZAddOptions{NX: true, LT: true, CH: true},
				want:    0,
				wantErr: true,
			},
			{
				name:        "11. Throw error when INCR flag is passed with more than one score/member pair",
				preset:      false,
				presetValue: nil,
				key:         "zadd_key11",
				entries: map[string]float64{
					"member1": 10.5,
					"member2": 12.5,
				},
				options: ZAddOptions{INCR: true},
				want:    0,
				wantErr: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZAdd(tt.key, tt.entries, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZADD() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZADD() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZCARD", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			want        int
			wantErr     bool
		}{
			{
				name:   "1. Get cardinality of valid sorted set",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
				}),
				key:     "zcard_key1",
				want:    3,
				wantErr: false,
			},
			{
				name:        "2. Return 0 when trying to get cardinality from non-existent key",
				preset:      false,
				presetValue: nil,
				key:         "zcard_key2",
				want:        0,
				wantErr:     false,
			},
			{
				name:        "3. Return error when not a sorted set",
				preset:      true,
				presetValue: "Default value",
				key:         "zcard_key3",
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZCard(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZCARD() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZCARD() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZCOUNT", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			min         float64
			max         float64
			want        int
			wantErr     bool
		}{
			{
				name:   "1. Get entire count using infinity boundaries",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
					{Value: "member4", Score: ss.Score(1083.13)},
					{Value: "member5", Score: ss.Score(11)},
					{Value: "member6", Score: ss.Score(math.Inf(-1))},
					{Value: "member7", Score: ss.Score(math.Inf(1))},
				}),
				key:     "zcount_key1",
				min:     math.Inf(-1),
				max:     math.Inf(1),
				want:    7,
				wantErr: false,
			},
			{
				name:   "2. Get count of sub-set from -inf to limit",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
					{Value: "member4", Score: ss.Score(1083.13)},
					{Value: "member5", Score: ss.Score(11)},
					{Value: "member6", Score: ss.Score(math.Inf(-1))},
					{Value: "member7", Score: ss.Score(math.Inf(1))},
				}),
				key:     "zcount_key2",
				min:     math.Inf(-1),
				max:     90,
				want:    5,
				wantErr: false,
			},
			{
				name:   "3. Get count of sub-set from bottom boundary to +inf limit",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "member1", Score: ss.Score(5.5)},
					{Value: "member2", Score: ss.Score(67.77)},
					{Value: "member3", Score: ss.Score(10)},
					{Value: "member4", Score: ss.Score(1083.13)},
					{Value: "member5", Score: ss.Score(11)},
					{Value: "member6", Score: ss.Score(math.Inf(-1))},
					{Value: "member7", Score: ss.Score(math.Inf(1))},
				}),
				key:     "zcount_key3",
				min:     1000,
				max:     math.Inf(1),
				want:    2,
				wantErr: false,
			},
			{
				name:        "4. Throw error when value at the key is not a sorted set",
				preset:      true,
				presetValue: "Default value",
				key:         "zcount_key4",
				min:         1,
				max:         10,
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZCount(tt.key, tt.min, tt.max)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZCOUNT() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZCOUNT() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZDIFF", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			withscores   bool
			keys         []string
			want         map[string]float64
			wantErr      bool
		}{
			{
				name:   "1. Get the difference between 2 sorted sets without scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiff_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1},
						{Value: "two", Score: 2},
						{Value: "three", Score: 3},
						{Value: "four", Score: 4},
					}),
					"zdiff_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "three", Score: 3},
						{Value: "four", Score: 4},
						{Value: "five", Score: 5},
						{Value: "six", Score: 6},
						{Value: "seven", Score: 7},
						{Value: "eight", Score: 8},
					}),
				},
				withscores: false,
				keys:       []string{"zdiff_key1", "zdiff_key2"},
				want:       map[string]float64{"one": 0, "two": 0},
				wantErr:    false,
			},
			{
				name:   "2. Get the difference between 2 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiff_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1},
						{Value: "two", Score: 2},
						{Value: "three", Score: 3},
						{Value: "four", Score: 4},
					}),
					"zdiff_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "three", Score: 3},
						{Value: "four", Score: 4},
						{Value: "five", Score: 5},
						{Value: "six", Score: 6},
						{Value: "seven", Score: 7},
						{Value: "eight", Score: 8},
					}),
				},
				withscores: true,
				keys:       []string{"zdiff_key3", "zdiff_key4"},
				want:       map[string]float64{"one": 1, "two": 2},
				wantErr:    false,
			},
			{
				name:   "3. Get the difference between 3 sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiff_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zdiff_key6": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zdiff_key7": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				withscores: true,
				keys:       []string{"zdiff_key5", "zdiff_key6", "zdiff_key7"},
				want:       map[string]float64{"three": 3, "four": 4, "five": 5, "six": 6},
				wantErr:    false,
			},
			{
				name:   "4. Return sorted set if only one key exists and is a sorted set",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiff_key8": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				withscores: true,
				keys:       []string{"zdiff_key8", "zdiff_non-existent-key-1", "zdiff_non-existent-key-2", "zdiff_non-existent-key-3"},
				want: map[string]float64{
					"one": 1, "two": 2, "three": 3, "four": 4,
					"five": 5, "six": 6, "seven": 7, "eight": 8,
				},
				wantErr: false,
			},
			{
				name:   "5. Throw error when one of the keys is not a sorted set",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiff_key9": "Default value",
					"zdiff_key10": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zdiff_key11": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				withscores: false,
				keys:       []string{"zdiff_key9", "zdiff_key10", "zdiff_key11"},
				want:       nil,
				wantErr:    true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZDiff(tt.withscores, tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZDIFF() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ZDIFF() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZDIFFSTORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			destination  string
			keys         []string
			want         int
			wantErr      bool
		}{
			{
				name:   "1. Get the difference between 2 sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiffstore_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
					"zdiffstore_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				destination: "zdiffstore_destination1",
				keys:        []string{"zdiffstore_key1", "zdiffstore_key2"},
				want:        2,
				wantErr:     false,
			},
			{
				name:   "2. Get the difference between 3 sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiffstore_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zdiffstore_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zdiffstore_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zdiffstore_destination2",
				keys:        []string{"zdiffstore_key3", "zdiffstore_key4", "zdiffstore_key5"},
				want:        4,
				wantErr:     false,
			},
			{
				name:   "3. Return base sorted set element if base set is the only existing key provided and is a valid sorted set",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiffstore_key6": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				destination: "zdiffstore_destination3",
				keys:        []string{"zdiffstore_key6", "zdiffstore_non-existent-key-1", "zdiffstore_non-existent-key-2"},
				want:        8,
				wantErr:     false,
			},
			{
				name:   "4. Throw error when base sorted set is not a set",
				preset: true,
				presetValues: map[string]interface{}{
					"zdiffstore_key7": "Default value",
					"zdiffstore_key8": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zdiffstore_key9": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zdiffstore_destination4",
				keys:        []string{"zdiffstore_key7", "zdiffstore_key8", "zdiffstore_key9"},
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZDiffStore(tt.destination, tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZDIFFSTORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZDIFFSTORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZINCRBY", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			increment   float64
			member      string
			want        float64
			wantErr     bool
		}{
			{
				name:   "1. Successfully increment by int. Return the new score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:       "zincrby_key1",
				increment: 5,
				member:    "one",
				want:      6,
				wantErr:   false,
			},
			{
				name:   "2. Successfully increment by float. Return new score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:       "zincrby_key2",
				increment: 346.785,
				member:    "one",
				want:      347.785,
			},
			{
				name:        "3. Increment on non-existent sorted set will create the set with the member and increment as its score",
				preset:      false,
				presetValue: nil,
				key:         "zincrby_key3",
				increment:   346.785,
				member:      "one",
				want:        346.785,
				wantErr:     false,
			},
			{
				name:   "4. Increment score to +inf",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:       "zincrby_key4",
				increment: math.Inf(1),
				member:    "one",
				want:      math.Inf(1),
				wantErr:   false,
			},
			{
				name:   "5. Increment score to -inf",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:       "zincrby_key5",
				increment: math.Inf(-1),
				member:    "one",
				want:      math.Inf(-1),
				wantErr:   false,
			},
			{
				name:   "6. Incrementing score by negative increment should lower the score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:       "zincrby_key6",
				increment: -2.5,
				member:    "five",
				want:      2.5,
				wantErr:   false,
			},
			{
				name:        "7. Return error when attempting to increment on a value that is not a valid sorted set",
				preset:      true,
				presetValue: "Default value",
				key:         "zincrby_key7",
				increment:   -2.5,
				member:      "five",
				want:        0,
				wantErr:     true,
			},
			{
				name:   "8. Return error when trying to increment a member that already has score -inf",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: ss.Score(math.Inf(-1))},
				}),
				key:       "zincrby_key8",
				increment: 2.5,
				member:    "one",
				want:      0,
				wantErr:   true,
			},
			{
				name:   "9. Return error when trying to increment a member that already has score +inf",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: ss.Score(math.Inf(1))},
				}),
				key:       "zincrby_key9",
				increment: 2.5,
				member:    "one",
				want:      0,
				wantErr:   true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZIncrBy(tt.key, tt.increment, tt.member)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZINCRBY() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZINCRBY() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZINTER", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			keys         []string
			options      ZInterOptions
			want         map[string]float64
			wantErr      bool
		}{
			{
				name:   "1. Get the intersection between 2 sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
					"zinter_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				keys:    []string{"zinter_key1", "zinter_key2"},
				options: ZInterOptions{},
				want:    map[string]float64{"three": 0, "four": 0, "five": 0},
				wantErr: false,
			},
			{
				name:   "2. Get the intersection between 3 sorted sets with scores, SUM by default",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
					}),
					"zinter_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zinter_key3", "zinter_key4", "zinter_key5"},
				options: ZInterOptions{WithScores: true},
				want:    map[string]float64{"one": 3, "eight": 24},
				wantErr: false,
			},
			{
				name:   "3. Get the intersection between 3 sorted sets with scores (MIN)",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key6": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key7": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinter_key8": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zinter_key6", "zinter_key7", "zinter_key8"},
				options: ZInterOptions{Aggregate: "MIN", WithScores: true},
				want:    map[string]float64{"one": 1, "eight": 8},
				wantErr: false,
			},
			{
				name:   "4. Get the intersection between 3 sorted sets with scores. (MAX)",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key9": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key10": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinter_key11": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zinter_key9", "zinter_key10", "zinter_key11"},
				options: ZInterOptions{WithScores: true, Aggregate: "MAX"},
				want:    map[string]float64{"one": 1000, "eight": 800},
				wantErr: false,
			},
			{
				name:   "5. Get the intersection between 3 sorted sets with scores (SUM w/ weights)",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key12": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key13": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinter_key14": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zinter_key12", "zinter_key13", "zinter_key14"},
				options: ZInterOptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 5, 3}},
				want:    map[string]float64{"one": 3105, "eight": 2808},
				wantErr: false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// Use MAX aggregate with added weights.
				name:   "6. Get the intersection between 3 sorted sets with scores (MAX w/ weights)",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key15": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key16": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinter_key17": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zinter_key15", "zinter_key16", "zinter_key17"},
				options: ZInterOptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 5, 3}},
				want:    map[string]float64{"one": 3000, "eight": 2400},
				wantErr: false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// Use MIN aggregate with added weights.
				name:   "7. Get the intersection between 3 sorted sets with scores (MIN w/ weights)",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key18": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key19": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinter_key20": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zinter_key18", "zinter_key19", "zinter_key20"},
				options: ZInterOptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 5, 3}},
				want:    map[string]float64{"one": 5, "eight": 8},
				wantErr: false,
			},
			{
				name:   "8. Throw an error if there are more weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key21": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key22": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{"zinter_key21", "zinter_key22"},
				options: ZInterOptions{Weights: []float64{1, 2, 3}},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "9. Throw an error if there are fewer weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key23": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key24": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
					}),
					"zinter_key25": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{"zinter_key23", "zinter_key24", "zinter_key25"},
				options: ZInterOptions{Weights: []float64{5, 4}},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "10. Throw an error if there are no keys provided",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key26": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
					"zinter_key27": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
					"zinter_key28": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{},
				options: ZInterOptions{},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "11. Throw an error if any of the provided keys are not sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key29": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinter_key30": "Default value",
					"zinter_key31": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{"zinter_key29", "zinter_key30", "zinter_key31"},
				options: ZInterOptions{},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "12. If any of the keys does not exist, return an empty array",
				preset: true,
				presetValues: map[string]interface{}{
					"zinter_key32": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zinter_key33": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zinter_non-existent", "zinter_key32", "zinter_key33"},
				options: ZInterOptions{},
				want:    map[string]float64{},
				wantErr: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZInter(tt.keys, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZINTER() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ZINTER() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZINTERSTORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			destination  string
			keys         []string
			options      ZInterStoreOptions
			want         int
			wantErr      bool
		}{
			{
				name:   "1. Get the intersection between 2 sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
					"zinterstore_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				destination: "zinterstore_destination1",
				keys:        []string{"zinterstore_key1", "zinterstore_key2"},
				options:     ZInterStoreOptions{},
				want:        3,
				wantErr:     false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// By default, the SUM aggregate will be used.
				name:   "2. Get the intersection between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zinterstore_destination2",
				keys:        []string{"zinterstore_key3", "zinterstore_key4", "zinterstore_key5"},
				options:     ZInterStoreOptions{WithScores: true},
				want:        2,
				wantErr:     false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// Use MIN aggregate.
				name:   "3. Get the intersection between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key6": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key7": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinterstore_key8": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zinterstore_destination3",
				keys:        []string{"zinterstore_key6", "zinterstore_key7", "zinterstore_key8"},
				options:     ZInterStoreOptions{WithScores: true, Aggregate: "MIN"},
				want:        2,
				wantErr:     false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// Use MAX aggregate.
				name:   "4. Get the intersection between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key9": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key10": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinterstore_key11": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zinterstore_destination4",
				keys:        []string{"zinterstore_key9", "zinterstore_key10", "zinterstore_key11"},
				options:     ZInterStoreOptions{WithScores: true, Aggregate: "MAX"},
				want:        2,
				wantErr:     false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// Use SUM aggregate with weights modifier.
				name:   "5. Get the intersection between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key12": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key13": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinterstore_key14": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zinterstore_destination5",
				keys:        []string{"zinterstore_key12", "zinterstore_key13", "zinterstore_key14"},
				options:     ZInterStoreOptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 5, 3}},
				want:        2,
				wantErr:     false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// Use MAX aggregate with added weights.
				name:   "6. Get the intersection between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key15": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key16": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinterstore_key17": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zinterstore_destination6",
				keys:        []string{"zinterstore_key15", "zinterstore_key16", "zinterstore_key17"},
				options:     ZInterStoreOptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 5, 3}},
				want:        2,
				wantErr:     false,
			},
			{
				// Get the intersection between 3 sorted sets with scores.
				// Use MIN aggregate with added weights.
				name:   "7. Get the intersection between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key18": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key19": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zinterstore_key20": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zinterstore_destination7",
				keys:        []string{"zinterstore_key18", "zinterstore_key19", "zinterstore_key20"},
				options:     ZInterStoreOptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 5, 3}},
				want:        2,
				wantErr:     false,
			},
			{
				name:   "8. Throw an error if there are more weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key21": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key22": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				destination: "zinterstore_destination8",
				keys:        []string{"zinterstore_key21", "zinterstore_key22"},
				options:     ZInterStoreOptions{Weights: []float64{1, 2, 3}},
				want:        0,
				wantErr:     true,
			},
			{
				name:   "9. Throw an error if there are fewer weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key23": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key24": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
					}),
					"zinterstore_key25": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				destination: "zinterstore_destination9",
				keys:        []string{"zinterstore_key23", "zinterstore_key24"},
				options:     ZInterStoreOptions{Weights: []float64{5}},
				want:        0,
				wantErr:     true,
			},
			{
				name:   "10. Throw an error if there are no keys provided",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key26": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
					"zinterstore_key27": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
					"zinterstore_key28": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				destination: "zinterstore_destination10",
				keys:        []string{},
				options:     ZInterStoreOptions{Weights: []float64{5, 4}},
				want:        0,
				wantErr:     true,
			},
			{
				name:   "11. Throw an error if any of the provided keys are not sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key29": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zinterstore_key30": "Default value",
					"zinterstore_key31": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				destination: "zinterstore_destination11",
				keys:        []string{"zinterstore_key29", "zinterstore_key30", "zinterstore_key31"},
				options:     ZInterStoreOptions{},
				want:        0,
				wantErr:     true,
			},
			{
				name:   "12. If any of the keys does not exist, return an empty array",
				preset: true,
				presetValues: map[string]interface{}{
					"zinterstore_key32": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zinterstore_key33": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zinterstore_destination12",
				keys:        []string{"zinterstore_non-existent", "zinterstore_key32", "zinterstore_key33"},
				options:     ZInterStoreOptions{},
				want:        0,
				wantErr:     false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZInterStore(tt.destination, tt.keys, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZINTERSTORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZINTERSTORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZLEXCOUNT", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			min         string
			max         string
			want        int
			wantErr     bool
		}{
			{
				name:   "1. Get entire count using infinity boundaries",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "e", Score: ss.Score(1)},
					{Value: "f", Score: ss.Score(1)},
					{Value: "g", Score: ss.Score(1)},
					{Value: "h", Score: ss.Score(1)},
					{Value: "i", Score: ss.Score(1)},
					{Value: "j", Score: ss.Score(1)},
					{Value: "k", Score: ss.Score(1)},
				}),
				key:     "zlexcount_key1",
				min:     "f",
				max:     "j",
				want:    5,
				wantErr: false,
			},
			{
				name:   "2. Return 0 when the members do not have the same score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "a", Score: ss.Score(5.5)},
					{Value: "b", Score: ss.Score(67.77)},
					{Value: "c", Score: ss.Score(10)},
					{Value: "d", Score: ss.Score(1083.13)},
					{Value: "e", Score: ss.Score(11)},
					{Value: "f", Score: ss.Score(math.Inf(-1))},
					{Value: "g", Score: ss.Score(math.Inf(1))},
				}),
				key:     "zlexcount_key2",
				min:     "a",
				max:     "b",
				want:    0,
				wantErr: false,
			},
			{
				name:        "3. Return 0 when the key does not exist",
				preset:      false,
				presetValue: nil,
				key:         "zlexcount_key3",
				min:         "a",
				max:         "z",
				want:        0,
				wantErr:     false,
			},
			{
				name:        "4. Return error when the value at the key is not a sorted set",
				preset:      true,
				presetValue: "Default value",
				key:         "zlexcount_key4",
				min:         "a",
				max:         "z",
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZLexCount(tt.key, tt.min, tt.max)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZLEXCOUNT() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZLEXCOUNT() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZMPOP", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			keys         []string
			options      ZMPopOptions
			want         [][]string
			wantErr      bool
		}{
			{
				name:   "1. Successfully pop one min element by default",
				preset: true,
				presetValues: map[string]interface{}{
					"zmpop_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
				},
				keys:    []string{"zmpop_key1"},
				options: ZMPopOptions{},
				want: [][]string{
					{"one", "1"},
				},
				wantErr: false,
			},
			{
				name:   "2. Successfully pop one min element by specifying MIN",
				preset: true,
				presetValues: map[string]interface{}{
					"zmpop_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
				},
				keys:    []string{"zmpop_key2"},
				options: ZMPopOptions{Min: true},
				want: [][]string{
					{"one", "1"},
				},
				wantErr: false,
			},
			{
				name:   "3. Successfully pop one max element by specifying MAX modifier",
				preset: true,
				presetValues: map[string]interface{}{
					"zmpop_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
				},
				keys:    []string{"zmpop_key3"},
				options: ZMPopOptions{Max: true},
				want: [][]string{
					{"five", "5"},
				},
				wantErr: false,
			},
			{
				name:   "4. Successfully pop multiple min elements",
				preset: true,
				presetValues: map[string]interface{}{
					"zmpop_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
					}),
				},
				keys:    []string{"zmpop_key4"},
				options: ZMPopOptions{Min: true, Count: 5},
				want: [][]string{
					{"one", "1"}, {"two", "2"}, {"three", "3"},
					{"four", "4"}, {"five", "5"},
				},
				wantErr: false,
			},
			{
				name:   "5. Successfully pop multiple max elements",
				preset: true,
				presetValues: map[string]interface{}{
					"zmpop_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
					}),
				},
				keys:    []string{"zmpop_key5"},
				options: ZMPopOptions{Max: true, Count: 5},
				want:    [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
				wantErr: false,
			},
			{
				name:   "6. Successfully pop elements from the first set which is non-empty",
				preset: true,
				presetValues: map[string]interface{}{
					"zmpop_key6": ss.NewSortedSet([]ss.MemberParam{}),
					"zmpop_key7": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
					}),
				},
				keys:    []string{"zmpop_key6", "zmpop_key7"},
				options: ZMPopOptions{Max: true, Count: 5},
				want:    [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
				wantErr: false,
			},
			{
				name:   "7. Skip the non-set items and pop elements from the first non-empty sorted set found",
				preset: true,
				presetValues: map[string]interface{}{
					"zmpop_key8":  "Default value",
					"zmpop_key9":  56,
					"zmpop_key10": ss.NewSortedSet([]ss.MemberParam{}),
					"zmpop_key11": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
					}),
				},
				keys:    []string{"zmpop_key8", "zmpop_key9", "zmpop_key10", "zmpop_key11"},
				options: ZMPopOptions{Min: true, Count: 5},
				want:    [][]string{{"one", "1"}, {"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}},
				wantErr: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZMPop(tt.keys, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZMPOP() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !internal.CompareNestedStringArrays(got, tt.want) {
					t.Errorf("ZMPOP() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZMSCORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			members     []string
			want        []interface{}
			wantErr     bool
		}{
			{ // Return multiple scores from the sorted set.
				// Return nil for elements that do not exist in the sorted set.
				name:   "1. Return multiple scores from the sorted set",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
					{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
					{Value: "five", Score: 5},
				}),
				key:     "zmscore_key1",
				members: []string{"one", "none", "two", "one", "three", "four", "none", "five"},
				want:    []interface{}{"1.1", nil, "245", "1.1", "3", "4.055", nil, "5"},
				wantErr: false,
			},
			{
				name:        "2. If key does not exist, return empty array",
				preset:      false,
				presetValue: nil,
				key:         "zmscore_key2",
				members:     []string{"one", "two", "three", "four"},
				want:        []interface{}{},
				wantErr:     false,
			},
			{
				name:        "3. Throw error when trying to find scores from elements that are not sorted sets",
				preset:      true,
				presetValue: "Default value",
				key:         "zmscore_key3",
				members:     []string{"one", "two", "three"},
				want:        []interface{}{},
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZMScore(tt.key, tt.members...)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZMSCORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("ZMSCORE() got length = %v, want length %v", len(got), len(tt.want))
					return
				}
				for i := 0; i < len(got); i++ {
					if got[i] == nil && tt.want[i] == nil {
						continue
					}
					if (got[i] == nil) != (tt.want[i] == nil) {
						t.Errorf("ZMSCORE() got[%d] = %v, want[%d] %v", i, got, i, tt.want)
					}
					wantf, _ := strconv.ParseFloat(tt.want[i].(string), 64)
					if got[i] != wantf {
						t.Errorf("ZMSCORE() got[%d] = %v, want[%d] %v", i, got[i], i, wantf)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_ZPOP", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			count       uint
			popFunc     func(key string, count uint) ([][]string, error)
			want        [][]string
			wantErr     bool
		}{
			{
				name:   "1. Successfully pop one min element",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:     "zpop_key1",
				count:   1,
				popFunc: server.ZPopMin,
				want: [][]string{
					{"one", "1"},
				},
				wantErr: false,
			},
			{
				name:   "2. Successfully pop one max element",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:     "zpop_key2",
				count:   1,
				popFunc: server.ZPopMax,
				want:    [][]string{{"five", "5"}},
				wantErr: false,
			},
			{
				name:   "3. Successfully pop multiple min elements",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
				}),
				popFunc: server.ZPopMin,
				key:     "zpop_key3",
				count:   5,
				want: [][]string{
					{"one", "1"}, {"two", "2"}, {"three", "3"},
					{"four", "4"}, {"five", "5"},
				},
				wantErr: false,
			},
			{
				name:   "4. Successfully pop multiple max elements",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
				}),
				popFunc: server.ZPopMax,
				key:     "zpop_key4",
				count:   5,
				want:    [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
				wantErr: false,
			},
			{
				name:        "5. Throw an error when trying to pop from an element that's not a sorted set",
				preset:      true,
				presetValue: "Default value",
				popFunc:     server.ZPopMin,
				key:         "zpop_key5",
				count:       1,
				want:        [][]string{},
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := tt.popFunc(tt.key, tt.count)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZPOPMAX() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !internal.CompareNestedStringArrays(got, tt.want) {
					t.Errorf("ZPOPMAX() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZRANDMEMBER", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			count       int
			withscores  bool
			want        int
			wantErr     bool
		}{
			{ // Return multiple random elements without removing them.
				// Count is positive, do not allow repeated elements.
				name:   "1. Return multiple random elements without removing them",
				preset: true,
				key:    "zrandmember_key1",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2}, {Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6}, {Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				count:      3,
				withscores: false,
				want:       3,
				wantErr:    false,
			},
			{
				// Return multiple random elements and their scores without removing them.
				// Count is negative, so allow repeated numbers.
				name:   "2. Return multiple random elements and their scores without removing them",
				preset: true,
				key:    "zrandmember_key2",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2}, {Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6}, {Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				count:      -5,
				withscores: true,
				want:       5,
				wantErr:    false,
			},
			{
				name:        "3. Return error when the source key is not a sorted set",
				preset:      true,
				key:         "zrandmember_key3",
				presetValue: "Default value",
				count:       1,
				withscores:  false,
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRandMember(tt.key, tt.count, tt.withscores)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZRANDMEMBER() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != tt.want {
					t.Errorf("ZRANDMEMBER() got = %v, want %v", len(got), tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZRANGE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			start       string
			stop        string
			options     ZRangeOptions
			want        map[string]float64
			wantErr     bool
		}{
			{
				name:   "1. Get elements withing score range without score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				key:     "zrange_key1",
				start:   "3",
				stop:    "7",
				options: ZRangeOptions{ByScore: true},
				want:    map[string]float64{"three": 0, "four": 0, "five": 0, "six": 0, "seven": 0},
				wantErr: false,
			},
			{
				name:   "2. Get elements within score range with score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				key:     "zrange_key2",
				start:   "3",
				stop:    "7",
				options: ZRangeOptions{ByScore: true, WithScores: true},
				want:    map[string]float64{"three": 3, "four": 4, "five": 5, "six": 6, "seven": 7},
				wantErr: false,
			},
			{
				// Get elements within score range with offset and limit.
				// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
				name:   "3. Get elements within score range with offset and limit",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				key:     "zrange_key3",
				start:   "3",
				stop:    "7",
				options: ZRangeOptions{WithScores: true, ByScore: true, Offset: 2, Count: 4},
				want:    map[string]float64{"three": 3, "four": 4, "five": 5},
				wantErr: false,
			},
			{
				name:   "4. Get elements within lex range without score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "a", Score: 1}, {Value: "e", Score: 1},
					{Value: "b", Score: 1}, {Value: "f", Score: 1},
					{Value: "c", Score: 1}, {Value: "g", Score: 1},
					{Value: "d", Score: 1}, {Value: "h", Score: 1},
				}),
				key:     "zrange_key4",
				start:   "c",
				stop:    "g",
				options: ZRangeOptions{ByLex: true},
				want:    map[string]float64{"c": 0, "d": 0, "e": 0, "f": 0, "g": 0},
				wantErr: false,
			},
			{
				name:   "5. Get elements within lex range with score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "a", Score: 1}, {Value: "e", Score: 1},
					{Value: "b", Score: 1}, {Value: "f", Score: 1},
					{Value: "c", Score: 1}, {Value: "g", Score: 1},
					{Value: "d", Score: 1}, {Value: "h", Score: 1},
				}),
				key:     "zrange_key5",
				start:   "a",
				stop:    "f",
				options: ZRangeOptions{ByLex: true, WithScores: true},
				want:    map[string]float64{"a": 1, "b": 1, "c": 1, "d": 1, "e": 1, "f": 1},
				wantErr: false,
			},
			{
				// Get elements within lex range with offset and limit.
				// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
				name:   "6. Get elements within lex range with offset and limit",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "a", Score: 1}, {Value: "b", Score: 1},
					{Value: "c", Score: 1}, {Value: "d", Score: 1},
					{Value: "e", Score: 1}, {Value: "f", Score: 1},
					{Value: "g", Score: 1}, {Value: "h", Score: 1},
				}),
				key:     "zrange_key6",
				start:   "a",
				stop:    "h",
				options: ZRangeOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
				want:    map[string]float64{"c": 1, "d": 1, "e": 1},
				wantErr: false,
			},
			{
				name:   "7. Return an empty map when we use BYLEX while elements have different scores",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "a", Score: 1}, {Value: "b", Score: 5},
					{Value: "c", Score: 2}, {Value: "d", Score: 6},
					{Value: "e", Score: 3}, {Value: "f", Score: 7},
					{Value: "g", Score: 4}, {Value: "h", Score: 8},
				}),
				key:     "zrange_key7",
				start:   "a",
				stop:    "h",
				options: ZRangeOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
				want:    map[string]float64{},
				wantErr: false,
			},
			{
				name:        "8. Throw error when the key does not hold a sorted set",
				preset:      true,
				presetValue: "Default value",
				key:         "zrange_key10",
				start:       "a",
				stop:        "h",
				options:     ZRangeOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRange(tt.key, tt.start, tt.stop, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZRANGE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ZRANGE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZRANGESTORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			destination  string
			source       string
			start        string
			stop         string
			options      ZRangeStoreOptions
			want         int
			wantErr      bool
		}{
			{
				name:   "1. Get elements within score range without score",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				destination: "zrangestore_destination1",
				source:      "zrangestore_key1",
				start:       "3",
				stop:        "7",
				options:     ZRangeStoreOptions{ByScore: true},
				want:        5,
				wantErr:     false,
			},
			{
				name:   "2. Get elements within score range with score",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				destination: "zrangestore_destination2",
				source:      "zrangestore_key2",
				start:       "3",
				stop:        "7",
				options:     ZRangeStoreOptions{WithScores: true, ByScore: true},
				want:        5,
				wantErr:     false,
			},
			{
				// Get elements within score range with offset and limit.
				// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
				name:   "3. Get elements within score range with offset and limit",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				destination: "zrangestore_destination3",
				source:      "zrangestore_key3",
				start:       "3",
				stop:        "7",
				options:     ZRangeStoreOptions{ByScore: true, WithScores: true, Offset: 2, Count: 4},
				want:        3,
				wantErr:     false,
			},
			{
				name:   "4. Get elements within lex range without score",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "a", Score: 1}, {Value: "e", Score: 1},
						{Value: "b", Score: 1}, {Value: "f", Score: 1},
						{Value: "c", Score: 1}, {Value: "g", Score: 1},
						{Value: "d", Score: 1}, {Value: "h", Score: 1},
					}),
				},
				destination: "zrangestore_destination4",
				source:      "zrangestore_key4",
				start:       "c",
				stop:        "g",
				options:     ZRangeStoreOptions{ByLex: true},
				want:        5,
				wantErr:     false,
			},
			{
				name:   "5. Get elements within lex range with score",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "a", Score: 1}, {Value: "e", Score: 1},
						{Value: "b", Score: 1}, {Value: "f", Score: 1},
						{Value: "c", Score: 1}, {Value: "g", Score: 1},
						{Value: "d", Score: 1}, {Value: "h", Score: 1},
					}),
				},
				destination: "zrangestore_destination5",
				source:      "zrangestore_key5",
				start:       "a",
				stop:        "f",
				options:     ZRangeStoreOptions{ByLex: true, WithScores: true},
				want:        6,
				wantErr:     false,
			},
			{
				// Get elements within lex range with offset and limit.
				// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
				name:   "6. Get elements within lex range with offset and limit",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key6": ss.NewSortedSet([]ss.MemberParam{
						{Value: "a", Score: 1}, {Value: "b", Score: 1},
						{Value: "c", Score: 1}, {Value: "d", Score: 1},
						{Value: "e", Score: 1}, {Value: "f", Score: 1},
						{Value: "g", Score: 1}, {Value: "h", Score: 1},
					}),
				},
				destination: "zrangestore_destination6",
				source:      "zrangestore_key6",
				start:       "a",
				stop:        "h",
				options:     ZRangeStoreOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
				want:        3,
				wantErr:     false,
			},
			{
				// Get elements within lex range with offset and limit + reverse the results.
				// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
				// REV reverses the original set before getting the range.
				name:   "7. Get elements within lex range with offset and limit + reverse the results",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key7": ss.NewSortedSet([]ss.MemberParam{
						{Value: "a", Score: 1}, {Value: "b", Score: 1},
						{Value: "c", Score: 1}, {Value: "d", Score: 1},
						{Value: "e", Score: 1}, {Value: "f", Score: 1},
						{Value: "g", Score: 1}, {Value: "h", Score: 1},
					}),
				},
				destination: "zrangestore_destination7",
				source:      "zrangestore_key7",
				start:       "a",
				stop:        "h",
				options:     ZRangeStoreOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
				want:        3,
				wantErr:     false,
			},
			{
				name:   "8. Return an empty slice when we use BYLEX while elements have different scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key8": ss.NewSortedSet([]ss.MemberParam{
						{Value: "a", Score: 1}, {Value: "b", Score: 5},
						{Value: "c", Score: 2}, {Value: "d", Score: 6},
						{Value: "e", Score: 3}, {Value: "f", Score: 7},
						{Value: "g", Score: 4}, {Value: "h", Score: 8},
					}),
				},
				destination: "zrangestore_destination8",
				source:      "zrangestore_key8",
				start:       "a",
				stop:        "h",
				options:     ZRangeStoreOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
				want:        0,
				wantErr:     false,
			},
			{
				name:   "9. Throw error when the key does not hold a sorted set",
				preset: true,
				presetValues: map[string]interface{}{
					"zrangestore_key9": "Default value",
				},
				destination: "zrangestore_destination9",
				source:      "zrangestore_key9",
				start:       "a",
				stop:        "h",
				options:     ZRangeStoreOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZRangeStore(tt.destination, tt.source, tt.start, tt.stop, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZRANGESTORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZRANGESTORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZRANK", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			member      string
			withscores  bool
			want        map[int]float64
			wantErr     bool
		}{
			{
				name:   "1. Return element's rank from a sorted set",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:        "zrank_key1",
				member:     "four",
				withscores: false,
				want:       map[int]float64{3: 0},
				wantErr:    false,
			},
			{
				name:   "2. Return element's rank from a sorted set with its score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 100.1}, {Value: "two", Score: 245},
					{Value: "three", Score: 305.43}, {Value: "four", Score: 411.055},
					{Value: "five", Score: 500},
				}),
				key:        "zrank_key2",
				member:     "four",
				withscores: true,
				want:       map[int]float64{3: 411.055},
				wantErr:    false,
			},
			{
				name:        "3. If key does not exist, return nil value",
				preset:      false,
				presetValue: nil,
				key:         "zrank_key3",
				member:      "one",
				withscores:  false,
				want:        map[int]float64{},
				wantErr:     false,
			},
			{
				name:   "4. If key exists and is a sorted set, but the member does not exist, return nil",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
					{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
					{Value: "five", Score: 5},
				}),
				key:        "zrank_key4",
				member:     "non-existent",
				withscores: false,
				want:       map[int]float64{},
				wantErr:    false,
			},
			{
				name:        "5. Throw error when trying to find scores from elements that are not sorted sets",
				preset:      true,
				presetValue: "Default value",
				key:         "zrank_key5",
				member:      "one",
				withscores:  false,
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRank(tt.key, tt.member, tt.withscores)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZRANK() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ZRANK() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZREM", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			members     []string
			want        int
			wantErr     bool
		}{
			{
				// Successfully remove multiple elements from sorted set, skipping non-existent members.
				// Return deleted count.
				name:   "1. Successfully remove multiple elements from sorted set, skipping non-existent members",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
				}),
				key:     "zrem_key1",
				members: []string{"three", "four", "five", "none", "six", "none", "seven"},
				want:    5,
				wantErr: false,
			},
			{
				name:        "2. If key does not exist, return 0",
				preset:      false,
				presetValue: nil,
				key:         "zrem_key2",
				members:     []string{"member"},
				want:        0,
				wantErr:     false,
			},
			{
				name:        "3. Return error key is not a sorted set",
				preset:      true,
				presetValue: "Default value",
				key:         "zrem_key3",
				members:     []string{"member"},
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRem(tt.key, tt.members...)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZREM() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZREM() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZREMRANGEBYSCORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			min         float64
			max         float64
			want        int
			wantErr     bool
		}{
			{
				name:   "1. Successfully remove multiple elements with scores inside the provided range",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
				}),
				key:     "zremrangebyscore_key1",
				min:     3,
				max:     7,
				want:    5,
				wantErr: false,
			},
			{
				name:    "2. If key does not exist, return 0",
				preset:  false,
				key:     "zremrangebyscore_key2",
				min:     2,
				max:     4,
				want:    0,
				wantErr: false,
			},
			{
				name:        "3. Return error key is not a sorted set",
				preset:      true,
				presetValue: "Default value",
				key:         "zremrangebyscore_key3",
				min:         2,
				max:         4,
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRemRangeByScore(tt.key, tt.min, tt.max)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZREMRANGEBYSCORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZREMRANGEBYSCORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZSCORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			member      string
			want        interface{}
			wantErr     bool
		}{
			{
				name:   "1. Return score from a sorted set",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
					{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
					{Value: "five", Score: 5},
				}),
				key:     "zscore_key1",
				member:  "four",
				want:    4.055,
				wantErr: false,
			},
			{
				name:        "2. If key does not exist, return nil value",
				preset:      false,
				presetValue: nil,
				key:         "zscore_key2",
				member:      "one",
				want:        nil,
				wantErr:     false,
			},
			{
				name:   "3. If key exists and is a sorted set, but the member does not exist, return nil",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
					{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
					{Value: "five", Score: 5},
				}),
				key:     "zscore_key3",
				member:  "non-existent",
				want:    nil,
				wantErr: false,
			},
			{
				name:        "4. Throw error when trying to find scores from elements that are not sorted sets",
				preset:      true,
				presetValue: "Default value",
				key:         "zscore_key4",
				member:      "one",
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZScore(tt.key, tt.member)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZSCORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ZSCORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZUNION", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			keys         []string
			options      ZUnionOptions
			want         map[string]float64
			wantErr      bool
		}{
			{
				name:   "1. Get the union between 2 sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
					"zunion_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				keys:    []string{"zunion_key1", "zunion_key2"},
				options: ZUnionOptions{},
				want: map[string]float64{
					"one": 0, "two": 0, "three": 0, "four": 0,
					"five": 0, "six": 0, "seven": 0, "eight": 0,
				},
				wantErr: false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// By default, the SUM aggregate will be used.
				name:   "2. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
					}),
					"zunion_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 36},
					}),
				},
				keys:    []string{"zunion_key3", "zunion_key4", "zunion_key5"},
				options: ZUnionOptions{WithScores: true},
				want: map[string]float64{
					"one": 3, "two": 4, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 24, "nine": 9,
					"ten": 10, "eleven": 11, "twelve": 24, "thirty-six": 72,
				},
				wantErr: false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MIN aggregate.
				name:   "3. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key6": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key7": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunion_key8": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
					}),
				},
				keys:    []string{"zunion_key6", "zunion_key7", "zunion_key8"},
				options: ZUnionOptions{WithScores: true, Aggregate: "MIN"},
				want: map[string]float64{
					"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
					"ten": 10, "eleven": 11, "twelve": 12, "thirty-six": 36,
				},
				wantErr: false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MAX aggregate.
				name:   "4. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key9": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key10": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunion_key11": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
					}),
				},
				keys:    []string{"zunion_key9", "zunion_key10", "zunion_key11"},
				options: ZUnionOptions{WithScores: true, Aggregate: "MAX"},
				want: map[string]float64{
					"one": 1000, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 800, "nine": 9,
					"ten": 10, "eleven": 11, "twelve": 12, "thirty-six": 72,
				},
				wantErr: false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use SUM aggregate with weights modifier.
				name:   "5. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key12": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key13": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunion_key14": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zunion_key12", "zunion_key13", "zunion_key14"},
				options: ZUnionOptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 2, 3}},
				want: map[string]float64{
					"one": 3102, "two": 6, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 2568,
					"nine": 27, "ten": 30, "eleven": 22, "twelve": 60, "thirty-six": 72,
				},
				wantErr: false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MAX aggregate with added weights.
				name:   "6. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key15": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key16": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunion_key17": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zunion_key15", "zunion_key16", "zunion_key17"},
				options: ZUnionOptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 2, 3}},
				want: map[string]float64{
					"one": 3000, "two": 4, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 2400,
					"nine": 27, "ten": 30, "eleven": 22, "twelve": 36, "thirty-six": 72,
				},
				wantErr: false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MIN aggregate with added weights.
				name:   "7. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key18": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key19": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunion_key20": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zunion_key18", "zunion_key19", "zunion_key20"},
				options: ZUnionOptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 2, 3}},
				want: map[string]float64{
					"one": 2, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 27,
					"ten": 30, "eleven": 22, "twelve": 24, "thirty-six": 72,
				},
				wantErr: false,
			},
			{
				name:   "8. Throw an error if there are more weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key21": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key22": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{"zunion_key21", "zunion_key22"},
				options: ZUnionOptions{Weights: []float64{1, 2, 3}},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "9. Throw an error if there are fewer weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key23": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key24": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
					}),
					"zunion_key25": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{"zunion_key23", "zunion_key24", "zunion_key25"},
				options: ZUnionOptions{Weights: []float64{5, 4}},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "10. Throw an error if there are no keys provided",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key26": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
					"zunion_key27": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
					"zunion_key28": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{},
				options: ZUnionOptions{Weights: []float64{5, 4}},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "11. Throw an error if any of the provided keys are not sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key29": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunion_key30": "Default value",
					"zunion_key31": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				keys:    []string{"zunion_key29", "zunion_key30", "zunion_key31"},
				options: ZUnionOptions{},
				want:    nil,
				wantErr: true,
			},
			{
				name:   "12. If any of the keys does not exist, skip it",
				preset: true,
				presetValues: map[string]interface{}{
					"zunion_key32": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zunion_key33": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				keys:    []string{"zunion_non-existent", "zunion_key32", "zunion_key33"},
				options: ZUnionOptions{},
				want: map[string]float64{
					"one": 0, "two": 0, "thirty-six": 0, "twelve": 0, "eleven": 0,
					"seven": 0, "eight": 0, "nine": 0, "ten": 0,
				},
				wantErr: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZUnion(tt.keys, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZUNION() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ZUNION() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZUNIONSTORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			preset       bool
			presetValues map[string]interface{}
			destination  string
			keys         []string
			options      ZUnionStoreOptions
			want         int
			wantErr      bool
		}{
			{
				name:   "1. Get the union between 2 sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key1": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5},
					}),
					"zunionstore_key2": ss.NewSortedSet([]ss.MemberParam{
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
				},
				destination: "zunionstore_destination1",
				keys:        []string{"zunionstore_key1", "zunionstore_key2"},
				options:     ZUnionStoreOptions{},
				want:        8,
				wantErr:     false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// By default, the SUM aggregate will be used.
				name:   "2. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key3": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key4": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key5": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 36},
					}),
				},
				destination: "zunionstore_destination2",
				keys:        []string{"zunionstore_key3", "zunionstore_key4", "zunionstore_key5"},
				options:     ZUnionStoreOptions{WithScores: true},
				want:        13,
				wantErr:     false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MIN aggregate.
				name:   "3. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key6": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key7": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunionstore_key8": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
					}),
				},
				destination: "zunionstore_destination3",
				keys:        []string{"zunionstore_key6", "zunionstore_key7", "zunionstore_key8"},
				options:     ZUnionStoreOptions{WithScores: true, Aggregate: "MIN"},
				want:        13,
				wantErr:     false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MAX aggregate.
				name:   "4. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key9": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key10": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunionstore_key11": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
					}),
				},
				destination: "zunionstore_destination4",
				keys:        []string{"zunionstore_key9", "zunionstore_key10", "zunionstore_key11"},
				options:     ZUnionStoreOptions{WithScores: true, Aggregate: "MAX"},
				want:        13,
				wantErr:     false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use SUM aggregate with weights modifier.
				name:   "5. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key12": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key13": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunionstore_key14": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zunionstore_destination5",
				keys:        []string{"zunionstore_key12", "zunionstore_key13", "zunionstore_key14"},
				options:     ZUnionStoreOptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 2, 3}},
				want:        13,
				wantErr:     false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MAX aggregate with added weights.
				name:   "6. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key15": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key16": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunionstore_key17": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zunionstore_destination6",
				keys:        []string{"zunionstore_key15", "zunionstore_key16", "zunionstore_key17"},
				options:     ZUnionStoreOptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 2, 3}},
				want:        13,
				wantErr:     false,
			},
			{
				// Get the union between 3 sorted sets with scores.
				// Use MIN aggregate with added weights.
				name:   "7. Get the union between 3 sorted sets with scores",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key18": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 100}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key19": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
					}),
					"zunionstore_key20": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zunionstore_destination7",
				keys:        []string{"zunionstore_destination7", "zunionstore_key18", "zunionstore_key19", "zunionstore_key20"},
				options:     ZUnionStoreOptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 2, 3}},
				want:        13,
				wantErr:     false,
			},
			{
				name:   "8. Throw an error if there are more weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key21": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key22": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				destination: "zunionstore_destination8",
				keys:        []string{"zunionstore_key21", "zunionstore_key22"},
				options:     ZUnionStoreOptions{Weights: []float64{1, 2, 3}},
				want:        0,
				wantErr:     true,
			},
			{
				name:   "9. Throw an error if there are fewer weights than keys",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key23": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key24": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
					}),
					"zunionstore_key25": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				destination: "zunionstore_destination9",
				keys:        []string{"zunionstore_key23", "zunionstore_key24", "zunionstore_key25"},
				options:     ZUnionStoreOptions{Weights: []float64{5, 4}},
				want:        0,
				wantErr:     true,
			},
			{
				name:   "10. Throw an error if any of the provided keys are not sorted sets",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key29": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "three", Score: 3}, {Value: "four", Score: 4},
						{Value: "five", Score: 5}, {Value: "six", Score: 6},
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					}),
					"zunionstore_key30": "Default value",
					"zunionstore_key31": ss.NewSortedSet([]ss.MemberParam{{Value: "one", Score: 1}}),
				},
				destination: "zunionstore_destination11",
				keys:        []string{"zunionstore_key29", "zunionstore_key30", "zunionstore_key31"},
				options:     ZUnionStoreOptions{},
				want:        0,
				wantErr:     true,
			},
			{
				name:   "11. If any of the keys does not exist, skip it",
				preset: true,
				presetValues: map[string]interface{}{
					"zunionstore_key32": ss.NewSortedSet([]ss.MemberParam{
						{Value: "one", Score: 1}, {Value: "two", Score: 2},
						{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
						{Value: "eleven", Score: 11},
					}),
					"zunionstore_key33": ss.NewSortedSet([]ss.MemberParam{
						{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
						{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
						{Value: "twelve", Score: 12},
					}),
				},
				destination: "zunionstore_destination12",
				keys:        []string{"zunionstore_non-existent", "zunionstore_key32", "zunionstore_key33"},
				want:        9,
				wantErr:     false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.ZUnionStore(tt.destination, tt.keys, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZUNIONSTORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZUNIONSTORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZRevRank", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			member      string
			withscores  bool
			want        map[int]float64
			wantErr     bool
		}{
			{
				name:   "1. Return element's rank from a sorted set",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				key:        "zrevrank_key1",
				member:     "four",
				withscores: false,
				want:       map[int]float64{1: 0},
				wantErr:    false,
			},
			{
				name:   "2. Return element's rank from a sorted set with its score",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 100.1}, {Value: "two", Score: 245},
					{Value: "three", Score: 305.43}, {Value: "four", Score: 411.055},
					{Value: "five", Score: 500},
				}),
				key:        "zrevrank_key2",
				member:     "four",
				withscores: true,
				want:       map[int]float64{1: 411.055},
				wantErr:    false,
			},
			{
				name:        "3. If key does not exist, return empty map",
				preset:      false,
				presetValue: nil,
				key:         "zrevrank_key3",
				member:      "one",
				withscores:  false,
				want:        map[int]float64{},
				wantErr:     false,
			},
			{
				name:   "4. If key exists and is a sorted set, but the member does not exist, return nil",
				preset: true,
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
					{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
					{Value: "five", Score: 5},
				}),
				key:        "zrevrank_key4",
				member:     "non-existent",
				withscores: false,
				want:       map[int]float64{},
				wantErr:    false,
			},
			{
				name:        "5. Throw error when trying to find scores from elements that are not sorted sets",
				preset:      true,
				presetValue: "Default value",
				key:         "zrevrank_key5",
				member:      "one",
				withscores:  false,
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRevRank(tt.key, tt.member, tt.withscores)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZREVRANK() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ZREVRANK() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZRemRangeByLex", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			key         string
			presetValue interface{}
			min         string
			max         string
			want        int
			wantErr     bool
		}{
			{
				name: "1. Successfully remove multiple elements with scores inside the provided range",
				key:  "ZremRangeByLexKey1",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "a", Score: 1}, {Value: "b", Score: 1},
					{Value: "c", Score: 1}, {Value: "d", Score: 1},
					{Value: "e", Score: 1}, {Value: "f", Score: 1},
					{Value: "g", Score: 1}, {Value: "h", Score: 1},
					{Value: "i", Score: 1}, {Value: "j", Score: 1},
				}),
				min:     "a",
				max:     "d",
				want:    4,
				wantErr: false,
			},
			{
				name: "2. Return 0 if the members do not have the same score",
				key:  "ZremRangeByLexKey2",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "a", Score: 1}, {Value: "b", Score: 2},
					{Value: "c", Score: 3}, {Value: "d", Score: 4},
					{Value: "e", Score: 5}, {Value: "f", Score: 6},
					{Value: "g", Score: 7}, {Value: "h", Score: 8},
					{Value: "i", Score: 9}, {Value: "j", Score: 10},
				}),
				min:     "d",
				max:     "g",
				want:    0,
				wantErr: false,
			},
			{
				name:        "3. If key does not exist, return 0",
				key:         "ZremRangeByLexKey3",
				presetValue: nil,
				min:         "2",
				max:         "4",
				want:        0,
				wantErr:     false,
			},
			{
				name:        "4. Return error key is not a sorted set",
				key:         "ZremRangeByLexKey4",
				presetValue: "Default value",
				min:         "a",
				max:         "d",
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRemRangeByLex(tt.key, tt.min, tt.max)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZRemRangeByLex() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZRemRangeByLex() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_ZRemRangeByRank", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			key         string
			presetValue interface{}
			min         int
			max         int
			want        int
			wantErr     bool
		}{
			{
				name: "1. Successfully remove multiple elements within range",
				key:  "ZremRangeByRankKey1",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
				}),
				min:     0,
				max:     5,
				want:    6,
				wantErr: false,
			},
			{
				name: "2. Establish boundaries from the end of the set when negative boundaries are provided",
				key:  "ZremRangeByRankKey2",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
				}),
				min:     -6,
				max:     -3,
				want:    4,
				wantErr: false,
			},
			{
				name:        "3. If key does not exist, return 0",
				key:         "ZremRangeByRankKey3",
				presetValue: nil,
				min:         2,
				max:         4,
				want:        0,
				wantErr:     false,
			},
			{
				name:        "4. Return error key is not a sorted set",
				presetValue: "Default value",
				key:         "ZremRangeByRankKey3",
				min:         4,
				max:         4,
				want:        0,
				wantErr:     true,
			},
			{
				name: "5. Return error when start index is out of bounds",
				key:  "ZremRangeByRankKey5",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
				}),
				min:     -12,
				max:     5,
				want:    0,
				wantErr: true,
			},
			{
				name: "6. Return error when end index is out of bounds",
				key:  "ZremRangeByRankKey6",
				presetValue: ss.NewSortedSet([]ss.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
				}),
				min:     0,
				max:     11,
				want:    0,
				wantErr: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.ZRemRangeByRank(tt.key, tt.min, tt.max)
				if (err != nil) != tt.wantErr {
					t.Errorf("ZRemRangeByRank() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("ZRemRangeByRank() got = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
