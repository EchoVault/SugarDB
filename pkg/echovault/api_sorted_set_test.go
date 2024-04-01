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

package echovault

import (
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/sorted_set"
	"github.com/echovault/echovault/pkg/commands"
	"github.com/echovault/echovault/pkg/constants"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestEchoVault_ZADD(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name        string
		preset      bool
		presetValue *sorted_set.SortedSet
		key         string
		entries     map[string]float64
		options     ZADDOptions
		want        int
		wantErr     bool
	}{
		{
			name:        "Create new sorted set and return the cardinality of the new sorted set",
			preset:      false,
			presetValue: nil,
			key:         "key1",
			entries: map[string]float64{
				"member1": 5.5,
				"member2": 67.77,
				"member3": 10,
				"member4": math.Inf(-1),
				"member5": math.Inf(1),
			},
			options: ZADDOptions{},
			want:    5,
			wantErr: false,
		},
		{
			name:   "Only add the elements that do not currently exist in the sorted set when NX flag is provided",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key2",
			entries: map[string]float64{
				"member1": 5.5,
				"member4": 67.77,
				"member5": 10,
			},
			options: ZADDOptions{NX: true},
			want:    2,
			wantErr: false,
		},
		{
			name:   "Do not add any elements when providing existing members with NX flag",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key3",
			entries: map[string]float64{
				"member1": 5.5,
				"member2": 67.77,
				"member3": 10,
			},
			options: ZADDOptions{NX: true},
			want:    0,
			wantErr: false,
		},
		{
			name:   "Successfully add elements to an existing set when XX flag is provided with existing elements",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key4",
			entries: map[string]float64{
				"member1": 55,
				"member2": 1005,
				"member3": 15,
				"member4": 99.75,
			},
			options: ZADDOptions{XX: true, CH: true},
			want:    3,
			wantErr: false,
		},
		{
			name:   "Fail to add element when providing XX flag with elements that do not exist in the sorted set",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key5",
			entries: map[string]float64{
				"member4": 5.5,
				"member5": 100.5,
				"member6": 15,
			},
			options: ZADDOptions{XX: true},
			want:    0,
			wantErr: false,
		},
		{
			// Only update the elements where provided score is greater than current score if GT flag
			// Return only the new elements added by default
			name:   "Only update the elements where provided score is greater than current score if GT flag",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key6",
			entries: map[string]float64{
				"member1": 7.5,
				"member4": 100.5,
				"member5": 15,
			},
			options: ZADDOptions{XX: true, CH: true, GT: true},
			want:    1,
			wantErr: false,
		},
		{
			// Only update the elements where provided score is less than current score if LT flag is provided
			// Return only the new elements added by default.
			name:   "Only update the elements where provided score is less than current score if LT flag is provided",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key7",
			entries: map[string]float64{
				"member1": 3.5,
				"member4": 100.5,
				"member5": 15,
			},
			options: ZADDOptions{XX: true, LT: true},
			want:    0,
			wantErr: false,
		},
		{
			name:   "Return all the elements that were updated AND added when CH flag is provided",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key8",
			entries: map[string]float64{
				"member1": 3.5,
				"member4": 100.5,
				"member5": 15,
			},
			options: ZADDOptions{XX: true, LT: true, CH: true},
			want:    1,
			wantErr: false,
		},
		{
			name:   "Increment the member by score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key: "key9",
			entries: map[string]float64{
				"member3": 5.5,
			},
			options: ZADDOptions{INCR: true},
			want:    0,
			wantErr: false,
		},
		{
			name:        "Fail when GT/LT flag is provided alongside NX flag",
			preset:      false,
			presetValue: nil,
			key:         "key10",
			entries: map[string]float64{
				"member1": 3.5,
				"member5": 15,
			},
			options: ZADDOptions{NX: true, LT: true, CH: true},
			want:    0,
			wantErr: true,
		},
		{
			name:        "Throw error when INCR flag is passed with more than one score/member pair",
			preset:      false,
			presetValue: nil,
			key:         "key11",
			entries: map[string]float64{
				"member1": 10.5,
				"member2": 12.5,
			},
			options: ZADDOptions{INCR: true},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZADD(tt.key, tt.entries, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZADD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZADD() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZCARD(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name        string
		preset      bool
		presetValue interface{}
		key         string
		want        int
		wantErr     bool
	}{
		{
			name:   "Get cardinality of valid sorted set",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
			}),
			key:     "key1",
			want:    3,
			wantErr: false,
		},
		{
			name:        "Return 0 when trying to get cardinality from non-existent key",
			preset:      false,
			presetValue: nil,
			key:         "key2",
			want:        0,
			wantErr:     false,
		},
		{
			name:        "Return error when not a sorted set",
			preset:      true,
			presetValue: "Default value",
			key:         "key3",
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZCARD(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZCARD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZCARD() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZCOUNT(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Get entire count using infinity boundaries",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
				{Value: "member4", Score: sorted_set.Score(1083.13)},
				{Value: "member5", Score: sorted_set.Score(11)},
				{Value: "member6", Score: sorted_set.Score(math.Inf(-1))},
				{Value: "member7", Score: sorted_set.Score(math.Inf(1))},
			}),
			key:     "key1",
			min:     math.Inf(-1),
			max:     math.Inf(1),
			want:    7,
			wantErr: false,
		},
		{
			name:   "Get count of sub-set from -inf to limit",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
				{Value: "member4", Score: sorted_set.Score(1083.13)},
				{Value: "member5", Score: sorted_set.Score(11)},
				{Value: "member6", Score: sorted_set.Score(math.Inf(-1))},
				{Value: "member7", Score: sorted_set.Score(math.Inf(1))},
			}),
			key:     "key2",
			min:     math.Inf(-1),
			max:     90,
			want:    5,
			wantErr: false,
		},
		{
			name:   "Get count of sub-set from bottom boundary to +inf limit",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "member1", Score: sorted_set.Score(5.5)},
				{Value: "member2", Score: sorted_set.Score(67.77)},
				{Value: "member3", Score: sorted_set.Score(10)},
				{Value: "member4", Score: sorted_set.Score(1083.13)},
				{Value: "member5", Score: sorted_set.Score(11)},
				{Value: "member6", Score: sorted_set.Score(math.Inf(-1))},
				{Value: "member7", Score: sorted_set.Score(math.Inf(1))},
			}),
			key:     "key3",
			min:     1000,
			max:     math.Inf(1),
			want:    2,
			wantErr: false,
		},
		{
			name:        "Throw error when value at the key is not a sorted set",
			preset:      true,
			presetValue: "Default value",
			key:         "key4",
			min:         1,
			max:         10,
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZCOUNT(tt.key, tt.min, tt.max)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZCOUNT() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZCOUNT() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZDIFF(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Get the difference between 2 sorted sets without scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1},
					{Value: "two", Score: 2},
					{Value: "three", Score: 3},
					{Value: "four", Score: 4},
				}),
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "three", Score: 3},
					{Value: "four", Score: 4},
					{Value: "five", Score: 5},
					{Value: "six", Score: 6},
					{Value: "seven", Score: 7},
					{Value: "eight", Score: 8},
				}),
			},
			withscores: false,
			keys:       []string{"key1", "key2"},
			want:       map[string]float64{"one": 0, "two": 0},
			wantErr:    false,
		},
		{
			name:   "Get the difference between 2 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1},
					{Value: "two", Score: 2},
					{Value: "three", Score: 3},
					{Value: "four", Score: 4},
				}),
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "three", Score: 3},
					{Value: "four", Score: 4},
					{Value: "five", Score: 5},
					{Value: "six", Score: 6},
					{Value: "seven", Score: 7},
					{Value: "eight", Score: 8},
				}),
			},
			withscores: true,
			keys:       []string{"key3", "key4"},
			want:       map[string]float64{"one": 1, "two": 2},
			wantErr:    false,
		},
		{
			name:   "Get the difference between 3 sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key7": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			withscores: true,
			keys:       []string{"key5", "key6", "key7"},
			want:       map[string]float64{"three": 3, "four": 4, "five": 5, "six": 6},
			wantErr:    false,
		},
		{
			name:   "Return sorted set if only one key exists and is a sorted set",
			preset: true,
			presetValues: map[string]interface{}{
				"key8": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			withscores: true,
			keys:       []string{"key8", "non-existent-key-1", "non-existent-key-2", "non-existent-key-3"},
			want: map[string]float64{
				"one": 1, "two": 2, "three": 3, "four": 4,
				"five": 5, "six": 6, "seven": 7, "eight": 8,
			},
			wantErr: false,
		},
		{
			name:   "Throw error when one of the keys is not a sorted set",
			preset: true,
			presetValues: map[string]interface{}{
				"key9": "Default value",
				"key10": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key11": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			withscores: false,
			keys:       []string{"key9", "key10", "key11"},
			want:       nil,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValues {
					presetValue(server, k, v)
				}
			}
			got, err := server.ZDIFF(tt.withscores, tt.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZDIFF() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ZDIFF() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZDIFFSTORE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Get the difference between 2 sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			destination: "destination1",
			keys:        []string{"key1", "key2"},
			want:        2,
			wantErr:     false,
		},
		{
			name:   "Get the difference between 3 sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination2",
			keys:        []string{"key3", "key4", "key5"},
			want:        4,
			wantErr:     false,
		},
		{
			name:   "Return base sorted set element if base set is the only existing key provided and is a valid sorted set",
			preset: true,
			presetValues: map[string]interface{}{
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			destination: "destination3",
			keys:        []string{"key6", "non-existent-key-1", "non-existent-key-2"},
			want:        8,
			wantErr:     false,
		},
		{
			name:   "Throw error when base sorted set is not a set",
			preset: true,
			presetValues: map[string]interface{}{
				"key7": "Default value",
				"key8": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key9": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination4",
			keys:        []string{"key7", "key8", "key9"},
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValues {
					presetValue(server, k, v)
				}
			}
			got, err := server.ZDIFFSTORE(tt.destination, tt.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZDIFFSTORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZDIFFSTORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZINCRBY(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Successfully increment by int. Return the new score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:       "key1",
			increment: 5,
			member:    "one",
			want:      6,
			wantErr:   false,
		},
		{
			name:   "Successfully increment by float. Return new score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:       "key2",
			increment: 346.785,
			member:    "one",
			want:      347.785,
		},
		{
			name:        "Increment on non-existent sorted set will create the set with the member and increment as its score",
			preset:      false,
			presetValue: nil,
			key:         "key3",
			increment:   346.785,
			member:      "one",
			want:        346.785,
			wantErr:     false,
		},
		{ // 4.
			name:   "Increment score to +inf",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:       "key4",
			increment: math.Inf(1),
			member:    "one",
			want:      math.Inf(1),
			wantErr:   false,
		},
		{
			name:   "Increment score to -inf",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:       "key5",
			increment: math.Inf(-1),
			member:    "one",
			want:      math.Inf(-1),
			wantErr:   false,
		},
		{
			name:   "Incrementing score by negative increment should lower the score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:       "key6",
			increment: -2.5,
			member:    "five",
			want:      2.5,
			wantErr:   false,
		},
		{
			name:        "Return error when attempting to increment on a value that is not a valid sorted set",
			preset:      true,
			presetValue: "Default value",
			key:         "key7",
			increment:   -2.5,
			member:      "five",
			want:        0,
			wantErr:     true,
		},
		{
			name:   "Return error when trying to increment a member that already has score -inf",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: sorted_set.Score(math.Inf(-1))},
			}),
			key:       "key8",
			increment: 2.5,
			member:    "one",
			want:      0,
			wantErr:   true,
		},
		{
			name:   "Return error when trying to increment a member that already has score +inf",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: sorted_set.Score(math.Inf(1))},
			}),
			key:       "key9",
			increment: 2.5,
			member:    "one",
			want:      0,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZINCRBY(tt.key, tt.increment, tt.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZINCRBY() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZINCRBY() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZINTER(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name         string
		preset       bool
		presetValues map[string]interface{}
		keys         []string
		options      ZINTEROptions
		want         map[string]float64
		wantErr      bool
	}{
		{
			name:   "Get the intersection between 2 sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			keys:    []string{"key1", "key2"},
			options: ZINTEROptions{},
			want:    map[string]float64{"three": 0, "four": 0, "five": 0},
			wantErr: false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
				}),
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key3", "key4", "key5"},
			options: ZINTEROptions{WithScores: true},
			want:    map[string]float64{"one": 3, "eight": 24},
			wantErr: false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key7": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key8": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key6", "key7", "key8"},
			options: ZINTEROptions{Aggregate: "MIN", WithScores: true},
			want:    map[string]float64{"one": 1, "eight": 8},
			wantErr: false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate.
			preset: true,
			presetValues: map[string]interface{}{
				"key9": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key10": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key11": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key9", "key10", "key11"},
			options: ZINTEROptions{WithScores: true, Aggregate: "MAX"},
			want:    map[string]float64{"one": 1000, "eight": 800},
			wantErr: false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key12": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key13": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key14": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key12", "key13", "key14"},
			options: ZINTEROptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 5, 3}},
			want:    map[string]float64{"one": 3105, "eight": 2808},
			wantErr: false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key15": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key16": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key17": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key15", "key16", "key17"},
			options: ZINTEROptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 5, 3}},
			want:    map[string]float64{"one": 3000, "eight": 2400},
			wantErr: false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key18": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key19": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key20": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key18", "key19", "key20"},
			options: ZINTEROptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 5, 3}},
			want:    map[string]float64{"one": 5, "eight": 8},
			wantErr: false,
		},
		{
			name:   "Throw an error if there are more weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key21": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key22": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{"key21", "key22"},
			options: ZINTEROptions{Weights: []float64{1, 2, 3}},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Throw an error if there are fewer weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key23": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key24": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
				}),
				"key25": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{"key23", "key24", "key25"},
			options: ZINTEROptions{Weights: []float64{5, 4}},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Throw an error if there are no keys provided",
			preset: true,
			presetValues: map[string]interface{}{
				"key26": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
				"key27": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
				"key28": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{},
			options: ZINTEROptions{},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Throw an error if any of the provided keys are not sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key29": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key30": "Default value",
				"key31": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{"key29", "key30", "key31"},
			options: ZINTEROptions{},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "If any of the keys does not exist, return an empty array",
			preset: true,
			presetValues: map[string]interface{}{
				"key32": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key33": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"non-existent", "key32", "key33"},
			options: ZINTEROptions{},
			want:    map[string]float64{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValues {
					presetValue(server, k, v)
				}
			}
			got, err := server.ZINTER(tt.keys, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZINTER() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ZINTER() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZINTERSTORE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name         string
		preset       bool
		presetValues map[string]interface{}
		destination  string
		keys         []string
		options      ZINTERSTOREOptions
		want         int
		wantErr      bool
	}{
		{
			name:   "Get the intersection between 2 sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			destination: "destination1",
			keys:        []string{"key1", "key2"},
			options:     ZINTERSTOREOptions{},
			want:        3,
			wantErr:     false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
				}),
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination2",
			keys:        []string{"key3", "key4", "key5"},
			options:     ZINTERSTOREOptions{WithScores: true},
			want:        2,
			wantErr:     false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key7": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key8": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination3",
			keys:        []string{"key6", "key7", "key8"},
			options:     ZINTERSTOREOptions{WithScores: true, Aggregate: "MIN"},
			want:        2,
			wantErr:     false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key9": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key10": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key11": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination4",
			keys:        []string{"key9", "key10", "key11"},
			options:     ZINTERSTOREOptions{WithScores: true, Aggregate: "MAX"},
			want:        2,
			wantErr:     false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key12": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key13": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key14": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination5",
			keys:        []string{"key12", "key13", "key14"},
			options:     ZINTERSTOREOptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 5, 3}},
			want:        2,
			wantErr:     false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key15": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key16": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key17": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination6",
			keys:        []string{"key15", "key16", "key17"},
			options:     ZINTERSTOREOptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 5, 3}},
			want:        2,
			wantErr:     false,
		},
		{
			// Get the intersection between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			name:   "Get the intersection between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key18": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key19": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key20": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination7",
			keys:        []string{"key18", "key19", "key20"},
			options:     ZINTERSTOREOptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 5, 3}},
			want:        2,
			wantErr:     false,
		},
		{
			name:   "Throw an error if there are more weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key21": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key22": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			destination: "destination8",
			keys:        []string{"key21", "key22"},
			options:     ZINTERSTOREOptions{Weights: []float64{1, 2, 3}},
			want:        0,
			wantErr:     true,
		},
		{
			name:   "Throw an error if there are fewer weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key23": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key24": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
				}),
				"key25": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			destination: "destination9",
			keys:        []string{"key23", "key24"},
			options:     ZINTERSTOREOptions{Weights: []float64{5}},
			want:        0,
			wantErr:     true,
		},
		{
			name:   "Throw an error if there are no keys provided",
			preset: true,
			presetValues: map[string]interface{}{
				"key26": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
				"key27": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
				"key28": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			destination: "destination10",
			keys:        []string{},
			options:     ZINTERSTOREOptions{Weights: []float64{5, 4}},
			want:        0,
			wantErr:     true,
		},
		{
			name:   "Throw an error if any of the provided keys are not sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key29": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key30": "Default value",
				"key31": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			destination: "destination11",
			keys:        []string{"key29", "key30", "key31"},
			options:     ZINTERSTOREOptions{},
			want:        0,
			wantErr:     true,
		},
		{
			name:   "If any of the keys does not exist, return an empty array",
			preset: true,
			presetValues: map[string]interface{}{
				"key32": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key33": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination12",
			keys:        []string{"non-existent", "key32", "key33"},
			options:     ZINTERSTOREOptions{},
			want:        0,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValues {
					presetValue(server, k, v)
				}
			}
			got, err := server.ZINTERSTORE(tt.destination, tt.keys, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZINTERSTORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZINTERSTORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZLEXCOUNT(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Get entire count using infinity boundaries",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "e", Score: sorted_set.Score(1)},
				{Value: "f", Score: sorted_set.Score(1)},
				{Value: "g", Score: sorted_set.Score(1)},
				{Value: "h", Score: sorted_set.Score(1)},
				{Value: "i", Score: sorted_set.Score(1)},
				{Value: "j", Score: sorted_set.Score(1)},
				{Value: "k", Score: sorted_set.Score(1)},
			}),
			key:     "key1",
			min:     "f",
			max:     "j",
			want:    5,
			wantErr: false,
		},
		{
			name:   "Return 0 when the members do not have the same score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "a", Score: sorted_set.Score(5.5)},
				{Value: "b", Score: sorted_set.Score(67.77)},
				{Value: "c", Score: sorted_set.Score(10)},
				{Value: "d", Score: sorted_set.Score(1083.13)},
				{Value: "e", Score: sorted_set.Score(11)},
				{Value: "f", Score: sorted_set.Score(math.Inf(-1))},
				{Value: "g", Score: sorted_set.Score(math.Inf(1))},
			}),
			key:     "key2",
			min:     "a",
			max:     "b",
			want:    0,
			wantErr: false,
		},
		{
			name:        "Return 0 when the key does not exist",
			preset:      false,
			presetValue: nil,
			key:         "key3",
			min:         "a",
			max:         "z",
			want:        0,
			wantErr:     false,
		},
		{
			name:        "Return error when the value at the key is not a sorted set",
			preset:      true,
			presetValue: "Default value",
			key:         "key4",
			min:         "a",
			max:         "z",
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZLEXCOUNT(tt.key, tt.min, tt.max)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZLEXCOUNT() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZLEXCOUNT() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZMPOP(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name         string
		preset       bool
		presetValues map[string]interface{}
		keys         []string
		options      ZMPOPOptions
		want         [][]string
		wantErr      bool
	}{
		{
			name:   "Successfully pop one min element by default",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
			},
			keys:    []string{"key1"},
			options: ZMPOPOptions{},
			want: [][]string{
				{"one", "1"},
			},
			wantErr: false,
		},
		{
			name:   "Successfully pop one min element by specifying MIN",
			preset: true,
			presetValues: map[string]interface{}{
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
			},
			keys:    []string{"key2"},
			options: ZMPOPOptions{Min: true},
			want: [][]string{
				{"one", "1"},
			},
			wantErr: false,
		},
		{
			name:   "Successfully pop one max element by specifying MAX modifier",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
			},
			keys:    []string{"key3"},
			options: ZMPOPOptions{Max: true},
			want: [][]string{
				{"five", "5"},
			},
			wantErr: false,
		},
		{
			name:   "Successfully pop multiple min elements",
			preset: true,
			presetValues: map[string]interface{}{
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
				}),
			},
			keys:    []string{"key4"},
			options: ZMPOPOptions{Min: true, Count: 5},
			want: [][]string{
				{"one", "1"}, {"two", "2"}, {"three", "3"},
				{"four", "4"}, {"five", "5"},
			},
			wantErr: false,
		},
		{
			name:   "Successfully pop multiple max elements",
			preset: true,
			presetValues: map[string]interface{}{
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
				}),
			},
			keys:    []string{"key5"},
			options: ZMPOPOptions{Max: true, Count: 5},
			want:    [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
			wantErr: false,
		},
		{
			name:   "Successfully pop elements from the first set which is non-empty",
			preset: true,
			presetValues: map[string]interface{}{
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{}),
				"key7": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
				}),
			},
			keys:    []string{"key6", "key7"},
			options: ZMPOPOptions{Max: true, Count: 5},
			want:    [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
			wantErr: false,
		},
		{
			name:   "Skip the non-set items and pop elements from the first non-empty sorted set found",
			preset: true,
			presetValues: map[string]interface{}{
				"key8":  "Default value",
				"key9":  56,
				"key10": sorted_set.NewSortedSet([]sorted_set.MemberParam{}),
				"key11": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
				}),
			},
			keys:    []string{"key8", "key9", "key10", "key11"},
			options: ZMPOPOptions{Min: true, Count: 5},
			want:    [][]string{{"one", "1"}, {"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValues {
					presetValue(server, k, v)
				}
			}
			got, err := server.ZMPOP(tt.keys, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZMPOP() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !internal.CompareNestedStringArrays(got, tt.want) {
				t.Errorf("ZMPOP() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZMSCORE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Return multiple scores from the sorted set",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
				{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
				{Value: "five", Score: 5},
			}),
			key:     "key1",
			members: []string{"one", "none", "two", "one", "three", "four", "none", "five"},
			want:    []interface{}{"1.1", nil, "245", "1.1", "3", "4.055", nil, "5"},
			wantErr: false,
		},
		{
			name:        "If key does not exist, return empty array",
			preset:      false,
			presetValue: nil,
			key:         "key2",
			members:     []string{"one", "two", "three", "four"},
			want:        []interface{}{},
			wantErr:     false,
		},
		{
			name:        "Throw error when trying to find scores from elements that are not sorted sets",
			preset:      true,
			presetValue: "Default value",
			key:         "key3",
			members:     []string{"one", "two", "three"},
			want:        []interface{}{},
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZMSCORE(tt.key, tt.members...)
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
}

func TestEchoVault_ZPOP(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name        string
		preset      bool
		presetValue interface{}
		key         string
		count       int
		popFunc     func(key string, count int) ([][]string, error)
		want        [][]string
		wantErr     bool
	}{
		{
			name:   "Successfully pop one min element",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:     "key1",
			count:   1,
			popFunc: server.ZPOPMIN,
			want: [][]string{
				{"one", "1"},
			},
			wantErr: false,
		},
		{
			name:   "Successfully pop one max element",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:     "key2",
			count:   1,
			popFunc: server.ZPOPMAX,
			want:    [][]string{{"five", "5"}},
			wantErr: false,
		},
		{
			name:   "Successfully pop multiple min elements",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6},
			}),
			popFunc: server.ZPOPMIN,
			key:     "key3",
			count:   5,
			want: [][]string{
				{"one", "1"}, {"two", "2"}, {"three", "3"},
				{"four", "4"}, {"five", "5"},
			},
			wantErr: false,
		},
		{
			name:   "Successfully pop multiple max elements",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6},
			}),
			popFunc: server.ZPOPMAX,
			key:     "key4",
			count:   5,
			want:    [][]string{{"two", "2"}, {"three", "3"}, {"four", "4"}, {"five", "5"}, {"six", "6"}},
			wantErr: false,
		},
		{
			name:        "Throw an error when trying to pop from an element that's not a sorted set",
			preset:      true,
			presetValue: "Default value",
			popFunc:     server.ZPOPMIN,
			key:         "key5",
			count:       1,
			want:        [][]string{},
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
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
}

func TestEchoVault_ZRANDMEMBER(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Return multiple random elements without removing them",
			preset: true,
			key:    "key1",
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
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
			name:   "Return multiple random elements and their scores without removing them",
			preset: true,
			key:    "key2",
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2}, {Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6}, {Value: "seven", Score: 7}, {Value: "eight", Score: 8},
			}),
			count:      -5,
			withscores: true,
			want:       5,
			wantErr:    false,
		},
		{
			name:        "Return error when the source key is not a sorted set",
			preset:      true,
			key:         "key3",
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
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZRANDMEMBER(tt.key, tt.count, tt.withscores)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZRANDMEMBER() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != tt.want {
				t.Errorf("ZRANDMEMBER() got = %v, want %v", len(got), tt.want)
			}
		})
	}
}

func TestEchoVault_ZRANGE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name        string
		preset      bool
		presetValue interface{}
		key         string
		start       string
		stop        string
		options     ZRANGEOptions
		want        map[string]float64
		wantErr     bool
	}{
		{
			name:   "Get elements withing score range without score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6},
				{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
			}),
			key:     "key1",
			start:   "3",
			stop:    "7",
			options: ZRANGEOptions{ByScore: true},
			want:    map[string]float64{"three": 0, "four": 0, "five": 0, "six": 0, "seven": 0},
			wantErr: false,
		},
		{
			name:   "Get elements within score range with score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6},
				{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
			}),
			key:     "key2",
			start:   "3",
			stop:    "7",
			options: ZRANGEOptions{ByScore: true, WithScores: true},
			want:    map[string]float64{"three": 3, "four": 4, "five": 5, "six": 6, "seven": 7},
			wantErr: false,
		},
		{
			// Get elements within score range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			name:   "Get elements within score range with offset and limit",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6},
				{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
			}),
			key:     "key3",
			start:   "3",
			stop:    "7",
			options: ZRANGEOptions{WithScores: true, ByScore: true, Offset: 2, Count: 4},
			want:    map[string]float64{"three": 3, "four": 4, "five": 5},
			wantErr: false,
		},
		{
			name:   "Get elements within lex range without score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "a", Score: 1}, {Value: "e", Score: 1},
				{Value: "b", Score: 1}, {Value: "f", Score: 1},
				{Value: "c", Score: 1}, {Value: "g", Score: 1},
				{Value: "d", Score: 1}, {Value: "h", Score: 1},
			}),
			key:     "key4",
			start:   "c",
			stop:    "g",
			options: ZRANGEOptions{ByLex: true},
			want:    map[string]float64{"c": 0, "d": 0, "e": 0, "f": 0, "g": 0},
			wantErr: false,
		},
		{
			name:   "Get elements within lex range with score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "a", Score: 1}, {Value: "e", Score: 1},
				{Value: "b", Score: 1}, {Value: "f", Score: 1},
				{Value: "c", Score: 1}, {Value: "g", Score: 1},
				{Value: "d", Score: 1}, {Value: "h", Score: 1},
			}),
			key:     "key5",
			start:   "a",
			stop:    "f",
			options: ZRANGEOptions{ByLex: true, WithScores: true},
			want:    map[string]float64{"a": 1, "b": 1, "c": 1, "d": 1, "e": 1, "f": 1},
			wantErr: false,
		},
		{
			// Get elements within lex range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			name:   "Get elements within lex range with offset and limit",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "a", Score: 1}, {Value: "b", Score: 1},
				{Value: "c", Score: 1}, {Value: "d", Score: 1},
				{Value: "e", Score: 1}, {Value: "f", Score: 1},
				{Value: "g", Score: 1}, {Value: "h", Score: 1},
			}),
			key:     "key6",
			start:   "a",
			stop:    "h",
			options: ZRANGEOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
			want:    map[string]float64{"c": 1, "d": 1, "e": 1},
			wantErr: false,
		},
		{
			name:   "Return an empty map when we use BYLEX while elements have different scores",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "a", Score: 1}, {Value: "b", Score: 5},
				{Value: "c", Score: 2}, {Value: "d", Score: 6},
				{Value: "e", Score: 3}, {Value: "f", Score: 7},
				{Value: "g", Score: 4}, {Value: "h", Score: 8},
			}),
			key:     "key7",
			start:   "a",
			stop:    "h",
			options: ZRANGEOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
			want:    map[string]float64{},
			wantErr: false,
		},
		{
			name:        "Throw error when the key does not hold a sorted set",
			preset:      true,
			presetValue: "Default value",
			key:         "key10",
			start:       "a",
			stop:        "h",
			options:     ZRANGEOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
			want:        nil,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZRANGE(tt.key, tt.start, tt.stop, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZRANGE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ZRANGE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZRANGESTORE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name         string
		preset       bool
		presetValues map[string]interface{}
		destination  string
		source       string
		start        string
		stop         string
		options      ZRANGESTOREOptions
		want         int
		wantErr      bool
	}{
		{
			name:   "Get elements within score range without score",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			destination: "destination1",
			source:      "key1",
			start:       "3",
			stop:        "7",
			options:     ZRANGESTOREOptions{ByScore: true},
			want:        5,
			wantErr:     false,
		},
		{
			name:   "Get elements within score range with score",
			preset: true,
			presetValues: map[string]interface{}{
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			destination: "destination2",
			source:      "key2",
			start:       "3",
			stop:        "7",
			options:     ZRANGESTOREOptions{WithScores: true, ByScore: true},
			want:        5,
			wantErr:     false,
		},
		{
			// Get elements within score range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			name:   "Get elements within score range with offset and limit",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			destination: "destination3",
			source:      "key3",
			start:       "3",
			stop:        "7",
			options:     ZRANGESTOREOptions{ByScore: true, WithScores: true, Offset: 2, Count: 4},
			want:        3,
			wantErr:     false,
		},
		{
			name:   "Get elements within lex range without score",
			preset: true,
			presetValues: map[string]interface{}{
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "a", Score: 1}, {Value: "e", Score: 1},
					{Value: "b", Score: 1}, {Value: "f", Score: 1},
					{Value: "c", Score: 1}, {Value: "g", Score: 1},
					{Value: "d", Score: 1}, {Value: "h", Score: 1},
				}),
			},
			destination: "destination4",
			source:      "key4",
			start:       "c",
			stop:        "g",
			options:     ZRANGESTOREOptions{ByLex: true},
			want:        5,
			wantErr:     false,
		},
		{
			name:   "Get elements within lex range with score",
			preset: true,
			presetValues: map[string]interface{}{
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "a", Score: 1}, {Value: "e", Score: 1},
					{Value: "b", Score: 1}, {Value: "f", Score: 1},
					{Value: "c", Score: 1}, {Value: "g", Score: 1},
					{Value: "d", Score: 1}, {Value: "h", Score: 1},
				}),
			},
			destination: "destination5",
			source:      "key5",
			start:       "a",
			stop:        "f",
			options:     ZRANGESTOREOptions{ByLex: true, WithScores: true},
			want:        6,
			wantErr:     false,
		},
		{
			// Get elements within lex range with offset and limit.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			name:   "Get elements within lex range with offset and limit",
			preset: true,
			presetValues: map[string]interface{}{
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "a", Score: 1}, {Value: "b", Score: 1},
					{Value: "c", Score: 1}, {Value: "d", Score: 1},
					{Value: "e", Score: 1}, {Value: "f", Score: 1},
					{Value: "g", Score: 1}, {Value: "h", Score: 1},
				}),
			},
			destination: "destination6",
			source:      "key6",
			start:       "a",
			stop:        "h",
			options:     ZRANGESTOREOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
			want:        3,
			wantErr:     false,
		},
		{
			// Get elements within lex range with offset and limit + reverse the results.
			// Offset and limit are in where we start and stop counting in the original sorted set (NOT THE RESULT).
			// REV reverses the original set before getting the range.
			name:   "Get elements within lex range with offset and limit + reverse the results",
			preset: true,
			presetValues: map[string]interface{}{
				"key7": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "a", Score: 1}, {Value: "b", Score: 1},
					{Value: "c", Score: 1}, {Value: "d", Score: 1},
					{Value: "e", Score: 1}, {Value: "f", Score: 1},
					{Value: "g", Score: 1}, {Value: "h", Score: 1},
				}),
			},
			destination: "destination7",
			source:      "key7",
			start:       "a",
			stop:        "h",
			options:     ZRANGESTOREOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
			want:        3,
			wantErr:     false,
		},
		{
			name:   "Return an empty slice when we use BYLEX while elements have different scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key8": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "a", Score: 1}, {Value: "b", Score: 5},
					{Value: "c", Score: 2}, {Value: "d", Score: 6},
					{Value: "e", Score: 3}, {Value: "f", Score: 7},
					{Value: "g", Score: 4}, {Value: "h", Score: 8},
				}),
			},
			destination: "destination8",
			source:      "key8",
			start:       "a",
			stop:        "h",
			options:     ZRANGESTOREOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
			want:        0,
			wantErr:     false,
		},
		{
			name:   "Throw error when the key does not hold a sorted set",
			preset: true,
			presetValues: map[string]interface{}{
				"key9": "Default value",
			},
			destination: "destination9",
			source:      "key9",
			start:       "a",
			stop:        "h",
			options:     ZRANGESTOREOptions{WithScores: true, ByLex: true, Offset: 2, Count: 4},
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValues {
					presetValue(server, k, v)
				}
			}
			got, err := server.ZRANGESTORE(tt.destination, tt.source, tt.start, tt.stop, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZRANGESTORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZRANGESTORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZRANK(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Return element's rank from a sorted set",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5},
			}),
			key:        "key1",
			member:     "four",
			withscores: false,
			want:       map[int]float64{3: 0},
			wantErr:    false,
		},
		{
			name:   "Return element's rank from a sorted set with its score",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 100.1}, {Value: "two", Score: 245},
				{Value: "three", Score: 305.43}, {Value: "four", Score: 411.055},
				{Value: "five", Score: 500},
			}),
			key:        "key2",
			member:     "four",
			withscores: true,
			want:       map[int]float64{3: 411.055},
			wantErr:    false,
		},
		{
			name:        "If key does not exist, return nil value",
			preset:      false,
			presetValue: nil,
			key:         "key3",
			member:      "one",
			withscores:  false,
			want:        nil,
			wantErr:     false,
		},
		{
			name:   "If key exists and is a sorted set, but the member does not exist, return nil",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
				{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
				{Value: "five", Score: 5},
			}),
			key:        "key4",
			member:     "non-existent",
			withscores: false,
			want:       nil,
			wantErr:    false,
		},
		{
			name:        "Throw error when trying to find scores from elements that are not sorted sets",
			preset:      true,
			presetValue: "Default value",
			key:         "key5",
			member:      "one",
			withscores:  false,
			want:        nil,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZRANK(tt.key, tt.member, tt.withscores)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZRANK() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ZRANK() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZREM(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Successfully remove multiple elements from sorted set, skipping non-existent members",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6},
				{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
			}),
			key:     "key1",
			members: []string{"three", "four", "five", "none", "six", "none", "seven"},
			want:    5,
			wantErr: false,
		},
		{
			name:        "If key does not exist, return 0",
			preset:      false,
			presetValue: nil,
			key:         "key2",
			members:     []string{"member"},
			want:        0,
			wantErr:     false,
		},
		{
			name:        "Return error key is not a sorted set",
			preset:      true,
			presetValue: "Default value",
			key:         "key3",
			members:     []string{"member"},
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZREM(tt.key, tt.members...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZREM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZREM() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZREMRANGEBYSCORE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Successfully remove multiple elements with scores inside the provided range",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1}, {Value: "two", Score: 2},
				{Value: "three", Score: 3}, {Value: "four", Score: 4},
				{Value: "five", Score: 5}, {Value: "six", Score: 6},
				{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
			}),
			key:     "key1",
			min:     3,
			max:     7,
			want:    5,
			wantErr: false,
		},
		{
			name:    "If key does not exist, return 0",
			preset:  false,
			key:     "key2",
			min:     2,
			max:     4,
			want:    0,
			wantErr: false,
		},
		{
			name:        "Return error key is not a sorted set",
			preset:      true,
			presetValue: "Default value",
			key:         "key3",
			min:         2,
			max:         4,
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZREMRANGEBYSCORE(tt.key, tt.min, tt.max)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZREMRANGEBYSCORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZREMRANGEBYSCORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZSCORE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:   "Return score from a sorted set",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
				{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
				{Value: "five", Score: 5},
			}),
			key:     "key1",
			member:  "four",
			want:    4.055,
			wantErr: false,
		},
		{
			name:        "If key does not exist, return nil value",
			preset:      false,
			presetValue: nil,
			key:         "key2",
			member:      "one",
			want:        nil,
			wantErr:     false,
		},
		{
			name:   "If key exists and is a sorted set, but the member does not exist, return nil",
			preset: true,
			presetValue: sorted_set.NewSortedSet([]sorted_set.MemberParam{
				{Value: "one", Score: 1.1}, {Value: "two", Score: 245},
				{Value: "three", Score: 3}, {Value: "four", Score: 4.055},
				{Value: "five", Score: 5},
			}),
			key:     "key3",
			member:  "non-existent",
			want:    nil,
			wantErr: false,
		},
		{
			name:        "Throw error when trying to find scores from elements that are not sorted sets",
			preset:      true,
			presetValue: "Default value",
			key:         "key4",
			member:      "one",
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.ZSCORE(tt.key, tt.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZSCORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ZSCORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZUNION(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name         string
		preset       bool
		presetValues map[string]interface{}
		keys         []string
		options      ZUNIONOptions
		want         map[string]float64
		wantErr      bool
	}{
		{
			name:   "Get the union between 2 sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			keys:    []string{"key1", "key2"},
			options: ZUNIONOptions{},
			want: map[string]float64{
				"one": 0, "two": 0, "three": 0, "four": 0,
				"five": 0, "six": 0, "seven": 0, "eight": 0,
			},
			wantErr: false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
				}),
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 36},
				}),
			},
			keys:    []string{"key3", "key4", "key5"},
			options: ZUNIONOptions{WithScores: true},
			want: map[string]float64{
				"one": 3, "two": 4, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 24, "nine": 9,
				"ten": 10, "eleven": 11, "twelve": 24, "thirty-six": 72,
			},
			wantErr: false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MIN aggregate.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key7": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key8": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
				}),
			},
			keys:    []string{"key6", "key7", "key8"},
			options: ZUNIONOptions{WithScores: true, Aggregate: "MIN"},
			want: map[string]float64{
				"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
				"ten": 10, "eleven": 11, "twelve": 12, "thirty-six": 36,
			},
			wantErr: false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MAX aggregate.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key9": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key10": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key11": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
				}),
			},
			keys:    []string{"key9", "key10", "key11"},
			options: ZUNIONOptions{WithScores: true, Aggregate: "MAX"},
			want: map[string]float64{
				"one": 1000, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 800, "nine": 9,
				"ten": 10, "eleven": 11, "twelve": 12, "thirty-six": 72,
			},
			wantErr: false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key12": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key13": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key14": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key12", "key13", "key14"},
			options: ZUNIONOptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 2, 3}},
			want: map[string]float64{
				"one": 3102, "two": 6, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 2568,
				"nine": 27, "ten": 30, "eleven": 22, "twelve": 60, "thirty-six": 72,
			},
			wantErr: false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key15": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key16": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key17": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key15", "key16", "key17"},
			options: ZUNIONOptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 2, 3}},
			want: map[string]float64{
				"one": 3000, "two": 4, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 2400,
				"nine": 27, "ten": 30, "eleven": 22, "twelve": 36, "thirty-six": 72,
			},
			wantErr: false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key18": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key19": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key20": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"key18", "key19", "key20"},
			options: ZUNIONOptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 2, 3}},
			want: map[string]float64{
				"one": 2, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 27,
				"ten": 30, "eleven": 22, "twelve": 24, "thirty-six": 72,
			},
			wantErr: false,
		},
		{
			name:   "Throw an error if there are more weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key21": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key22": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{"key21", "key22"},
			options: ZUNIONOptions{Weights: []float64{1, 2, 3}},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Throw an error if there are fewer weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key23": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key24": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
				}),
				"key25": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{"key23", "key24", "key25"},
			options: ZUNIONOptions{Weights: []float64{5, 4}},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Throw an error if there are no keys provided",
			preset: true,
			presetValues: map[string]interface{}{
				"key26": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
				"key27": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
				"key28": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{},
			options: ZUNIONOptions{Weights: []float64{5, 4}},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "Throw an error if any of the provided keys are not sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key29": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key30": "Default value",
				"key31": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			keys:    []string{"key29", "key30", "key31"},
			options: ZUNIONOptions{},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "If any of the keys does not exist, skip it",
			preset: true,
			presetValues: map[string]interface{}{
				"key32": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key33": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			keys:    []string{"non-existent", "key32", "key33"},
			options: ZUNIONOptions{},
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
					presetValue(server, k, v)
				}
			}
			got, err := server.ZUNION(tt.keys, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZUNION() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ZUNION() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_ZUNIONSTORE(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		name         string
		preset       bool
		presetValues map[string]interface{}
		destination  string
		keys         []string
		options      ZUNIONSTOREOptions
		want         int
		wantErr      bool
	}{
		{
			name:   "Get the union between 2 sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key1": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5},
				}),
				"key2": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
			},
			destination: "destination1",
			keys:        []string{"key1", "key2"},
			options:     ZUNIONSTOREOptions{},
			want:        8,
			wantErr:     false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// By default, the SUM aggregate will be used.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key3": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key4": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 8},
				}),
				"key5": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 36},
				}),
			},
			destination: "destination2",
			keys:        []string{"key3", "key4", "key5"},
			options:     ZUNIONSTOREOptions{WithScores: true},
			want:        13,
			wantErr:     false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MIN aggregate.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key6": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key7": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key8": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
				}),
			},
			destination: "destination3",
			keys:        []string{"key6", "key7", "key8"},
			options:     ZUNIONSTOREOptions{WithScores: true, Aggregate: "MIN"},
			want:        13,
			wantErr:     false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MAX aggregate.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key9": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key10": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key11": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12}, {Value: "thirty-six", Score: 72},
				}),
			},
			destination: "destination4",
			keys:        []string{"key9", "key10", "key11"},
			options:     ZUNIONSTOREOptions{WithScores: true, Aggregate: "MAX"},
			want:        13,
			wantErr:     false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use SUM aggregate with weights modifier.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key12": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key13": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key14": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination5",
			keys:        []string{"key12", "key13", "key14"},
			options:     ZUNIONSTOREOptions{WithScores: true, Aggregate: "SUM", Weights: []float64{1, 2, 3}},
			want:        13,
			wantErr:     false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MAX aggregate with added weights.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key15": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key16": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key17": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination6",
			keys:        []string{"key15", "key16", "key17"},
			options:     ZUNIONSTOREOptions{WithScores: true, Aggregate: "MAX", Weights: []float64{1, 2, 3}},
			want:        13,
			wantErr:     false,
		},
		{
			// Get the union between 3 sorted sets with scores.
			// Use MIN aggregate with added weights.
			name:   "Get the union between 3 sorted sets with scores",
			preset: true,
			presetValues: map[string]interface{}{
				"key18": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 100}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key19": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11}, {Value: "eight", Score: 80},
				}),
				"key20": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1000}, {Value: "eight", Score: 800},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination7",
			keys:        []string{"destination7", "key18", "key19", "key20"},
			options:     ZUNIONSTOREOptions{WithScores: true, Aggregate: "MIN", Weights: []float64{1, 2, 3}},
			want:        13,
			wantErr:     false,
		},
		{
			name:   "Throw an error if there are more weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key21": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key22": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			destination: "destination8",
			keys:        []string{"key21", "key22"},
			options:     ZUNIONSTOREOptions{Weights: []float64{1, 2, 3}},
			want:        0,
			wantErr:     true,
		},
		{
			name:   "Throw an error if there are fewer weights than keys",
			preset: true,
			presetValues: map[string]interface{}{
				"key23": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key24": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
				}),
				"key25": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			destination: "destination9",
			keys:        []string{"key23", "key24", "key25"},
			options:     ZUNIONSTOREOptions{Weights: []float64{5, 4}},
			want:        0,
			wantErr:     true,
		},
		{
			name:   "Throw an error if any of the provided keys are not sorted sets",
			preset: true,
			presetValues: map[string]interface{}{
				"key29": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "three", Score: 3}, {Value: "four", Score: 4},
					{Value: "five", Score: 5}, {Value: "six", Score: 6},
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
				}),
				"key30": "Default value",
				"key31": sorted_set.NewSortedSet([]sorted_set.MemberParam{{Value: "one", Score: 1}}),
			},
			destination: "destination11",
			keys:        []string{"key29", "key30", "key31"},
			options:     ZUNIONSTOREOptions{},
			want:        0,
			wantErr:     true,
		},
		{
			name:   "If any of the keys does not exist, skip it",
			preset: true,
			presetValues: map[string]interface{}{
				"key32": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "one", Score: 1}, {Value: "two", Score: 2},
					{Value: "thirty-six", Score: 36}, {Value: "twelve", Score: 12},
					{Value: "eleven", Score: 11},
				}),
				"key33": sorted_set.NewSortedSet([]sorted_set.MemberParam{
					{Value: "seven", Score: 7}, {Value: "eight", Score: 8},
					{Value: "nine", Score: 9}, {Value: "ten", Score: 10},
					{Value: "twelve", Score: 12},
				}),
			},
			destination: "destination12",
			keys:        []string{"non-existent", "key32", "key33"},
			want:        9,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValues {
					presetValue(server, k, v)
				}
			}
			got, err := server.ZUNIONSTORE(tt.destination, tt.keys, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZUNIONSTORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ZUNIONSTORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}
