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
	"github.com/echovault/sugardb/internal/modules/set"
	"reflect"
	"slices"
	"testing"
)

func TestSugarDB_Set(t *testing.T) {
	server := createSugarDB()

	t.Cleanup(func() {
		server.ShutDown()
	})

	t.Run("TestSugarDB_SADD", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			members     []string
			want        int
			wantErr     bool
		}{
			{
				name:        "1. Create new set on a non-existent key, return count of added elements",
				presetValue: nil,
				key:         "sadd_key1",
				members:     []string{"one", "two", "three", "four"},
				want:        4,
				wantErr:     false,
			},
			{
				name:        "2. Add members to an exiting set, skip members that already exist in the set, return added count",
				presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
				key:         "sadd_key2",
				members:     []string{"three", "four", "five", "six", "seven"},
				want:        3,
				wantErr:     false,
			},
			{
				name:        "2. Throw error when trying to add to a key that does not hold a set",
				presetValue: "Default value",
				key:         "sadd_key3",
				members:     []string{"member"},
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
				got, err := server.SAdd(tt.key, tt.members...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SADD() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SADD() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SCARD", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			want        int
			wantErr     bool
		}{
			{
				name:        "1. Get cardinality of valid set",
				presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
				key:         "scard_key1",
				want:        4,
				wantErr:     false,
			},
			{
				name:        "2. Return 0 when trying to get cardinality on non-existent key",
				presetValue: nil,
				key:         "scard_key2",
				want:        0,
				wantErr:     false,
			},
			{
				name:        "3. Throw error when trying to get cardinality of a value that is not a set",
				presetValue: "Default value",
				key:         "scard_key3",
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
				got, err := server.SCard(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("SCARD() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SCARD() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SDIFF", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			keys         []string
			want         []string
			wantErr      bool
		}{
			{
				name: "1. Get the difference between 2 sets",
				presetValues: map[string]interface{}{
					"sdiff_key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"sdiff_key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
				},
				keys:    []string{"sdiff_key1", "sdiff_key2"},
				want:    []string{"one", "two"},
				wantErr: false,
			},
			{
				name: "2. Get the difference between 3 sets",
				presetValues: map[string]interface{}{
					"sdiff_key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sdiff_key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sdiff_key5": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sdiff_key3", "sdiff_key4", "sdiff_key5"},
				want:    []string{"three", "four", "five", "six"},
				wantErr: false,
			},
			{
				name: "3. Return base set element if base set is the only valid set",
				presetValues: map[string]interface{}{
					"sdiff_key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sdiff_key7": "Default value",
					"sdiff_key8": 123456789,
				},
				keys:    []string{"sdiff_key6", "sdiff_key7", "sdiff_key8"},
				want:    []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
				wantErr: false,
			},
			{
				name: "4. Throw error when base set is not a set",
				presetValues: map[string]interface{}{
					"sdiff_key9":  "Default value",
					"sdiff_key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sdiff_key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sdiff_key9", "sdiff_key10", "sdiff_key11"},
				want:    nil,
				wantErr: true,
			},
			{
				name: "5. Throw error when base set is non-existent",
				presetValues: map[string]interface{}{
					"sdiff_key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sdiff_key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sdiff_non-existent", "sdiff_key7", "sdiff_key8"},
				want:    nil,
				wantErr: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SDiff(tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SDIFF() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("SDIFF() got = %v, want %v", got, tt.want)
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("SDIFF() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_SDIFFSTORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			destination  string
			keys         []string
			want         int
			wantErr      bool
		}{
			{
				name: "1. Get the difference between 2 sets",
				presetValues: map[string]interface{}{
					"sdiffstore_key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"sdiffstore_key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
				},
				destination: "sdiffstore_destination1",
				keys:        []string{"sdiffstore_key1", "sdiffstore_key2"},
				want:        2,
				wantErr:     false,
			},
			{
				name: "2. Get the difference between 3 sets",
				presetValues: map[string]interface{}{
					"sdiffstore_key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sdiffstore_key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sdiffstore_key5": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				destination: "sdiffstore_destination2",
				keys:        []string{"sdiffstore_key3", "sdiffstore_key4", "sdiffstore_key5"},
				want:        4,
				wantErr:     false,
			},
			{
				name: "3. Return base set element if base set is the only valid set",
				presetValues: map[string]interface{}{
					"sdiffstore_key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sdiffstore_key7": "Default value",
					"sdiffstore_key8": 123456789,
				},
				destination: "sdiffstore_destination3",
				keys:        []string{"sdiffstore_key6", "sdiffstore_key7", "sdiffstore_key8"},
				want:        8,
				wantErr:     false,
			},
			{
				name: "4. Throw error when base set is not a set",
				presetValues: map[string]interface{}{
					"sdiffstore_key9":  "Default value",
					"sdiffstore_key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sdiffstore_key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				destination: "sdiffstore_destination4",
				keys:        []string{"sdiffstore_key9", "sdiffstore_key10", "sdiffstore_key11"},
				want:        0,
				wantErr:     true,
			},
			{
				name: "5. Throw error when base set is non-existent",
				presetValues: map[string]interface{}{
					"sdiffstore_key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sdiffstore_key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				destination: "sdiffstore_destination5",
				keys:        []string{"sdiffstore_non-existent", "sdiffstore_key7", "sdiffstore_key8"},
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SDiffStore(tt.destination, tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SDIFFSTORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SDIFFSTORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SINTER", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			keys         []string
			want         []string
			wantErr      bool
		}{
			{
				name: "1. Get the intersection between 2 sets",
				presetValues: map[string]interface{}{
					"sinter_key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"sinter_key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
				},
				keys:    []string{"sinter_key1", "sinter_key2"},
				want:    []string{"three", "four", "five"},
				wantErr: false,
			},
			{
				name: "2. Get the intersection between 3 sets",
				presetValues: map[string]interface{}{
					"sinter_key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sinter_key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
					"sinter_key5": set.NewSet([]string{"one", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sinter_key3", "sinter_key4", "sinter_key5"},
				want:    []string{"one", "eight"},
				wantErr: false,
			},
			{
				name: "3. Throw an error if any of the provided keys are not sets",
				presetValues: map[string]interface{}{
					"sinter_key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sinter_key7": "Default value",
					"sinter_key8": set.NewSet([]string{"one"}),
				},
				keys:    []string{"sinter_key6", "sinter_key7", "sinter_key8"},
				want:    nil,
				wantErr: true,
			},
			{
				name: "4. Throw error when base set is not a set",
				presetValues: map[string]interface{}{
					"sinter_key9":  "Default value",
					"sinter_key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sinter_key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sinter_key9", "sinter_key10", "sinter_key11"},
				want:    nil,
				wantErr: true,
			},
			{
				name: "5. If any of the keys does not exist, return an empty array",
				presetValues: map[string]interface{}{
					"sinter_key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sinter_key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sinter_non-existent", "sinter_key12", "sinter_key13"},
				want:    []string{},
				wantErr: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SInter(tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SINTER() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("SINTER() got = %v, want %v", got, tt.want)
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("SINTER() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_SINTERCARD", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			keys         []string
			limit        uint
			want         int
			wantErr      bool
		}{
			{
				name: "1. Get the full intersect cardinality between 2 sets",
				presetValues: map[string]interface{}{
					"sintercard_key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"sintercard_key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
				},
				keys:    []string{"sintercard_key1", "sintercard_key2"},
				limit:   0,
				want:    3,
				wantErr: false,
			},
			{
				name: "2. Get an intersect cardinality between 2 sets with a limit",
				presetValues: map[string]interface{}{
					"sintercard_key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"}),
					"sintercard_key4": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"}),
				},
				keys:    []string{"sintercard_key3", "sintercard_key4"},
				limit:   3,
				want:    3,
				wantErr: false,
			},
			{
				name: "3. Get the full intersect cardinality between 3 sets",
				presetValues: map[string]interface{}{
					"sintercard_key5": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sintercard_key6": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
					"sintercard_key7": set.NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sintercard_key5", "sintercard_key6", "sintercard_key7"},
				limit:   0,
				want:    2,
				wantErr: false,
			},
			{
				name: "4. Get the intersection of 3 sets with a limit",
				presetValues: map[string]interface{}{
					"sintercard_key8":  set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sintercard_key9":  set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
					"sintercard_key10": set.NewSet([]string{"one", "two", "seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sintercard_key8", "sintercard_key9", "sintercard_key10"},
				limit:   2,
				want:    2,
				wantErr: false,
			},
			{
				name: "5. Return error if any of the keys is non-existent",
				presetValues: map[string]interface{}{
					"sintercard_key11": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sintercard_key13": set.NewSet([]string{"one"}),
				},
				keys:    []string{"sintercard_key11", "sintercard_key12", "sintercard_key13"},
				limit:   0,
				want:    0,
				wantErr: false,
			},
			{
				name: "6. Throw error when one of the keys is not a valid set",
				presetValues: map[string]interface{}{
					"sintercard_key14": "Default value",
					"sintercard_key15": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sintercard_key16": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sintercard_key14", "sintercard_key15", "sintercard_key16"},
				want:    0,
				wantErr: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SInterCard(tt.keys, tt.limit)
				if (err != nil) != tt.wantErr {
					t.Errorf("SINTERCARD() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SINTERCARD() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SINTERSTORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			destination  string
			keys         []string
			want         int
			wantErr      bool
		}{
			{
				name: "1. Get the intersection between 2 sets and store it at the destination",
				presetValues: map[string]interface{}{
					"sinterstore_key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"sinterstore_key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
				},
				destination: "sinterstore_destination1",
				keys:        []string{"sinterstore_key1", "sinterstore_key2"},
				want:        3,
				wantErr:     false,
			},
			{
				name: "2. Get the intersection between 3 sets and store it at the destination key",
				presetValues: map[string]interface{}{
					"sinterstore_key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sinterstore_key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
					"sinterstore_key5": set.NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
				},
				destination: "sinterstore_destination2",
				keys:        []string{"sinterstore_key3", "sinterstore_key4", "sinterstore_key5"},
				want:        2,
				wantErr:     false,
			},
			{
				name: "3. Throw error when any of the keys is not a set",
				presetValues: map[string]interface{}{
					"sinterstore_key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sinterstore_key7": "Default value",
					"sinterstore_key8": set.NewSet([]string{"one"}),
				},
				destination: "sinterstore_destination3",
				keys:        []string{"sinterstore_key6", "sinterstore_key7", "sinterstore_key8"},
				want:        0,
				wantErr:     true,
			},
			{
				name: "4. Throw error when base set is not a set",
				presetValues: map[string]interface{}{
					"sinterstore_key9":  "Default value",
					"sinterstore_key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sinterstore_key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				destination: "sinterstore_destination4",
				keys:        []string{"sinterstore_key9", "sinterstore_key10", "sinterstore_key11"},
				want:        0,
				wantErr:     true,
			},
			{
				name: "5. Return an empty intersection if one of the keys does not exist",
				presetValues: map[string]interface{}{
					"sinterstore_key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sinterstore_key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				destination: "sinterstore_destination5",
				keys:        []string{"sinterstore_non-existent", "sinterstore_key12", "sinterstore_key13"},
				want:        0,
				wantErr:     false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SInterStore(tt.destination, tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SINTERSTORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SINTERSTORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SISMEMBER", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			member      string
			want        bool
			wantErr     bool
		}{
			{
				name:        "1. Return true when element is a member of the set",
				presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
				key:         "sismember_key1",
				member:      "three",
				want:        true,
				wantErr:     false,
			},
			{
				name:        "2. Return false when element is not a member of the set",
				presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
				key:         "sismember_key2",
				member:      "five",
				want:        false,
				wantErr:     false,
			},
			{
				name:        "3. Throw error when trying to assert membership when the key does not hold a valid set",
				presetValue: "Default value",
				key:         "sismember_key3",
				member:      "one",
				want:        false,
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
				got, err := server.SisMember(tt.key, tt.member)
				if (err != nil) != tt.wantErr {
					t.Errorf("SISMEMBER() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SISMEMBER() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SMEMBERS", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			want        []string
			wantErr     bool
		}{
			{
				name:        "1. Return all the members of the set",
				key:         "smembers_key1",
				presetValue: set.NewSet([]string{"one", "two", "three", "four", "five"}),
				want:        []string{"one", "two", "three", "four", "five"},
				wantErr:     false,
			},
			{
				name:        "2. If the key does not exist, return an empty array",
				key:         "smembers_key2",
				presetValue: nil,
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "3. Throw error when the provided key is not a set",
				key:         "smembers_key3",
				presetValue: "Default value",
				want:        nil,
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
				got, err := server.SMembers(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("SMEMBERS() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("SMEMBERS() got = %v, want %v", got, tt.want)
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("SMEMBERS() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_SMISMEMBER", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			members     []string
			want        []bool
			wantErr     bool
		}{
			{
				// Return set membership status for multiple elements (true for present and false for absent).
				// The placement of the membership status flag should be consistent with the order the elements
				// are in within the original command
				name:        "1. Return set membership status for multiple elements",
				presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven"}),
				key:         "smismember_key1",
				members:     []string{"three", "four", "five", "six", "eight", "nine", "seven"},
				want:        []bool{true, true, true, true, false, false, true},
				wantErr:     false,
			},
			{
				name:        "2. If the set key does not exist, return an array of zeroes as long as the list of members",
				presetValue: nil,
				key:         "smismember_key2",
				members:     []string{"one", "two", "three", "four"},
				want:        []bool{false, false, false, false},
				wantErr:     false,
			},
			{
				name:        "3. Throw error when trying to assert membership when the key does not hold a valid set",
				presetValue: "Default value",
				key:         "smismember_key3",
				members:     []string{"one"},
				want:        nil,
				wantErr:     true,
			},
			{
				name:        "4. Throw error for empty member slice",
				presetValue: nil,
				key:         "smismember_key4",
				members:     []string{},
				want:        nil,
				wantErr:     true,
			},
			{
				name:        "5. Throw error for nil member slice",
				presetValue: nil,
				key:         "smismember_key4",
				members:     nil,
				want:        nil,
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
				got, err := server.SMisMember(tt.key, tt.members...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SMISMEMBER() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("SMISMEMBER() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SMOVE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			source       string
			destination  string
			member       string
			want         bool
			wantErr      bool
		}{
			{
				name: "1. Return true after a successful move of a member from source set to destination set",
				presetValues: map[string]interface{}{
					"smove_source1":      set.NewSet([]string{"one", "two", "three", "four"}),
					"smove_destination1": set.NewSet([]string{"five", "six", "seven", "eight"}),
				},
				source:      "smove_source1",
				destination: "smove_destination1",
				member:      "four",
				want:        true,
				wantErr:     false,
			},
			{
				name: "2. Return false when trying to move a member from source set to destination set when it doesn't exist in source",
				presetValues: map[string]interface{}{
					"smove_source2":      set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"smove_destination2": set.NewSet([]string{"five", "six", "seven", "eight"}),
				},
				source:      "smove_source2",
				destination: "smove_destination2",
				member:      "six",
				want:        false,
				wantErr:     false,
			},
			{
				name: "3. Return error when the source key is not a set",
				presetValues: map[string]interface{}{
					"smove_source3":      "Default value",
					"smove_destination3": set.NewSet([]string{"five", "six", "seven", "eight"}),
				},
				source:      "smove_source3",
				destination: "smove_destination3",
				member:      "five",
				want:        false,
				wantErr:     true,
			},
			{
				name: "4. Return error when the destination key is not a set",
				presetValues: map[string]interface{}{
					"smove_source4":      set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"smove_destination4": "Default value",
				},
				source:      "smove_source4",
				destination: "smove_destination4",
				member:      "five",
				want:        false,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SMove(tt.source, tt.destination, tt.member)
				if (err != nil) != tt.wantErr {
					t.Errorf("SMOVE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SMOVE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SPOP", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			count       uint
			want        []string
			wantErr     bool
		}{
			{
				name:        "1. Return multiple popped elements and modify the set",
				key:         "spop_key1",
				presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				count:       3,
				want:        []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
				wantErr:     false,
			},
			{
				name:        "2. Return error when the source key is not a set",
				key:         "spop_key2",
				presetValue: "Default value",
				count:       1,
				want:        nil,
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
				got, err := server.SPop(tt.key, tt.count)
				if (err != nil) != tt.wantErr {
					t.Errorf("SPOP() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("SPOP() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_SRANDMEMBER", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			count       int
			wantCount   int
			wantErr     bool
		}{
			{
				// Return multiple random elements without removing them
				// Count is positive, do not allow repeated elements
				name:        "1. Return multiple random elements without removing them",
				key:         "srandmember_key1",
				presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				count:       3,
				wantCount:   3,
				wantErr:     false,
			},
			{
				// Return multiple random elements without removing them
				// Count is negative, so allow repeated numbers
				name:        "2. Return multiple random elements without removing them",
				key:         "srandmember_key2",
				presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				count:       -5,
				wantCount:   5,
				wantErr:     false,
			},
			{
				name:        "3. Return error when the source key is not a set",
				key:         "srandmember_key3",
				presetValue: "Default value",
				count:       1,
				wantCount:   0,
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
				got, err := server.SRandMember(tt.key, tt.count)
				if (err != nil) != tt.wantErr {
					t.Errorf("SRANDMEMBER() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != tt.wantCount {
					t.Errorf("SRANDMEMBER() got = %v, want %v", len(got), tt.wantCount)
				}
				if tt.count > 0 {
					s := set.NewSet(got)
					if s.Cardinality() != len(got) {
						t.Errorf("SRANDMEMBER - UNIQUE () got = %v, want %v", len(got), s.Cardinality())
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_SREM", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			members     []string
			want        int
			wantErr     bool
		}{
			{
				name:        "1. Remove multiple elements and return the number of elements removed",
				key:         "srem_key1",
				presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				members:     []string{"one", "two", "three", "nine"},
				want:        3,
				wantErr:     false,
			},
			{
				name:        "2. If key does not exist, return 0",
				key:         "srem_key2",
				presetValue: nil,
				members:     []string{"one", "two", "three", "nine"},
				want:        0,
				wantErr:     false,
			},
			{
				name:        "3. Return error when the source key is not a set",
				key:         "srem_key3",
				presetValue: "Default value",
				members:     []string{"one"},
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
				got, err := server.SRem(tt.key, tt.members...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SREM() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SREM() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_SUNION", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			keys         []string
			want         []string
			wantErr      bool
		}{
			{
				name: "1. Get the union between 2 sets",
				presetValues: map[string]interface{}{
					"sunion_key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"sunion_key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
				},
				keys:    []string{"sunion_key1", "sunion_key2"},
				want:    []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
				wantErr: false,
			},
			{
				name: "2. Get the union between 3 sets",
				presetValues: map[string]interface{}{
					"sunion_key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sunion_key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
					"sunion_key5": set.NewSet([]string{"one", "eight", "nine", "ten", "twelve"}),
				},
				keys: []string{"sunion_key3", "sunion_key4", "sunion_key5"},
				want: []string{
					"one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
					"ten", "eleven", "twelve", "thirty-six",
				},
				wantErr: false,
			},
			{
				name: "3. Throw an error if any of the provided keys are not sets",
				presetValues: map[string]interface{}{
					"sunion_key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sunion_key7": "Default value",
					"sunion_key8": set.NewSet([]string{"one"}),
				},
				keys:    []string{"sunion_key6", "sunion_key7", "sunion_key8"},
				want:    nil,
				wantErr: true,
			},
			{
				name: "4. Throw error any of the keys does not hold a set",
				presetValues: map[string]interface{}{
					"sunion_key9":  "Default value",
					"sunion_key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
					"sunion_key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
				},
				keys:    []string{"sunion_key9", "sunion_key10", "sunion_key11"},
				want:    nil,
				wantErr: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SUnion(tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SUNION() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("SUNION() got = %v, want %v", got, tt.want)
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("SUNION() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_SUNIONSTORE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValues map[string]interface{}
			destination  string
			keys         []string
			want         int
			wantErr      bool
		}{
			{
				name: "1. Get the intersection between 2 sets and store it at the destination",
				presetValues: map[string]interface{}{
					"sunionstore_key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
					"sunionstore_key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
				},
				destination: "sunionstore_destination1",
				keys:        []string{"sunionstore_key1", "sunionstore_key2"},
				want:        8,
				wantErr:     false,
			},
			{
				name: "2. Get the intersection between 3 sets and store it at the destination key",
				presetValues: map[string]interface{}{
					"sunionstore_key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sunionstore_key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
					"sunionstore_key5": set.NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
				},
				destination: "sunionstore_destination2",
				keys:        []string{"sunionstore_key3", "sunionstore_key4", "sunionstore_key5"},
				want:        13,
				wantErr:     false,
			},
			{
				name: "3. Throw error when any of the keys is not a set",
				presetValues: map[string]interface{}{
					"sunionstore_key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
					"sunionstore_key7": "Default value",
					"sunionstore_key8": set.NewSet([]string{"one"}),
				},
				destination: "sunionstore_destination3",
				keys:        []string{"sunionstore_key6", "sunionstore_key7", "sunionstore_key8"},
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.presetValues != nil {
					for k, v := range tt.presetValues {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.SUnionStore(tt.destination, tt.keys...)
				if (err != nil) != tt.wantErr {
					t.Errorf("SUNIONSTORE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("SUNIONSTORE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
