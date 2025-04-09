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
	"reflect"
	"testing"
)

func TestSugarDB_List(t *testing.T) {
	server := createSugarDB()
	t.Cleanup(func() {
		server.ShutDown()
	})

	t.Run("TestSugarDB_LLEN", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			preset      bool
			presetValue interface{}
			name        string
			key         string
			want        int
			wantErr     bool
		}{
			{
				name:        "1. If key exists and is a list, return the lists length",
				preset:      true,
				key:         "llen_key1",
				presetValue: []string{"value1", "value2", "value3", "value4"},
				want:        4,
				wantErr:     false,
			},
			{
				name:        "2. If key does not exist, return 0",
				preset:      false,
				key:         "llen_key2",
				presetValue: nil,
				want:        0,
				wantErr:     false,
			},
			{
				preset:      true,
				key:         "llen_key5",
				name:        "3. Trying to get lengths on a non-list returns error",
				presetValue: "Default value",
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.LLen(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("LLEN() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("LLEN() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_LINDEX", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			preset      bool
			presetValue interface{}
			key         string
			index       uint
			name        string
			want        string
			wantErr     bool
		}{
			{
				name:        "1. Return last element within range",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "lindex_key1",
				index:       3,
				want:        "value4",
				wantErr:     false,
			},
			{
				name:        "2. Return first element within range",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "lindex_key2",
				index:       0,
				want:        "value1",
				wantErr:     false,
			},
			{
				name:        "3. Return middle element within range",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "lindex_key3",
				index:       1,
				want:        "value2",
				wantErr:     false,
			},
			{
				name:        "4. If key does not exist, return error",
				preset:      false,
				presetValue: nil,
				key:         "lindex_key4",
				index:       0,
				want:        "",
				wantErr:     false,
			},
			{
				name:        "5. Trying to get element by index on a non-list returns error",
				preset:      true,
				presetValue: "Default value",
				key:         "lindex_key5",
				index:       0,
				want:        "",
				wantErr:     true,
			},
			{
				name:        "6. Trying to get index out of range index beyond last index",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3"},
				key:         "lindex_key6",
				index:       3,
				want:        "",
				wantErr:     false,
			},
		}
		for _, tt := range tests {
			if tt.preset {
				err := presetValue(server, context.Background(), tt.key, tt.presetValue)
				if err != nil {
					t.Error(err)
					return
				}
			}
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got, err := server.LIndex(tt.key, tt.index)
				if (err != nil) != tt.wantErr {
					t.Errorf("LINDEX() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("LINDEX() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_LMOVE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue map[string]interface{}
			source      string
			destination string
			whereFrom   string
			whereTo     string
			want        bool
			wantErr     bool
		}{
			{
				name:   "1. Move element from LEFT of left list to LEFT of right list",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_source1":      []string{"one", "two", "three"},
					"lmove_destination1": []string{"one", "two", "three"},
				},
				source:      "lmove_source1",
				destination: "lmove_destination1",
				whereFrom:   "LEFT",
				whereTo:     "LEFT",
				want:        true,
				wantErr:     false,
			},
			{
				name:   "2. Move element from LEFT of left list to RIGHT of right list",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_source2":      []string{"one", "two", "three"},
					"lmove_destination2": []string{"one", "two", "three"},
				},
				source:      "lmove_source2",
				destination: "lmove_destination2",
				whereFrom:   "LEFT",
				whereTo:     "RIGHT",
				want:        true,
				wantErr:     false,
			},
			{
				name:   "3. Move element from RIGHT of left list to LEFT of right list",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_source3":      []string{"one", "two", "three"},
					"lmove_destination3": []string{"one", "two", "three"},
				},
				source:      "lmove_source3",
				destination: "lmove_destination3",
				whereFrom:   "RIGHT",
				whereTo:     "LEFT",
				want:        true,
				wantErr:     false,
			},
			{
				name:   "4. Move element from RIGHT of left list to RIGHT of right list",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_source4":      []string{"one", "two", "three"},
					"lmove_destination4": []string{"one", "two", "three"},
				},
				source:      "lmove_source4",
				destination: "lmove_destination4",
				whereFrom:   "RIGHT",
				whereTo:     "RIGHT",
				want:        true,
				wantErr:     false,
			},
			{
				name:   "5. Throw error when the right list is non-existent",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_source5": []string{"one", "two", "three"},
				},
				source:      "lmove_source5",
				destination: "lmove_destination5",
				whereFrom:   "LEFT",
				whereTo:     "LEFT",
				want:        false,
				wantErr:     true,
			},
			{
				name:   "6. Throw error when right list in not a list",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_source6":      []string{"one", "two", "tree"},
					"lmove_destination6": "Default value",
				},
				source:      "lmove_source6",
				destination: "lmove_destination6",
				whereFrom:   "LEFT",
				whereTo:     "LEFT",
				want:        false,
				wantErr:     true,
			},
			{
				name:   "7. Throw error when left list is non-existent",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_destination7": []string{"one", "two", "three"},
				},
				source:      "lmove_source7",
				destination: "lmove_destination7",
				whereFrom:   "LEFT",
				whereTo:     "LEFT",
				want:        false,
				wantErr:     true,
			},
			{
				name:   "8. Throw error when left list is not a list",
				preset: true,
				presetValue: map[string]interface{}{
					"lmove_source8":      "Default value",
					"lmove_destination8": []string{"one", "two", "three"},
				},
				source:      "lmove_source8",
				destination: "lmove_destination8",
				whereFrom:   "LEFT",
				whereTo:     "LEFT",
				want:        false,
				wantErr:     true,
			},
			{
				name:        "9. Throw error when WHEREFROM argument is not LEFT/RIGHT",
				preset:      false,
				presetValue: map[string]interface{}{},
				source:      "lmove_source9",
				destination: "lmove_destination9",
				whereFrom:   "LEFT",
				whereTo:     "LEFT",
				want:        false,
				wantErr:     true,
			},
			{
				name:        "10. Throw error when WHERETO argument is not LEFT/RIGHT",
				preset:      false,
				presetValue: map[string]interface{}{},
				source:      "lmove_source10",
				destination: "lmove_destination10",
				whereFrom:   "LEFT",
				whereTo:     "LEFT",
				want:        false,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					for k, v := range tt.presetValue {
						err := presetValue(server, context.Background(), k, v)
						if err != nil {
							t.Error(err)
							return
						}
					}
				}
				got, err := server.LMove(tt.source, tt.destination, tt.whereFrom, tt.whereTo)
				if (err != nil) != tt.wantErr {
					t.Errorf("LMOVE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("LMOVE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_POP", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			count       uint
			popFunc     func(key string, count uint) ([]string, error)
			want        []string
			wantErr     bool
		}{
			{
				name:        "1. LPOP returns last element and removed first element from the list",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "pop_key1",
				count:       1,
				popFunc:     server.LPop,
				want:        []string{"value1"},
				wantErr:     false,
			},
			{
				name:        "2. RPOP returns last element and removed last element from the list",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "pop_key2",
				count:       1,
				popFunc:     server.RPop,
				want:        []string{"value4"},
				wantErr:     false,
			},
			{
				name:        "3. Trying to execute LPOP from a non-list item return an error",
				preset:      true,
				key:         "pop_key3",
				count:       1,
				presetValue: "Default value",
				popFunc:     server.LPop,
				want:        []string{},
				wantErr:     true,
			},
			{
				name:        "4. Trying to execute RPOP from a non-list item return an error",
				preset:      true,
				presetValue: "Default value",
				key:         "pop_key6",
				count:       1,
				popFunc:     server.RPop,
				want:        []string{},
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := tt.popFunc(tt.key, tt.count)
				if (err != nil) != tt.wantErr {
					t.Errorf("POP() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("POP() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_LPUSH", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			key         string
			values      []string
			presetValue interface{}
			lpushFunc   func(key string, values ...string) (int, error)
			want        int
			wantErr     bool
		}{
			{
				name:        "1. LPUSHX to existing list prepends the element to the list",
				preset:      true,
				presetValue: []string{"1", "2", "4", "5"},
				key:         "lpush_key1",
				values:      []string{"value1", "value2"},
				lpushFunc:   server.LPushX,
				want:        6,
				wantErr:     false,
			},
			{
				name:        "2. LPUSH on existing list prepends the elements to the list",
				preset:      true,
				presetValue: []string{"1", "2", "4", "5"},
				key:         "lpush_key2",
				values:      []string{"value1", "value2"},
				lpushFunc:   server.LPush,
				want:        6,
				wantErr:     false,
			},
			{
				name:        "3. LPUSH on non-existent list creates the list",
				preset:      false,
				presetValue: nil,
				key:         "lpush_key3",
				values:      []string{"value1", "value2"},
				lpushFunc:   server.LPush,
				want:        2,
				wantErr:     false,
			},
			{
				name:        "4. LPUSHX command returns error on non-existent list",
				preset:      false,
				presetValue: nil,
				key:         "lpush_key4",
				values:      []string{"value1", "value2"},
				lpushFunc:   server.LPushX,
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := tt.lpushFunc(tt.key, tt.values...)
				if (err != nil) != tt.wantErr {
					t.Errorf("LPUSH() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("LPUSH() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_RPUSH", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			key         string
			values      []string
			presetValue interface{}
			rpushFunc   func(key string, values ...string) (int, error)
			want        int
			wantErr     bool
		}{
			{
				name:        "1. RPUSH on non-existent list creates the list",
				preset:      false,
				presetValue: nil,
				key:         "rpush_key1",
				values:      []string{"value1", "value2"},
				rpushFunc:   server.RPush,
				want:        2,
				wantErr:     false,
			},
			{
				name:        "2. RPUSHX command returns error on non-existent list",
				preset:      false,
				presetValue: nil,
				key:         "rpush_key2",
				values:      []string{"value1", "value2"},
				rpushFunc:   server.RPushX,
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := tt.rpushFunc(tt.key, tt.values...)
				if (err != nil) != tt.wantErr {
					t.Errorf("RPUSH() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("RPUSH() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_LRANGE", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			start       int
			end         int
			want        []string
			wantErr     bool
		}{
			{
				// Return sub-list within range.
				// Both start and end indices are positive.
				// End index is greater than start index.
				name:        "1. Return sub-list within range.",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
				key:         "lrange_key1",
				start:       3,
				end:         6,
				want:        []string{"value4", "value5", "value6", "value7"},
				wantErr:     false,
			},
			{
				name:        "2. Return sub-list from start index to the end of the list when end index is -1",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
				key:         "lrange_key2",
				start:       3,
				end:         -1,
				want:        []string{"value4", "value5", "value6", "value7", "value8"},
				wantErr:     false,
			},
			{
				name:        "3. Return empty list when the end index is less than start index",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
				key:         "lrange_key3",
				start:       3,
				end:         0,
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "4. If key does not exist, return empty list",
				preset:      false,
				presetValue: nil,
				key:         "lrange_key4",
				start:       0,
				end:         2,
				want:        []string{},
				wantErr:     false,
			},

			{
				name:        "5. Error when executing command on non-list command",
				preset:      true,
				presetValue: "Default value",
				key:         "lrange_key5",
				start:       0,
				end:         3,
				want:        nil,
				wantErr:     true,
			},
			{
				name:        "6. Start index calculated from end of list when start index is less than 0",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "lrange_key6",
				start:       -3,
				end:         3,
				want:        []string{"value2", "value3", "value4"},
				wantErr:     false,
			},
			{
				name:        "7. Empty list when start index is higher than the length of the list",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3"},
				key:         "lrange_key7",
				start:       10,
				end:         11,
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "8. One element when start and end indices are equal",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3"},
				key:         "lrange_key8",
				start:       1,
				end:         1,
				want:        []string{"value2"},
				wantErr:     false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.LRange(tt.key, tt.start, tt.end)
				if (err != nil) != tt.wantErr {
					t.Errorf("LRANGE() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("LRANGE() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_LREM", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			count       int
			value       string
			want        int
			wantErr     bool
		}{
			{
				name:        "1. Remove the first 3 elements that appear in the list",
				preset:      true,
				presetValue: []string{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
				key:         "lrem_key1",
				count:       3,
				value:       "4",
				want:        3,
				wantErr:     false,
			},
			{
				name:        "2. Remove the last 3 elements that appear in the list",
				preset:      true,
				presetValue: []string{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
				key:         "lrem_key2",
				count:       -3,
				value:       "4",
				want:        3,
				wantErr:     false,
			},
			{
				name:        "3. Throw error on non-list item",
				preset:      true,
				presetValue: "Default value",
				key:         "lrem_key8",
				count:       0,
				value:       "value1",
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.LRem(tt.key, tt.count, tt.value)
				if (err != nil) != tt.wantErr {
					t.Errorf("LREM() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("LREM() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_LSET", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			index       int
			value       string
			want        bool
			wantErr     bool
		}{
			{
				name:        "1. Return last element within range",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "lset_key1",
				index:       3,
				value:       "new-value",
				want:        true,
				wantErr:     false,
			},
			{
				name:        "2. Return first element within range",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "lset_key2",
				index:       0,
				value:       "new-value",
				want:        true,
				wantErr:     false,
			},
			{
				name:        "3. Return middle element within range",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "lset_key3",
				index:       1,
				value:       "new-value",
				want:        true,
				wantErr:     false,
			},
			{
				name:        "4. If key does not exist, return error",
				preset:      false,
				presetValue: nil,
				key:         "lset_key4",
				index:       0,
				value:       "element",
				want:        false,
				wantErr:     true,
			},
			{
				name:        "5. Trying to get element by index on a non-list returns error",
				preset:      true,
				presetValue: "Default value",
				key:         "lset_key5",
				index:       0,
				value:       "element",
				want:        false,
				wantErr:     true,
			},
			{
				name:        "6. Trying to get index out of range index beyond last index",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3"},
				key:         "lset_key6",
				index:       3,
				value:       "element",
				want:        false,
				wantErr:     true,
			},
			{
				name:        "7. Trying to get index out of range with negative index",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3"},
				key:         "lset_key7",
				index:       -4,
				value:       "element",
				want:        false,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.LSet(tt.key, tt.index, tt.value)
				if (err != nil) != tt.wantErr {
					t.Errorf("LSET() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("LSET() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_LTRIM", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			preset      bool
			presetValue interface{}
			key         string
			start       int
			end         int
			want        bool
			wantErr     bool
		}{
			{
				// Return trim within range.
				// Both start and end indices are positive.
				// End index is greater than start index.
				name:        "1. Return trim within range",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
				key:         "ltrim_key1",
				start:       3,
				end:         6,
				want:        true,
				wantErr:     false,
			},
			{
				name:        "2. Return element from start index to end index when end index is greater than length of the list",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
				key:         "ltrim_key2",
				start:       5,
				end:         -1,
				want:        true,
				wantErr:     false,
			},
			{
				name:        "3. Return false when end index is smaller than start index.",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "ltrim_key3",
				start:       3,
				end:         1,
				want:        true,
				wantErr:     false,
			},
			{
				name:        "4. If key does not exist, return true",
				preset:      false,
				presetValue: nil,
				key:         "ltrim_key4",
				start:       0,
				end:         2,
				want:        true,
				wantErr:     false,
			},
			{
				name:        "5. Trying to get element by index on a non-list returns error",
				preset:      true,
				presetValue: "Default value",
				key:         "ltrim_key5",
				start:       0,
				end:         3,
				want:        false,
				wantErr:     true,
			},
			{
				name:        "6. Trim from the end when start index is less than 0",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3", "value4"},
				key:         "ltrim_key6",
				start:       -3,
				end:         3,
				want:        true,
				wantErr:     false,
			},
			{
				name:        "7. Return true when start index is higher than the length of the list",
				preset:      true,
				presetValue: []string{"value1", "value2", "value3"},
				key:         "ltrim_key7",
				start:       10,
				end:         11,
				want:        true,
				wantErr:     false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.preset {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.LTrim(tt.key, tt.start, tt.end)
				if (err != nil) != tt.wantErr {
					t.Errorf("LTRIM() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("LTRIM() got = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
