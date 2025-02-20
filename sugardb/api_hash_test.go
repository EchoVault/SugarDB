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
	"github.com/echovault/sugardb/internal/modules/hash"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestSugarDB_Hash(t *testing.T) {
	server := createSugarDB()
	t.Cleanup(func() {
		server.ShutDown()
	})

	t.Run("TestSugarDB_HDEL", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			fields      []string
			want        int
			wantErr     bool
		}{
			{
				name: "1. Return count of deleted fields in the specified hash",
				key:  "hdel_key1",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
					"field7": {Value: "value7"},
				},
				fields:  []string{"field1", "field2", "field3", "field4", "field5", "field6"},
				want:    3,
				wantErr: false,
			},
			{
				name: "2. 0 response when passing delete fields that are non-existent on valid hash",
				key:  "hdel_key2",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: "value2"},
					"field3": {Value: "value3"},
				},
				fields:  []string{"field4", "field5", "field6"},
				want:    0,
				wantErr: false,
			},
			{
				name:        "3. 0 response when trying to call HDEL on non-existent key",
				key:         "hdel_key3",
				presetValue: nil,
				fields:      []string{"field1"},
				want:        0,
				wantErr:     false,
			},
			{
				name:        "4. Trying to get lengths on a non hash map returns error",
				presetValue: "Default value",
				key:         "hdel_key5",
				fields:      []string{"field1"},
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HDel(tt.key, tt.fields...)
				if (err != nil) != tt.wantErr {
					t.Errorf("HDEL() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("HDEL() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HEXISTS", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			field       string
			want        bool
			wantErr     bool
		}{
			{
				name: "1. Return 1 if the field exists in the hash",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				key:     "hexists_key1",
				field:   "field1",
				want:    true,
				wantErr: false,
			},
			{
				name:        "2. False response when trying to call HEXISTS on non-existent key",
				presetValue: hash.Hash{},
				key:         "hexists_key2",
				field:       "field1",
				want:        false,
				wantErr:     false,
			},
			{
				name:        "3. Trying to get lengths on a non hash map returns error",
				presetValue: "Default value",
				key:         "hexists_key5",
				field:       "field1",
				want:        false,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HExists(tt.key, tt.field)
				if (err != nil) != tt.wantErr {
					t.Errorf("HEXISTS() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("HEXISTS() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HGETALL", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			want        []string
			wantErr     bool
		}{
			{
				name: "1. Return an array containing all the fields and values of the hash",
				key:  "hgetall_key1",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				want:    []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
				wantErr: false,
			},
			{
				name:        "2. Empty array response when trying to call HGETALL on non-existent key",
				key:         "hgetall_key2",
				presetValue: hash.Hash{},
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "3. Trying to get lengths on a non hash map returns error",
				key:         "hgetall_key5",
				presetValue: "Default value",
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HGetAll(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("HGETALL() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("HGETALL() got = %v, want %v", got, tt.want)
					return
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("HGETALL() got = %v, want %v", got, tt.want)
						return
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_HINCRBY", func(t *testing.T) {
		t.Parallel()

		const (
			HINCRBY      = "HINCRBY"
			HINCRBYFLOAT = "HINCRBYFLOAT"
		)

		tests := []struct {
			name            string
			presetValue     interface{}
			incr_type       string
			key             string
			field           string
			increment_int   int
			increment_float float64
			want            float64
			wantErr         bool
		}{
			{
				name:          "1. Increment by integer on non-existent hash should create a new one",
				presetValue:   nil,
				incr_type:     HINCRBY,
				key:           "hincrby_key1",
				field:         "field1",
				increment_int: 1,
				want:          1,
				wantErr:       false,
			},
			{
				name:            "2. Increment by float on non-existent hash should create one",
				presetValue:     nil,
				incr_type:       HINCRBYFLOAT,
				key:             "hincrby_key2",
				field:           "field1",
				increment_float: 3.142,
				want:            3.142,
				wantErr:         false,
			},
			{
				name:          "3. Increment by integer on existing hash",
				presetValue:   hash.Hash{"field1": {Value: 1}},
				incr_type:     HINCRBY,
				key:           "hincrby_key3",
				field:         "field1",
				increment_int: 10,
				want:          11,
				wantErr:       false,
			},
			{
				name:            "4. Increment by float on an existing hash",
				presetValue:     hash.Hash{"field1": {Value: 3.142}},
				incr_type:       HINCRBYFLOAT,
				key:             "hincrby_key4",
				field:           "field1",
				increment_float: 3.142,
				want:            6.284,
				wantErr:         false,
			},
			{
				name:          "5. Error when trying to increment on a key that is not a hash",
				presetValue:   "Default value",
				incr_type:     HINCRBY,
				key:           "hincrby_key9",
				field:         "field1",
				increment_int: 3,
				want:          0,
				wantErr:       true,
			},
			{
				name:          "6. Error when trying to increment a hash field that is not a number",
				presetValue:   hash.Hash{"field1": {Value: "value1"}},
				incr_type:     HINCRBY,
				key:           "hincrby_key10",
				field:         "field1",
				increment_int: 1,
				want:          0,
				wantErr:       true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				var got float64
				var err error
				if tt.incr_type == HINCRBY {
					got, err = server.HIncrBy(tt.key, tt.field, tt.increment_int)
					if (err != nil) != tt.wantErr {
						t.Errorf("HINCRBY() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
				}
				if tt.incr_type == HINCRBYFLOAT {
					got, err = server.HIncrByFloat(tt.key, tt.field, tt.increment_float)
					if (err != nil) != tt.wantErr {
						t.Errorf("HINCRBYFLOAT() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
				}
				if got != tt.want {
					t.Errorf("HINCRBY/HINCRBYFLOAT() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HKEYS", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			want        []string
			wantErr     bool
		}{
			{
				name: "1. Return an array containing all the keys of the hash",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				key:     "hkeys_key1",
				want:    []string{"field1", "field2", "field3"},
				wantErr: false,
			},
			{
				name:        "2. Empty array response when trying to call HKEYS on non-existent key",
				presetValue: hash.Hash{},
				key:         "hkeys_key2",
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "3. Trying to get lengths on a non hash map returns error",
				presetValue: "Default value",
				key:         "hkeys_key3",
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HKeys(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("HKEYS() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("HKEYS() got = %v, want %v", got, tt.want)
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("HKEYS() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_HLEN", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			want        int
			wantErr     bool
		}{
			{
				name: "1. Return the correct length of the hash",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				key:     "hlen_key1",
				want:    3,
				wantErr: false,
			},
			{
				name:        "2. 0 Response when trying to call HLEN on non-existent key",
				presetValue: nil,
				key:         "hlen_key2",
				want:        0,
				wantErr:     false,
			},
			{
				name:        "3. Trying to get lengths on a non hash map returns error",
				presetValue: "Default value",
				key:         "hlen_key5",
				want:        0,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HLen(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("HLEN() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("HLEN() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HRANDFIELD", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			options     HRandFieldOptions
			wantCount   int
			want        []string
			wantErr     bool
		}{
			{
				name: "1. Get a random field",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				key:       "hrandfield_key1",
				options:   HRandFieldOptions{Count: 1},
				wantCount: 1,
				want:      []string{"field1", "field2", "field3"},
				wantErr:   false,
			},
			{
				name: "2. Get a random field with a value",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				key:       "hrandfield_key2",
				options:   HRandFieldOptions{WithValues: true, Count: 1},
				wantCount: 2,
				want:      []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
				wantErr:   false,
			},
			{
				name: "3. Get several random fields",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
					"field4": {Value: "value4"},
					"field5": {Value: "value6"},
				},
				key:       "hrandfield_key3",
				options:   HRandFieldOptions{Count: 3},
				wantCount: 3,
				want:      []string{"field1", "field2", "field3", "field4", "field5"},
				wantErr:   false,
			},
			{
				name: "4. Get several random fields with their corresponding values",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
					"field4": {Value: "value4"},
					"field5": {Value: "value5"},
				},
				key:       "hrandfield_key4",
				options:   HRandFieldOptions{WithValues: true, Count: 3},
				wantCount: 6,
				want: []string{
					"field1", "value1", "field2", "123456789", "field3",
					"3.142", "field4", "value4", "field5", "value5",
				},
				wantErr: false,
			},
			{
				name: "5. Get the entire hash",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
					"field4": {Value: "value4"},
					"field5": {Value: "value5"},
				},
				key:       "hrandfield_key5",
				options:   HRandFieldOptions{Count: 5},
				wantCount: 5,
				want:      []string{"field1", "field2", "field3", "field4", "field5"},
				wantErr:   false,
			},
			{
				name: "6. Get the entire hash with values",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
					"field4": {Value: "value4"},
					"field5": {Value: "value5"},
				},
				key:       "hrandfield_key6",
				options:   HRandFieldOptions{WithValues: true, Count: 5},
				wantCount: 10,
				want: []string{
					"field1", "value1", "field2", "123456789", "field3",
					"3.142", "field4", "value4", "field5", "value5",
				},
				wantErr: false,
			},
			{
				name:        "7. Trying to get random field on a non hash map returns error",
				presetValue: "Default value",
				key:         "hrandfield_key7",
				options:     HRandFieldOptions{},
				wantCount:   0,
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HRandField(tt.key, tt.options)
				if (err != nil) != tt.wantErr {
					t.Errorf("HRANDFIELD() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != tt.wantCount {
					t.Errorf("HRANDFIELD() got = %v, want %v", got, tt.want)
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("HRANDFIELD() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_HSET", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name            string
			presetValue     interface{}
			hsetFunc        func(key string, pairs map[string]string) (int, error)
			key             string
			fieldValuePairs map[string]string
			want            int
			wantErr         bool
		}{
			{
				name:            "1. HSETNX set field on non-existent hash map",
				key:             "hset_key1",
				presetValue:     nil,
				hsetFunc:        server.HSetNX,
				fieldValuePairs: map[string]string{"field1": "value1"},
				want:            1,
				wantErr:         false,
			},
			{
				name:            "2. HSETNX set field on existing hash map",
				key:             "hset_key2",
				presetValue:     hash.Hash{"field1": {Value: "value1"}},
				hsetFunc:        server.HSetNX,
				fieldValuePairs: map[string]string{"field2": "value2"},
				want:            1,
				wantErr:         false,
			},
			{
				name:            "3. HSETNX skips operation when setting on existing field",
				key:             "hset_key3",
				presetValue:     hash.Hash{"field1": {Value: "value1"}},
				hsetFunc:        server.HSetNX,
				fieldValuePairs: map[string]string{"field1": "value1"},
				want:            0,
				wantErr:         false,
			},
			{
				name:            "4. Regular HSET command on non-existent hash map",
				key:             "hset_key4",
				presetValue:     nil,
				fieldValuePairs: map[string]string{"field1": "value1", "field2": "value2"},
				hsetFunc:        server.HSet,
				want:            2,
				wantErr:         false,
			},
			{
				name:            "5. Regular HSET update on existing hash map",
				key:             "hset_key5",
				presetValue:     hash.Hash{"field1": {Value: "value1"}, "field2": {Value: "value2"}},
				fieldValuePairs: map[string]string{"field1": "value1-new", "field2": "value2-ne2", "field3": "value3"},
				hsetFunc:        server.HSet,
				want:            3,
				wantErr:         false,
			},
			{
				name:            "6. HSET overwrites when the target key is not a map",
				key:             "hset_key6",
				presetValue:     "Default preset value",
				fieldValuePairs: map[string]string{"field1": "value1"},
				hsetFunc:        server.HSet,
				want:            1,
				wantErr:         false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := tt.hsetFunc(tt.key, tt.fieldValuePairs)
				if (err != nil) != tt.wantErr {
					t.Errorf("HSET() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("HSET() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HSTRLEN", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			fields      []string
			want        []int
			wantErr     bool
		}{
			{
				// Return lengths of field values.
				// If the key does not exist, its length should be 0.
				name: "1. Return lengths of field values",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				key:     "hstrlen_key1",
				fields:  []string{"field1", "field2", "field3", "field4"},
				want:    []int{len("value1"), len("123456789"), len("3.142"), 0},
				wantErr: false,
			},
			{
				name:        "2. Response when trying to get HSTRLEN non-existent key",
				presetValue: hash.Hash{},
				key:         "hstrlen_key2",
				fields:      []string{"field1"},
				want:        []int{0},
				wantErr:     false,
			},
			{
				name:        "3. Command too short",
				key:         "hstrlen_key3",
				presetValue: hash.Hash{},
				fields:      []string{},
				want:        nil,
				wantErr:     true,
			},
			{
				name:        "4. Trying to get lengths on a non hash map returns error",
				key:         "hstrlen_key4",
				presetValue: "Default value",
				fields:      []string{"field1"},
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				t.Log(tt.name)
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HStrLen(tt.key, tt.fields...)
				if (err != nil) != tt.wantErr {
					t.Errorf("HSTRLEN() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("HSTRLEN() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HVALS", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			want        []string
			wantErr     bool
		}{
			{
				name: "1. Return all the values from a hash",
				key:  "hvals_key1",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 123456789},
					"field3": {Value: 3.142},
				},
				want:    []string{"value1", "123456789", "3.142"},
				wantErr: false,
			},
			{
				name:        "2. Empty array response when trying to get HSTRLEN non-existent key",
				key:         "hvals_key2",
				presetValue: nil,
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "3. Trying to get lengths on a non hash map returns error",
				key:         "hvals_key5",
				presetValue: "Default value",
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HVals(tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("HVALS() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("HVALS() got = %v, want %v", got, tt.want)
				}
				for _, g := range got {
					if !slices.Contains(tt.want, g) {
						t.Errorf("HVALS() got = %v, want %v", got, tt.want)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_HGet", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			fields      []string
			want        []string
			wantErr     bool
		}{
			{
				name: "1. Get values from existing hash.",
				key:  "HgetKey1",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:  []string{"field1", "field2", "field3", "field4"},
				want:    []string{"value1", "365", "3.142", ""},
				wantErr: false,
			},
			{
				name:        "2. Return empty slice when attempting to get from non-existed key",
				presetValue: nil,
				key:         "HgetKey2",
				fields:      []string{"field1"},
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "3. Error when trying to get from a value that is not a hash map",
				presetValue: "Default Value",
				key:         "HgetKey3",
				fields:      []string{"field1"},
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HGet(tt.key, tt.fields...)
				if (err != nil) != tt.wantErr {
					t.Errorf("HGet() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("HGet() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HMGet", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			fields      []string
			want        []string
			wantErr     bool
		}{
			{
				name: "1. Get values from existing hash.",
				key:  "HMgetKey1",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:  []string{"field1", "field2", "field3", "field4"},
				want:    []string{"value1", "365", "3.142", ""},
				wantErr: false,
			},
			{
				name:        "2. Return empty slice when attempting to get from non-existed key",
				presetValue: nil,
				key:         "HMgetKey2",
				fields:      []string{"field1"},
				want:        []string{},
				wantErr:     false,
			},
			{
				name:        "3. Error when trying to get from a value that is not a hash map",
				presetValue: "Default Value",
				key:         "HMgetKey3",
				fields:      []string{"field1"},
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HGet(tt.key, tt.fields...)
				if (err != nil) != tt.wantErr {
					t.Errorf("HMGet() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("HMGet() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HExpire", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name         string
			presetValue  interface{}
			key          string
			fields       []string
			expireOption ExpireOptions
			want         []int
			wantErr      bool
		}{
			{
				name: "1. Set Expiration from existing hash.",
				key:  "HExpireKey1",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:  []string{"field1", "field2", "field3"},
				want:    []int{1, 1, 1},
				wantErr: false,
			},
			{
				name:        "2. Return -2 when attempting to get from non-existed key",
				presetValue: nil,
				key:         "HExpireKey2",
				fields:      []string{"field1"},
				want:        []int{-2},
				wantErr:     false,
			},
			{
				name:        "3. Error when trying to get from a value that is not a hash map",
				presetValue: "Default Value",
				key:         "HExpireKey3",
				fields:      []string{"field1"},
				want:        nil,
				wantErr:     true,
			},
			{
				name: "4. Set Expiration with option NX.",
				key:  "HExpireKey4",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:       []string{"field1", "field2", "field3"},
				expireOption: NX,
				want:         []int{1, 1, 1},
				wantErr:      false,
			},
			{
				name: "5. Set Expiration with option XX.",
				key:  "HExpireKey5",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:       []string{"field1", "field2", "field3"},
				expireOption: XX,
				want:         []int{0, 0, 0},
				wantErr:      false,
			},
			{
				name: "6. Set Expiration with option GT.",
				key:  "HExpireKey6",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:       []string{"field1", "field2", "field3"},
				expireOption: GT,
				want:         []int{0, 0, 0},
				wantErr:      false,
			},
			{
				name: "7. Set Expiration with option LT.",
				key:  "HExpireKey7",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:       []string{"field1", "field2", "field3"},
				expireOption: LT,
				want:         []int{1, 1, 1},
				wantErr:      false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HExpire(tt.key, 5, tt.expireOption, tt.fields...)
				if (err != nil) != tt.wantErr {
					t.Errorf("HExpire() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("HExpire() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_HTTL", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			fields      []string
			want        []int
			wantErr     bool
		}{
			{
				name: "1. Get TTL for one field when expireTime is set.",
				key:  "HTTL_Key1",
				presetValue: hash.Hash{
					"field1": {Value: "value1", ExpireAt: server.clock.Now().Add(time.Duration(500) * time.Second)},
				},
				fields:  []string{"field1"},
				want:    []int{500},
				wantErr: false,
			},
			{
				name: "2. Get TTL for multiple fields when expireTime is set.",
				presetValue: hash.Hash{
					"field1": {Value: "value1", ExpireAt: server.clock.Now().Add(time.Duration(500) * time.Second)},
					"field2": {Value: "value2", ExpireAt: server.clock.Now().Add(time.Duration(500) * time.Second)},
					"field3": {Value: "value3", ExpireAt: server.clock.Now().Add(time.Duration(500) * time.Second)},
				},
				key:     "HTTL_Key2",
				fields:  []string{"field1", "field2", "field3"},
				want:    []int{500, 500, 500},
				wantErr: false,
			},
			{
				name: "3. Get TTL for one field when expireTime is not set.",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
				},
				key:     "HTTL_Key3",
				fields:  []string{"field1"},
				want:    []int{-1},
				wantErr: false,
			},
			{
				name: "4. Get TTL for multiple fields when expireTime is not set.",
				key:  "HTTL_Key4",
				presetValue: hash.Hash{
					"field1": {Value: "value1"},
					"field2": {Value: 365},
					"field3": {Value: 3.142},
				},
				fields:  []string{"field1", "field2", "field3"},
				want:    []int{-1, -1, -1},
				wantErr: false,
			},
			{
				name:        "5. Try to get TTL for key that doesn't exist.",
				key:         "HTTL_Key5",
				presetValue: nil,
				fields:      []string{"field1"},
				want:        []int{-2},
				wantErr:     false,
			},
			{
				name:        "6. Try to get TTL for key that isn't a hash.",
				key:         "HTTL_Key6",
				presetValue: "not a hash",
				fields:      []string{"field1", "field2", "field3"},
				want:        nil,
				wantErr:     true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
				}
				got, err := server.HTTL(tt.key, tt.fields...)
				if (err != nil) != tt.wantErr {
					t.Errorf("HExpire() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("HExpire() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("Test_HandleHPEXPIRETIME", func(t *testing.T) {
		t.Parallel()
	
		const fixedTimestamp int64 = 1136189545000
		var noOption ExpireOptions
	
		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			fields      []string
			want        []int64
			wantErr     bool
			setExpiry   bool
		}{
			{
				name: "1. Get expiration time for one field",
				key:  "HPExpireTime_Key1",
				presetValue: hash.Hash{
					"field1": hash.HashValue{
						Value: "value1",
					},
				},
				fields:    []string{"field1"},
				want:      []int64{fixedTimestamp},
				wantErr:   false,
				setExpiry: true,
			},
			{
				name: "2. Get expiration time for multiple fields",
				key:  "HPExpireTime_Key2",
				presetValue: hash.Hash{
					"field1": hash.HashValue{
						Value: "value1",
					},
					"field2": hash.HashValue{
						Value: "value2",
					},
					"field3": hash.HashValue{
						Value: "value3",
					},
				},
				fields:    []string{"field1", "field2", "field3"},
				want:      []int64{fixedTimestamp, fixedTimestamp, fixedTimestamp},
				wantErr:   false,
				setExpiry: true,
			},
			{
				name: "3. Mix of existing and non-existing fields",
				key:  "HPExpireTime_Key3",
				presetValue: hash.Hash{
					"field1": hash.HashValue{
						Value: "value1",
					},
					"field2": hash.HashValue{
						Value: "value2",
					},
				},
				fields:    []string{"field1", "nonexistent", "field2"},
				want:      []int64{fixedTimestamp, -2, fixedTimestamp},
				wantErr:   false,
				setExpiry: true,
			},
			{
				name: "4. Fields with no expiration set",
				key:  "HPExpireTime_Key4",
				presetValue: hash.Hash{
					"field1": hash.HashValue{Value: "value1"},
					"field2": hash.HashValue{Value: "value2"},
				},
				fields:    []string{"field1", "field2"},
				want:      []int64{-1, -1},
				wantErr:   false,
				setExpiry: false,
			},
			{
				name:        "6. Key doesn't exist",
				key:         "HPExpireTime_Key6",
				presetValue: nil,
				fields:      []string{"field1"},
				want:        []int64{},
				wantErr:     false,
				setExpiry:   false,
			},
			{
				name:        "7. Key is not a hash",
				key:         "HPExpireTime_Key7",
				presetValue: "not a hash",
				fields:      []string{"field1"},
				want:        nil,
				wantErr:     true,
				setExpiry:   false,
			},
		}
	
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
	
					if hash, ok := tt.presetValue.(hash.Hash); ok && tt.setExpiry {
						for _, field := range tt.fields {
							if hashValue, exists := hash[field]; exists && hashValue.Value != nil {
								_, err := server.HExpire(tt.key, 500, noOption, field)
								if err != nil {
									t.Error(err)
									return
								}
							}
						}
					}
				}
	
				got, err := server.HPExpireTime(tt.key, tt.fields...)
				if (err != nil) != tt.wantErr {
					t.Errorf("HPExpireTime() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("HPExpireTime() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("Test_HandleHEXPIRETIME", func(t *testing.T) {
		t.Parallel()
	
		const fixedTimestamp int64 = 1136189545
		var noOption ExpireOptions
	
		tests := []struct {
			name        string
			presetValue interface{}
			key         string
			fields      []string
			want        []int64
			wantErr     bool
			setExpiry   bool
		}{
			{
				name: "1. Get expiration time for one field",
				key:  "HExpireTime_Key1",
				presetValue: hash.Hash{
					"field1": hash.HashValue{
						Value: "value1",
					},
				},
				fields:    []string{"field1"},
				want:      []int64{fixedTimestamp},
				wantErr:   false,
				setExpiry: true,
			},
			{
				name: "2. Get expiration time for multiple fields",
				key:  "HExpireTime_Key2",
				presetValue: hash.Hash{
					"field1": hash.HashValue{
						Value: "value1",
					},
					"field2": hash.HashValue{
						Value: "value2",
					},
					"field3": hash.HashValue{
						Value: "value3",
					},
				},
				fields:    []string{"field1", "field2", "field3"},
				want:      []int64{fixedTimestamp, fixedTimestamp, fixedTimestamp},
				wantErr:   false,
				setExpiry: true,
			},
			{
				name: "3. Mix of existing and non-existing fields",
				key:  "HExpireTime_Key3",
				presetValue: hash.Hash{
					"field1": hash.HashValue{
						Value: "value1",
					},
					"field2": hash.HashValue{
						Value: "value2",
					},
				},
				fields:    []string{"field1", "nonexistent", "field2"},
				want:      []int64{fixedTimestamp, -2, fixedTimestamp},
				wantErr:   false,
				setExpiry: true,
			},
			{
				name: "4. Fields with no expiration set",
				key:  "HExpireTime_Key4",
				presetValue: hash.Hash{
					"field1": hash.HashValue{Value: "value1"},
					"field2": hash.HashValue{Value: "value2"},
				},
				fields:    []string{"field1", "field2"},
				want:      []int64{-1, -1},
				wantErr:   false,
				setExpiry: false,
			},
			{
				name:        "6. Key doesn't exist",
				key:         "HExpireTime_Key6",
				presetValue: nil,
				fields:      []string{"field1"},
				want:        []int64{},
				wantErr:     false,
				setExpiry:   false,
			},
			{
				name:        "7. Key is not a hash",
				key:         "HExpireTime_Key7",
				presetValue: "not a hash",
				fields:      []string{"field1"},
				want:        nil,
				wantErr:     true,
				setExpiry:   false,
			},
		}
	
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if tt.presetValue != nil {
					err := presetValue(server, context.Background(), tt.key, tt.presetValue)
					if err != nil {
						t.Error(err)
						return
					}
	
					if hash, ok := tt.presetValue.(hash.Hash); ok && tt.setExpiry {
						for _, field := range tt.fields {
							if hashValue, exists := hash[field]; exists && hashValue.Value != nil {
								_, err := server.HExpire(tt.key, 500, noOption, field)
								if err != nil {
									t.Error(err)
									return
								}
							}
						}
					}
				}
	
				got, err := server.HExpireTime(tt.key, tt.fields...)
				t.Logf("ExpireAt time: %v", got)
				if (err != nil) != tt.wantErr {
					t.Errorf("HExpireTime() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("HExpireTime() got = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
