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
	"context"
	"reflect"
	"slices"
	"testing"
)

func TestEchoVault_HDEL(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		fields      []string
		want        int
		wantErr     bool
	}{
		{
			name:        "Return count of deleted fields in the specified hash",
			key:         "key1",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142, "field7": "value7"},
			fields:      []string{"field1", "field2", "field3", "field4", "field5", "field6"},
			want:        3,
			wantErr:     false,
		},
		{
			name:        "0 response when passing delete fields that are non-existent on valid hash",
			key:         "key2",
			presetValue: map[string]interface{}{"field1": "value1", "field2": "value2", "field3": "value3"},
			fields:      []string{"field4", "field5", "field6"},
			want:        0,
			wantErr:     false,
		},
		{
			name:        "0 response when trying to call HDEL on non-existent key",
			key:         "key3",
			presetValue: nil,
			fields:      []string{"field1"},
			want:        0,
			wantErr:     false,
		},
		{
			name:        "Trying to get lengths on a non hash map returns error",
			presetValue: "Default value",
			key:         "key5",
			fields:      []string{"field1"},
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
}

func TestEchoVault_HEXISTS(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		field       string
		want        bool
		wantErr     bool
	}{
		{
			name:        "Return 1 if the field exists in the hash",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			key:         "key1",
			field:       "field1",
			want:        true,
			wantErr:     false,
		},
		{
			name:        "False response when trying to call HEXISTS on non-existent key",
			presetValue: map[string]interface{}{},
			key:         "key2",
			field:       "field1",
			want:        false,
			wantErr:     false,
		},
		{
			name:        "Trying to get lengths on a non hash map returns error",
			presetValue: "Default value",
			key:         "key5",
			field:       "field1",
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
}

func TestEchoVault_HGETALL(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        []string
		wantErr     bool
	}{
		{
			name:        "Return an array containing all the fields and values of the hash",
			key:         "key1",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			want:        []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
			wantErr:     false,
		},
		{
			name:        "Empty array response when trying to call HGETALL on non-existent key",
			key:         "key2",
			presetValue: map[string]interface{}{},
			want:        []string{},
			wantErr:     false,
		},
		{
			name:        "Trying to get lengths on a non hash map returns error",
			key:         "key5",
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
}

func TestEchoVault_HINCRBY(t *testing.T) {
	server := createEchoVault()

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
			name:          "Increment by integer on non-existent hash should create a new one",
			presetValue:   nil,
			incr_type:     HINCRBY,
			key:           "key1",
			field:         "field1",
			increment_int: 1,
			want:          1,
			wantErr:       false,
		},
		{
			name:            "Increment by float on non-existent hash should create one",
			presetValue:     nil,
			incr_type:       HINCRBYFLOAT,
			key:             "key2",
			field:           "field1",
			increment_float: 3.142,
			want:            3.142,
			wantErr:         false,
		},
		{
			name:          "Increment by integer on existing hash",
			presetValue:   map[string]interface{}{"field1": 1},
			incr_type:     HINCRBY,
			key:           "key3",
			field:         "field1",
			increment_int: 10,
			want:          11,
			wantErr:       false,
		},
		{
			name:            "Increment by float on an existing hash",
			presetValue:     map[string]interface{}{"field1": 3.142},
			incr_type:       HINCRBYFLOAT,
			key:             "key4",
			field:           "field1",
			increment_float: 3.142,
			want:            6.284,
			wantErr:         false,
		},
		{
			name:          "Error when trying to increment on a key that is not a hash",
			presetValue:   "Default value",
			incr_type:     HINCRBY,
			key:           "key9",
			field:         "field1",
			increment_int: 3,
			want:          0,
			wantErr:       true,
		},
		{
			name:          "Error when trying to increment a hash field that is not a number",
			presetValue:   map[string]interface{}{"field1": "value1"},
			incr_type:     HINCRBY,
			key:           "key10",
			field:         "field1",
			increment_int: 1,
			want:          0,
			wantErr:       true,
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
}

func TestEchoVault_HKEYS(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        []string
		wantErr     bool
	}{
		{
			name:        "Return an array containing all the keys of the hash",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			key:         "key1",
			want:        []string{"field1", "field2", "field3"},
			wantErr:     false,
		},
		{
			name:        "Empty array response when trying to call HKEYS on non-existent key",
			presetValue: map[string]interface{}{},
			key:         "key2",
			want:        []string{},
			wantErr:     false,
		},
		{
			name:        "Trying to get lengths on a non hash map returns error",
			presetValue: "Default value",
			key:         "key3",
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
}

func TestEchoVault_HLEN(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        int
		wantErr     bool
	}{
		{
			name:        "Return the correct length of the hash",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			key:         "key1",
			want:        3,
			wantErr:     false,
		},
		{
			name:        "0 Response when trying to call HLEN on non-existent key",
			presetValue: nil,
			key:         "key2",
			want:        0,
			wantErr:     false,
		},
		{
			name:        "Trying to get lengths on a non hash map returns error",
			presetValue: "Default value",
			key:         "key5",
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
}

func TestEchoVault_HRANDFIELD(t *testing.T) {
	server := createEchoVault()

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
			name:        "Get a random field",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			key:         "key1",
			options:     HRandFieldOptions{Count: 1},
			wantCount:   1,
			want:        []string{"field1", "field2", "field3"},
			wantErr:     false,
		},
		{
			name:        "Get a random field with a value",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			key:         "key2",
			options:     HRandFieldOptions{WithValues: true, Count: 1},
			wantCount:   2,
			want:        []string{"field1", "value1", "field2", "123456789", "field3", "3.142"},
			wantErr:     false,
		},
		{
			name: "Get several random fields",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			key:       "key3",
			options:   HRandFieldOptions{Count: 3},
			wantCount: 3,
			want:      []string{"field1", "field2", "field3", "field4", "field5"},
			wantErr:   false,
		},
		{
			name: "Get several random fields with their corresponding values",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			key:       "key4",
			options:   HRandFieldOptions{WithValues: true, Count: 3},
			wantCount: 6,
			want: []string{
				"field1", "value1", "field2", "123456789", "field3",
				"3.142", "field4", "value4", "field5", "value5",
			},
			wantErr: false,
		},
		{
			name: "Get the entire hash",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			key:       "key5",
			options:   HRandFieldOptions{Count: 5},
			wantCount: 5,
			want:      []string{"field1", "field2", "field3", "field4", "field5"},
			wantErr:   false,
		},
		{
			name: "Get the entire hash with values",
			presetValue: map[string]interface{}{
				"field1": "value1",
				"field2": 123456789,
				"field3": 3.142,
				"field4": "value4",
				"field5": "value5",
			},
			key:       "key5",
			options:   HRandFieldOptions{WithValues: true, Count: 5},
			wantCount: 10,
			want: []string{
				"field1", "value1", "field2", "123456789", "field3",
				"3.142", "field4", "value4", "field5", "value5",
			},
			wantErr: false,
		},
		{
			name:        "Trying to get random field on a non hash map returns error",
			presetValue: "Default value",
			key:         "key12",
			options:     HRandFieldOptions{},
			wantCount:   0,
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
}

func TestEchoVault_HSET(t *testing.T) {
	server := createEchoVault()

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
			name:            "HSETNX set field on non-existent hash map",
			key:             "key1",
			presetValue:     nil,
			hsetFunc:        server.HSetNX,
			fieldValuePairs: map[string]string{"field1": "value1"},
			want:            1,
			wantErr:         false,
		},
		{
			name:            "HSETNX set field on existing hash map",
			key:             "key2",
			presetValue:     map[string]interface{}{"field1": "value1"},
			hsetFunc:        server.HSetNX,
			fieldValuePairs: map[string]string{"field2": "value2"},
			want:            1,
			wantErr:         false,
		},
		{
			name:            "HSETNX skips operation when setting on existing field",
			key:             "key3",
			presetValue:     map[string]interface{}{"field1": "value1"},
			hsetFunc:        server.HSetNX,
			fieldValuePairs: map[string]string{"field1": "value1"},
			want:            0,
			wantErr:         false,
		},
		{
			name:            "Regular HSET command on non-existent hash map",
			key:             "key4",
			presetValue:     nil,
			fieldValuePairs: map[string]string{"field1": "value1", "field2": "value2"},
			hsetFunc:        server.HSet,
			want:            2,
			wantErr:         false,
		},
		{
			name:            "Regular HSET update on existing hash map",
			key:             "key5",
			presetValue:     map[string]interface{}{"field1": "value1", "field2": "value2"},
			fieldValuePairs: map[string]string{"field1": "value1-new", "field2": "value2-ne2", "field3": "value3"},
			hsetFunc:        server.HSet,
			want:            3,
			wantErr:         false,
		},
		{
			name:            "HSET overwrites when the target key is not a map",
			key:             "key6",
			presetValue:     "Default preset value",
			fieldValuePairs: map[string]string{"field1": "value1"},
			hsetFunc:        server.HSet,
			want:            1,
			wantErr:         false,
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
}

func TestEchoVault_HSTRLEN(t *testing.T) {
	server := createEchoVault()

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
			name:        "Return lengths of field values",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			key:         "key1",
			fields:      []string{"field1", "field2", "field3", "field4"},
			want:        []int{len("value1"), len("123456789"), len("3.142"), 0},
			wantErr:     false,
		},
		{
			name:        "Response when trying to get HSTRLEN non-existent key",
			presetValue: map[string]interface{}{},
			key:         "key2",
			fields:      []string{"field1"},
			want:        []int{0},
			wantErr:     false,
		},
		{
			name:        "Command too short",
			key:         "key3",
			presetValue: map[string]interface{}{},
			fields:      []string{},
			want:        nil,
			wantErr:     true,
		},
		{
			name:        "Trying to get lengths on a non hash map returns error",
			key:         "key4",
			presetValue: "Default value",
			fields:      []string{"field1"},
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
}

func TestEchoVault_HVALS(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        []string
		wantErr     bool
	}{
		{
			name:        "Return all the values from a hash",
			key:         "key1",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 123456789, "field3": 3.142},
			want:        []string{"value1", "123456789", "3.142"},
			wantErr:     false,
		},
		{
			name:        "Empty array response when trying to get HSTRLEN non-existent key",
			key:         "key2",
			presetValue: nil,
			want:        []string{},
			wantErr:     false,
		},
		{
			name:        "Trying to get lengths on a non hash map returns error",
			key:         "key5",
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
}

func TestEchoVault_HGet(t *testing.T) {
	server := createEchoVault()
	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		fields      []string
		want        []string
		wantErr     bool
	}{
		{
			name:        "1. Get values from existing hash.",
			key:         "HgetKey1",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 365, "field3": 3.142},
			fields:      []string{"field1", "field2", "field3", "field4"},
			want:        []string{"value1", "365", "3.142", ""},
			wantErr:     false,
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
}

func TestEchoVault_HMGet(t *testing.T) {
	server := createEchoVault()
	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		fields      []string
		want        []string
		wantErr     bool
	}{
		{
			name:        "1. Get values from existing hash.",
			key:         "HgetKey1",
			presetValue: map[string]interface{}{"field1": "value1", "field2": 365, "field3": 3.142},
			fields:      []string{"field1", "field2", "field3", "field4"},
			want:        []string{"value1", "365", "3.142", ""},
			wantErr:     false,
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
}
