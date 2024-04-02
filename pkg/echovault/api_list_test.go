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
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/commands"
	"github.com/echovault/echovault/pkg/constants"
	"reflect"
	"testing"
)

func TestEchoVault_LLEN(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

	tests := []struct {
		preset      bool
		presetValue interface{}
		name        string
		key         string
		want        int
		wantErr     bool
	}{
		{
			preset:      true,
			key:         "key1",
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			name:        "If key exists and is a list, return the lists length",
			want:        4,
			wantErr:     false,
		},
		{
			preset:      false,
			key:         "key2",
			name:        "If key does not exist, return 0",
			presetValue: nil,
			want:        0,
			wantErr:     false,
		},
		{
			preset:      true,
			key:         "key5",
			name:        "Trying to get lengths on a non-list returns error",
			presetValue: "Default value",
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.LLEN(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("LLEN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LLEN() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_LINDEX(t *testing.T) {
	server, _ := NewEchoVault(
		WithCommands(commands.All()),
		WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)

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
			name:        "Return last element within range",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key1",
			index:       3,
			want:        "value4",
			wantErr:     false,
		},
		{
			name:        "Return first element within range",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key2",
			index:       0,
			want:        "value1",
			wantErr:     false,
		},
		{
			name:        "Return middle element within range",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key3",
			index:       1,
			want:        "value2",
			wantErr:     false,
		},
		{
			name:        "If key does not exist, return error",
			preset:      false,
			presetValue: nil,
			key:         "key4",
			index:       0,
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Trying to get element by index on a non-list returns error",
			preset:      true,
			presetValue: "Default value",
			key:         "key5",
			index:       0,
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Trying to get index out of range index beyond last index",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3"},
			key:         "key6",
			index:       3,
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		if tt.preset {
			presetValue(server, tt.key, tt.presetValue)
		}
		t.Run(tt.name, func(t *testing.T) {
			got, err := server.LINDEX(tt.key, tt.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("LINDEX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LINDEX() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_LMOVE(t *testing.T) {
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
		presetValue map[string]interface{}
		source      string
		destination string
		whereFrom   string
		whereTo     string
		want        string
		wantErr     bool
	}{
		{
			name:   "Move element from LEFT of left list to LEFT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source1":      []interface{}{"one", "two", "three"},
				"destination1": []interface{}{"one", "two", "three"},
			},
			source:      "source1",
			destination: "destination1",
			whereFrom:   "LEFT",
			whereTo:     "LEFT",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:   "Move element from LEFT of left list to RIGHT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source2":      []interface{}{"one", "two", "three"},
				"destination2": []interface{}{"one", "two", "three"},
			},
			source:      "source2",
			destination: "destination2",
			whereFrom:   "LEFT",
			whereTo:     "RIGHT",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:   "Move element from RIGHT of left list to LEFT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source3":      []interface{}{"one", "two", "three"},
				"destination3": []interface{}{"one", "two", "three"},
			},
			source:      "source3",
			destination: "destination3",
			whereFrom:   "RIGHT",
			whereTo:     "LEFT",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:   "Move element from RIGHT of left list to RIGHT of right list",
			preset: true,
			presetValue: map[string]interface{}{
				"source4":      []interface{}{"one", "two", "three"},
				"destination4": []interface{}{"one", "two", "three"},
			},
			source:      "source4",
			destination: "destination4",
			whereFrom:   "RIGHT",
			whereTo:     "RIGHT",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:   "Throw error when the right list is non-existent",
			preset: true,
			presetValue: map[string]interface{}{
				"source5": []interface{}{"one", "two", "three"},
			},
			source:      "source5",
			destination: "destination5",
			whereFrom:   "LEFT",
			whereTo:     "LEFT",
			want:        "",
			wantErr:     true,
		},
		{
			name:   "Throw error when right list in not a list",
			preset: true,
			presetValue: map[string]interface{}{
				"source6":      []interface{}{"one", "two", "tree"},
				"destination6": "Default value",
			},
			source:      "source6",
			destination: "destination6",
			whereFrom:   "LEFT",
			whereTo:     "LEFT",
			want:        "",
			wantErr:     true,
		},
		{
			name:   "Throw error when left list is non-existent",
			preset: true,
			presetValue: map[string]interface{}{
				"destination7": []interface{}{"one", "two", "three"},
			},
			source:      "source7",
			destination: "destination7",
			whereFrom:   "LEFT",
			whereTo:     "LEFT",
			want:        "",
			wantErr:     true,
		},
		{
			name:   "Throw error when left list is not a list",
			preset: true,
			presetValue: map[string]interface{}{
				"source8":      "Default value",
				"destination8": []interface{}{"one", "two", "three"},
			},
			source:      "source8",
			destination: "destination8",
			whereFrom:   "LEFT",
			whereTo:     "LEFT",
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Throw error when WHEREFROM argument is not LEFT/RIGHT",
			preset:      false,
			presetValue: map[string]interface{}{},
			source:      "source9",
			destination: "destination9",
			whereFrom:   "LEFT",
			whereTo:     "LEFT",
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Throw error when WHERETO argument is not LEFT/RIGHT",
			preset:      false,
			presetValue: map[string]interface{}{},
			source:      "source10",
			destination: "destination10",
			whereFrom:   "LEFT",
			whereTo:     "LEFT",
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				for k, v := range tt.presetValue {
					presetValue(server, k, v)
				}
			}
			got, err := server.LMOVE(tt.source, tt.destination, tt.whereFrom, tt.whereTo)
			if (err != nil) != tt.wantErr {
				t.Errorf("LMOVE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LMOVE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_POP(t *testing.T) {
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
		popFunc     func(key string) (string, error)
		want        string
		wantErr     bool
	}{
		{
			name:        "LPOP returns last element and removed first element from the list",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key1",
			popFunc:     server.LPOP,
			want:        "value1",
			wantErr:     false,
		},
		{
			name:        "RPOP returns last element and removed last element from the list",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key2",
			popFunc:     server.RPOP,
			want:        "value4",
			wantErr:     false,
		},
		{
			name:        "Trying to execute LPOP from a non-list item return an error",
			preset:      true,
			key:         "key3",
			presetValue: "Default value",
			popFunc:     server.LPOP,
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Trying to execute RPOP from a non-list item return an error",
			preset:      true,
			presetValue: "Default value",
			key:         "key6",
			popFunc:     server.RPOP,
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := tt.popFunc(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("POP() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("POP() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_LPUSH(t *testing.T) {
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
		key         string
		values      []string
		presetValue interface{}
		lpushFunc   func(key string, values ...string) (string, error)
		want        string
		wantErr     bool
	}{
		{
			name:        "LPUSHX to existing list prepends the element to the list",
			preset:      true,
			presetValue: []interface{}{"1", "2", "4", "5"},
			key:         "key1",
			values:      []string{"value1", "value2"},
			lpushFunc:   server.LPUSHX,
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "LPUSH on existing list prepends the elements to the list",
			preset:      true,
			presetValue: []interface{}{"1", "2", "4", "5"},
			key:         "key2",
			values:      []string{"value1", "value2"},
			lpushFunc:   server.LPUSH,
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "LPUSH on non-existent list creates the list",
			preset:      false,
			presetValue: nil,
			key:         "key3",
			values:      []string{"value1", "value2"},
			lpushFunc:   server.LPUSH,
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "LPUSHX command returns error on non-existent list",
			preset:      false,
			presetValue: nil,
			key:         "key4",
			values:      []string{"value1", "value2"},
			lpushFunc:   server.LPUSHX,
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
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
}

func TestEchoVault_RPUSH(t *testing.T) {
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
		key         string
		values      []string
		presetValue interface{}
		rpushFunc   func(key string, values ...string) (string, error)
		want        string
		wantErr     bool
	}{
		{
			name:        "RPUSH on non-existent list creates the list",
			preset:      false,
			presetValue: nil,
			key:         "key1",
			values:      []string{"value1", "value2"},
			rpushFunc:   server.RPUSH,
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "RPUSHX command returns error on non-existent list",
			preset:      false,
			presetValue: nil,
			key:         "key2",
			values:      []string{"value1", "value2"},
			rpushFunc:   server.RPUSHX,
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
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
}

func TestEchoVault_LRANGE(t *testing.T) {
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
		start       int
		end         int
		want        []string
		wantErr     bool
	}{
		{
			// Return sub-list within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			name:        "Return sub-list within range.",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			key:         "key1",
			start:       3,
			end:         6,
			want:        []string{"value4", "value5", "value6", "value7"},
			wantErr:     false,
		},
		{
			name:        "Return sub-list from start index to the end of the list when end index is -1",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			key:         "key2",
			start:       3,
			end:         -1,
			want:        []string{"value4", "value5", "value6", "value7", "value8"},
			wantErr:     false,
		},
		{
			name:        "Return the reversed sub-list when the end index is greater than -1 but less than start index",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			key:         "key3",
			start:       3,
			end:         0,
			want:        []string{"value4", "value3", "value2", "value1"},
			wantErr:     false,
		},
		{
			name:        "If key does not exist, return error",
			preset:      false,
			presetValue: nil,
			key:         "key4",
			start:       0,
			end:         2,
			want:        nil,
			wantErr:     true,
		},

		{
			name:        "Error when executing command on non-list command",
			preset:      true,
			presetValue: "Default value",
			key:         "key5",
			start:       0,
			end:         3,
			want:        nil,
			wantErr:     true,
		},
		{
			name:        "Error when start index is less than 0",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key6",
			start:       -1,
			end:         3,
			want:        nil,
			wantErr:     true,
		},
		{
			name:        "Error when start index is higher than the length of the list",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3"},
			key:         "key7",
			start:       10,
			end:         11,
			want:        nil,
			wantErr:     true,
		},
		{
			name:        "Error when start and end indices are equal",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3"},
			key:         "key8",
			start:       1,
			end:         1,
			want:        nil,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.LRANGE(tt.key, tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("LRANGE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LRANGE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_LREM(t *testing.T) {
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
		value       string
		want        string
		wantErr     bool
	}{
		{
			name:        "Remove the first 3 elements that appear in the list",
			preset:      true,
			presetValue: []interface{}{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			key:         "key1",
			count:       3,
			value:       "4",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "Remove the last 3 elements that appear in the list",
			preset:      true,
			presetValue: []interface{}{"1", "2", "4", "4", "5", "6", "7", "4", "8", "4", "9", "10", "5", "4"},
			key:         "key2",
			count:       -3,
			value:       "4",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "Throw error on non-list item",
			preset:      true,
			presetValue: "Default value",
			key:         "LremKey8",
			count:       0,
			value:       "value1",
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		if tt.preset {
			presetValue(server, tt.key, tt.presetValue)
		}
		t.Run(tt.name, func(t *testing.T) {
			got, err := server.LREM(tt.key, tt.count, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("LREM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LREM() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_LSET(t *testing.T) {
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
		index       int
		value       string
		want        string
		wantErr     bool
	}{
		{
			name:        "Return last element within range",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key1",
			index:       3,
			value:       "new-value",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "Return first element within range",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key2",
			index:       0,
			value:       "new-value",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "Return middle element within range",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key3",
			index:       1,
			value:       "new-value",
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "If key does not exist, return error",
			preset:      false,
			presetValue: nil,
			key:         "key4",
			index:       0,
			value:       "element",
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Trying to get element by index on a non-list returns error",
			preset:      true,
			presetValue: "Default value",
			key:         "key5",
			index:       0,
			value:       "element",
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Trying to get index out of range index beyond last index",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3"},
			key:         "key6",
			index:       3,
			value:       "element",
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Trying to get index out of range with negative index",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3"},
			key:         "key7",
			index:       -1,
			value:       "element",
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.LSET(tt.key, tt.index, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("LSET() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LSET() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_LTRIM(t *testing.T) {
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
		start       int
		end         int
		want        string
		wantErr     bool
	}{
		{
			// Return trim within range.
			// Both start and end indices are positive.
			// End index is greater than start index.
			name:        "Return trim within range",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			key:         "key1",
			start:       3,
			end:         6,
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "Return element from start index to end index when end index is greater than length of the list",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"},
			key:         "key2",
			start:       5,
			end:         -1,
			want:        "OK",
			wantErr:     false,
		},
		{
			name:        "Return error when end index is smaller than start index but greater than -1",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key3",
			start:       3,
			end:         1,
			want:        "",
			wantErr:     true,
		},
		{
			name:        "If key does not exist, return error",
			preset:      false,
			presetValue: nil,
			key:         "key4",
			start:       0,
			end:         2,
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Trying to get element by index on a non-list returns error",
			preset:      true,
			presetValue: "Default value",
			key:         "key5",
			start:       0,
			end:         3,
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Error when start index is less than 0",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			key:         "key6",
			start:       -1,
			end:         3,
			want:        "",
			wantErr:     true,
		},
		{
			name:        "Error when start index is higher than the length of the list",
			preset:      true,
			presetValue: []interface{}{"value1", "value2", "value3"},
			key:         "key7",
			start:       10,
			end:         11,
			want:        "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.LTRIM(tt.key, tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("LTRIM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LTRIM() got = %v, want %v", got, tt.want)
			}
		})
	}
}
