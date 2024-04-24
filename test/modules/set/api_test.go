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
	"context"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/modules/set"
	"github.com/echovault/echovault/pkg/echovault"
	"reflect"
	"slices"
	"testing"
)

func createEchoVault() *echovault.EchoVault {
	ev, _ := echovault.NewEchoVault(
		echovault.WithConfig(config.Config{
			DataDir: "",
		}),
	)
	return ev
}

func presetValue(server *echovault.EchoVault, ctx context.Context, key string, value interface{}) error {
	if _, err := server.CreateKeyAndLock(ctx, key); err != nil {
		return err
	}
	if err := server.SetValue(ctx, key, value); err != nil {
		return err
	}
	server.KeyUnlock(ctx, key)
	return nil
}

func TestEchoVault_SADD(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		members     []string
		want        int
		wantErr     bool
	}{
		{
			name:        "Create new set on a non-existent key, return count of added elements",
			presetValue: nil,
			key:         "key1",
			members:     []string{"one", "two", "three", "four"},
			want:        4,
			wantErr:     false,
		},
		{
			name:        "Add members to an exiting set, skip members that already exist in the set, return added count",
			presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
			key:         "key2",
			members:     []string{"three", "four", "five", "six", "seven"},
			want:        3,
			wantErr:     false,
		},
		{
			name:        "Throw error when trying to add to a key that does not hold a set",
			presetValue: "Default value",
			key:         "key3",
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
			got, err := server.SADD(tt.key, tt.members...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SADD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SADD() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SCARD(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        int
		wantErr     bool
	}{
		{
			name:        "Get cardinality of valid set",
			presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
			key:         "key1",
			want:        4,
			wantErr:     false,
		},
		{
			name:        "Return 0 when trying to get cardinality on non-existent key",
			presetValue: nil,
			key:         "key2",
			want:        0,
			wantErr:     false,
		},
		{
			name:        "Throw error when trying to get cardinality of a value that is not a set",
			presetValue: "Default value",
			key:         "key3",
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
			got, err := server.SCARD(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("SCARD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SCARD() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SDIFF(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		keys         []string
		want         []string
		wantErr      bool
	}{
		{
			name: "Get the difference between 2 sets",
			presetValues: map[string]interface{}{
				"key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			keys:    []string{"key1", "key2"},
			want:    []string{"one", "two"},
			wantErr: false,
		},
		{
			name: "Get the difference between 3 sets",
			presetValues: map[string]interface{}{
				"key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key5": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key3", "key4", "key5"},
			want:    []string{"three", "four", "five", "six"},
			wantErr: false,
		},
		{
			name: "Return base set element if base set is the only valid set",
			presetValues: map[string]interface{}{
				"key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": 123456789,
			},
			keys:    []string{"key6", "key7", "key8"},
			want:    []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			wantErr: false,
		},
		{
			name: "Throw error when base set is not a set",
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key9", "key10", "key11"},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Throw error when base set is non-existent",
			presetValues: map[string]interface{}{
				"key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"non-existent", "key7", "key8"},
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
			got, err := server.SDIFF(tt.keys...)
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
}

func TestEchoVault_SDIFFSTORE(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		destination  string
		keys         []string
		want         int
		wantErr      bool
	}{
		{
			name: "Get the difference between 2 sets",
			presetValues: map[string]interface{}{
				"key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination: "destination1",
			keys:        []string{"key1", "key2"},
			want:        2,
			wantErr:     false,
		},
		{
			name: "Get the difference between 3 sets",
			presetValues: map[string]interface{}{
				"key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key5": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination: "destination2",
			keys:        []string{"key3", "key4", "key5"},
			want:        4,
			wantErr:     false,
		},
		{
			name: "Return base set element if base set is the only valid set",
			presetValues: map[string]interface{}{
				"key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": 123456789,
			},
			destination: "destination3",
			keys:        []string{"key6", "key7", "key8"},
			want:        8,
			wantErr:     false,
		},
		{
			name: "Throw error when base set is not a set",
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination: "destination4",
			keys:        []string{"key9", "key10", "key11"},
			want:        0,
			wantErr:     true,
		},
		{
			name:        " Throw error when base set is non-existent",
			destination: "destination5",
			presetValues: map[string]interface{}{
				"key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"non-existent", "key7", "key8"},
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
			got, err := server.SDIFFSTORE(tt.destination, tt.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SDIFFSTORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SDIFFSTORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SINTER(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		keys         []string
		want         []string
		wantErr      bool
	}{
		{
			name: "Get the intersection between 2 sets",
			presetValues: map[string]interface{}{
				"key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			keys:    []string{"key1", "key2"},
			want:    []string{"three", "four", "five"},
			wantErr: false,
		},
		{
			name: "Get the intersection between 3 sets",
			presetValues: map[string]interface{}{
				"key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key5": set.NewSet([]string{"one", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key3", "key4", "key5"},
			want:    []string{"one", "eight"},
			wantErr: false,
		},
		{
			name: "Throw an error if any of the provided keys are not sets",
			presetValues: map[string]interface{}{
				"key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": set.NewSet([]string{"one"}),
			},
			keys:    []string{"key6", "key7", "key8"},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Throw error when base set is not a set",
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key9", "key10", "key11"},
			want:    nil,
			wantErr: true,
		},
		{
			name: "If any of the keys does not exist, return an empty array",
			presetValues: map[string]interface{}{
				"key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"non-existent", "key7", "key8"},
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
			got, err := server.SINTER(tt.keys...)
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
}

func TestEchoVault_SINTERCARD(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		keys         []string
		limit        uint
		want         int
		wantErr      bool
	}{
		{
			name: "Get the full intersect cardinality between 2 sets",
			presetValues: map[string]interface{}{
				"key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			keys:    []string{"key1", "key2"},
			limit:   0,
			want:    3,
			wantErr: false,
		},
		{
			name: "Get an intersect cardinality between 2 sets with a limit",
			presetValues: map[string]interface{}{
				"key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"}),
				"key4": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"}),
			},
			keys:    []string{"key3", "key4"},
			limit:   3,
			want:    3,
			wantErr: false,
		},
		{
			name: "Get the full intersect cardinality between 3 sets",
			presetValues: map[string]interface{}{
				"key5": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key6": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key7": set.NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key5", "key6", "key7"},
			limit:   0,
			want:    2,
			wantErr: false,
		},
		{
			name: "Get the intersection of 3 sets with a limit",
			presetValues: map[string]interface{}{
				"key8":  set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key9":  set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key10": set.NewSet([]string{"one", "two", "seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key8", "key9", "key10"},
			limit:   2,
			want:    2,
			wantErr: false,
		},
		{
			name: "Return 0 if any of the keys does not exist",
			presetValues: map[string]interface{}{
				"key11": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key12": "Default value",
				"key13": set.NewSet([]string{"one"}),
			},
			keys:    []string{"key11", "key12", "key13", "non-existent"},
			limit:   0,
			want:    0,
			wantErr: false,
		},
		{
			name: "Throw error when one of the keys is not a valid set",
			presetValues: map[string]interface{}{
				"key14": "Default value",
				"key15": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key16": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key14", "key15", "key16"},
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
			got, err := server.SINTERCARD(tt.keys, tt.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("SINTERCARD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SINTERCARD() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SINTERSTORE(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		destination  string
		keys         []string
		want         int
		wantErr      bool
	}{
		{
			name: "Get the intersection between 2 sets and store it at the destination",
			presetValues: map[string]interface{}{
				"key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination: "destination1",
			keys:        []string{"key1", "key2"},
			want:        3,
			wantErr:     false,
		},
		{
			name: "Get the intersection between 3 sets and store it at the destination key",
			presetValues: map[string]interface{}{
				"key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key5": set.NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			destination: "destination2",
			keys:        []string{"key3", "key4", "key5"},
			want:        2,
			wantErr:     false,
		},
		{
			name: "Throw error when any of the keys is not a set",
			presetValues: map[string]interface{}{
				"key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": set.NewSet([]string{"one"}),
			},
			destination: "destination3",
			keys:        []string{"key6", "key7", "key8"},
			want:        0,
			wantErr:     true,
		},
		{
			name: "Throw error when base set is not a set",
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			destination: "destination4",
			keys:        []string{"key9", "key10", "key11"},
			want:        0,
			wantErr:     true,
		},
		{
			name:        "Return an empty intersection if one of the keys does not exist",
			destination: "destination5",
			presetValues: map[string]interface{}{
				"key12": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key13": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"non-existent", "key7", "key8"},
			want:    0,
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
			got, err := server.SINTERSTORE(tt.destination, tt.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SINTERSTORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SINTERSTORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SISMEMBER(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		member      string
		want        bool
		wantErr     bool
	}{
		{
			name:        "Return true when element is a member of the set",
			presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
			key:         "key1",
			member:      "three",
			want:        true,
			wantErr:     false,
		},
		{
			name:        "Return false when element is not a member of the set",
			presetValue: set.NewSet([]string{"one", "two", "three", "four"}),
			key:         "key2",
			member:      "five",
			want:        false,
			wantErr:     false,
		},
		{
			name:        "Throw error when trying to assert membership when the key does not hold a valid set",
			presetValue: "Default value",
			key:         "key3",
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
			got, err := server.SISMEMBER(tt.key, tt.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("SISMEMBER() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SISMEMBER() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SMEMBERS(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        []string
		wantErr     bool
	}{
		{
			name:        "Return all the members of the set",
			key:         "key1",
			presetValue: set.NewSet([]string{"one", "two", "three", "four", "five"}),
			want:        []string{"one", "two", "three", "four", "five"},
			wantErr:     false,
		},
		{
			name:        "If the key does not exist, return an empty array",
			key:         "key2",
			presetValue: nil,
			want:        []string{},
			wantErr:     false,
		},
		{
			name:        "Throw error when the provided key is not a set",
			key:         "key3",
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
			got, err := server.SMEMBERS(tt.key)
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
}

func TestEchoVault_SMISMEMBER(t *testing.T) {
	server := createEchoVault()

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
			name:        "Return set membership status for multiple elements",
			presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven"}),
			key:         "key1",
			members:     []string{"three", "four", "five", "six", "eight", "nine", "seven"},
			want:        []bool{true, true, true, true, false, false, true},
			wantErr:     false,
		},
		{
			name:        "If the set key does not exist, return an array of zeroes as long as the list of members",
			presetValue: nil,
			key:         "key2",
			members:     []string{"one", "two", "three", "four"},
			want:        []bool{false, false, false, false},
			wantErr:     false,
		},
		{
			name:        "Throw error when trying to assert membership when the key does not hold a valid set",
			presetValue: "Default value",
			key:         "key3",
			members:     []string{"one"},
			want:        nil,
			wantErr:     true,
		},
		{
			name:        "Throw error for empty member slice",
			presetValue: nil,
			key:         "key4",
			members:     []string{},
			want:        nil,
			wantErr:     true,
		},
		{
			name:        "Throw error for nil member slice",
			presetValue: nil,
			key:         "key4",
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
			got, err := server.SMISMEMBER(tt.key, tt.members...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SMISMEMBER() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SMISMEMBER() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SMOVE(t *testing.T) {
	server := createEchoVault()

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
			name: "Return true after a successful move of a member from source set to destination set",
			presetValues: map[string]interface{}{
				"source1":      set.NewSet([]string{"one", "two", "three", "four"}),
				"destination1": set.NewSet([]string{"five", "six", "seven", "eight"}),
			},
			source:      "source1",
			destination: "destination1",
			member:      "four",
			want:        true,
			wantErr:     false,
		},
		{
			name: "Return false when trying to move a member from source set to destination set when it doesn't exist in source",
			presetValues: map[string]interface{}{
				"source2":      set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"destination2": set.NewSet([]string{"five", "six", "seven", "eight"}),
			},
			source:      "source2",
			destination: "destination2",
			member:      "six",
			want:        false,
			wantErr:     false,
		},
		{
			name: "Return error when the source key is not a set",
			presetValues: map[string]interface{}{
				"source3":      "Default value",
				"destination3": set.NewSet([]string{"five", "six", "seven", "eight"}),
			},
			source:      "source3",
			destination: "destination3",
			member:      "five",
			want:        false,
			wantErr:     true,
		},
		{
			name: "Return error when the destination key is not a set",
			presetValues: map[string]interface{}{
				"source4":      set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"destination4": "Default value",
			},
			source:      "source4",
			destination: "destination4",
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
			got, err := server.SMOVE(tt.source, tt.destination, tt.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("SMOVE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SMOVE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SPOP(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		count       uint
		want        []string
		wantErr     bool
	}{
		{
			name:        "Return multiple popped elements and modify the set",
			key:         "key1",
			presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			count:       3,
			want:        []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			wantErr:     false,
		},
		{
			name:        "Return error when the source key is not a set",
			key:         "key2",
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
			got, err := server.SPOP(tt.key, tt.count)
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
}

func TestEchoVault_SRANDMEMBER(t *testing.T) {
	server := createEchoVault()

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
			name:        "Return multiple random elements without removing them",
			key:         "key1",
			presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			count:       3,
			wantCount:   3,
			wantErr:     false,
		},
		{
			// Return multiple random elements without removing them
			// Count is negative, so allow repeated numbers
			name:        "Return multiple random elements without removing them",
			key:         "key2",
			presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			count:       -5,
			wantCount:   5,
			wantErr:     false,
		},
		{
			name:        "Return error when the source key is not a set",
			key:         "key3",
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
			got, err := server.SRANDMEMBER(tt.key, tt.count)
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
}

func TestEchoVault_SREM(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		members     []string
		want        int
		wantErr     bool
	}{
		{
			name:        "Remove multiple elements and return the number of elements removed",
			key:         "key1",
			presetValue: set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
			members:     []string{"one", "two", "three", "nine"},
			want:        3,
			wantErr:     false,
		},
		{
			name:        "If key does not exist, return 0",
			key:         "key2",
			presetValue: nil,
			members:     []string{"one", "two", "three", "nine"},
			want:        0,
			wantErr:     false,
		},
		{
			name:        "Return error when the source key is not a set",
			key:         "key3",
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
			got, err := server.SREM(tt.key, tt.members...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SREM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SREM() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SUNION(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		keys         []string
		want         []string
		wantErr      bool
	}{
		{
			name: "Get the union between 2 sets",
			presetValues: map[string]interface{}{
				"key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			keys:    []string{"key1", "key2"},
			want:    []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
			wantErr: false,
		},
		{
			name: "Get the union between 3 sets",
			presetValues: map[string]interface{}{
				"key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key5": set.NewSet([]string{"one", "eight", "nine", "ten", "twelve"}),
			},
			keys: []string{"key3", "key4", "key5"},
			want: []string{
				"one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
				"ten", "eleven", "twelve", "thirty-six",
			},
			wantErr: false,
		},
		{
			name: "Throw an error if any of the provided keys are not sets",
			presetValues: map[string]interface{}{
				"key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": set.NewSet([]string{"one"}),
			},
			keys:    []string{"key6", "key7", "key8"},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Throw error any of the keys does not hold a set",
			presetValues: map[string]interface{}{
				"key9":  "Default value",
				"key10": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven"}),
				"key11": set.NewSet([]string{"seven", "eight", "nine", "ten", "twelve"}),
			},
			keys:    []string{"key9", "key10", "key11"},
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
			got, err := server.SUNION(tt.keys...)
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
}

func TestEchoVault_SUNIONSTORE(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		destination  string
		keys         []string
		want         int
		wantErr      bool
	}{
		{
			name: "Get the intersection between 2 sets and store it at the destination",
			presetValues: map[string]interface{}{
				"key1": set.NewSet([]string{"one", "two", "three", "four", "five"}),
				"key2": set.NewSet([]string{"three", "four", "five", "six", "seven", "eight"}),
			},
			destination: "destination1",
			keys:        []string{"key1", "key2"},
			want:        8,
			wantErr:     false,
		},
		{
			name: "Get the intersection between 3 sets and store it at the destination key",
			presetValues: map[string]interface{}{
				"key3": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key4": set.NewSet([]string{"one", "two", "thirty-six", "twelve", "eleven", "eight"}),
				"key5": set.NewSet([]string{"one", "seven", "eight", "nine", "ten", "twelve"}),
			},
			destination: "destination2",
			keys:        []string{"key3", "key4", "key5"},
			want:        13,
			wantErr:     false,
		},
		{
			name: "Throw error when any of the keys is not a set",
			presetValues: map[string]interface{}{
				"key6": set.NewSet([]string{"one", "two", "three", "four", "five", "six", "seven", "eight"}),
				"key7": "Default value",
				"key8": set.NewSet([]string{"one"}),
			},
			destination: "destination3",
			keys:        []string{"key6", "key7", "key8"},
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
			got, err := server.SUNIONSTORE(tt.destination, tt.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SUNIONSTORE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SUNIONSTORE() got = %v, want %v", got, tt.want)
			}
		})
	}
}
