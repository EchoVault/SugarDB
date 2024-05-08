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
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/clock"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestEchoVault_DEL(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]internal.KeyData
		keys         []string
		want         int
		wantErr      bool
	}{
		{
			name: "Delete several keys and return deleted count",
			keys: []string{"key1", "key2", "key3", "key4", "key5"},
			presetValues: map[string]internal.KeyData{
				"key1": {Value: "value1", ExpireAt: time.Time{}},
				"key2": {Value: "value2", ExpireAt: time.Time{}},
				"key3": {Value: "value3", ExpireAt: time.Time{}},
				"key4": {Value: "value4", ExpireAt: time.Time{}},
			},
			want:    4,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValues != nil {
				for k, d := range tt.presetValues {
					presetKeyData(server, context.Background(), k, d)
				}
			}
			got, err := server.Del(tt.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("DEL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DEL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_EXPIRE(t *testing.T) {
	mockClock := clock.NewClock()

	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]internal.KeyData
		cmd          string
		key          string
		time         int
		expireOpts   ExpireOptions
		pexpireOpts  PExpireOptions
		want         int
		wantErr      bool
	}{
		{
			name:       "Set new expire by seconds",
			cmd:        "EXPIRE",
			key:        "key1",
			time:       100,
			expireOpts: ExpireOptions{},
			presetValues: map[string]internal.KeyData{
				"key1": {Value: "value1", ExpireAt: time.Time{}},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:        "Set new expire by milliseconds",
			cmd:         "PEXPIRE",
			key:         "key2",
			time:        1000,
			pexpireOpts: PExpireOptions{},
			presetValues: map[string]internal.KeyData{
				"key2": {Value: "value2", ExpireAt: time.Time{}},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:       "Set new expire only when key does not have an expiry time with NX flag",
			cmd:        "EXPIRE",
			key:        "key3",
			time:       1000,
			expireOpts: ExpireOptions{NX: true},
			presetValues: map[string]internal.KeyData{
				"key3": {Value: "value3", ExpireAt: time.Time{}},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:       "Return 0 when NX flag is provided and key already has an expiry time",
			cmd:        "EXPIRE",
			key:        "key4",
			time:       1000,
			expireOpts: ExpireOptions{NX: true},
			presetValues: map[string]internal.KeyData{
				"key4": {Value: "value4", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:       "Set new expire time from now key only when the key already has an expiry time with XX flag",
			cmd:        "EXPIRE",
			key:        "key5",
			time:       1000,
			expireOpts: ExpireOptions{XX: true},
			presetValues: map[string]internal.KeyData{
				"key5": {Value: "value5", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:       "Return 0 when key does not have an expiry and the XX flag is provided",
			cmd:        "EXPIRE",
			time:       1000,
			expireOpts: ExpireOptions{XX: true},
			key:        "key6",
			presetValues: map[string]internal.KeyData{
				"key6": {Value: "value6", ExpireAt: time.Time{}},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:       "Set expiry time when the provided time is after the current expiry time when GT flag is provided",
			cmd:        "EXPIRE",
			key:        "key7",
			time:       100000,
			expireOpts: ExpireOptions{GT: true},
			presetValues: map[string]internal.KeyData{
				"key7": {Value: "value7", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:       "Return 0 when GT flag is passed and current expiry time is greater than provided time",
			cmd:        "EXPIRE",
			key:        "key8",
			time:       1000,
			expireOpts: ExpireOptions{GT: true},
			presetValues: map[string]internal.KeyData{
				"key8": {Value: "value8", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:       "Return 0 when GT flag is passed and key does not have an expiry time",
			cmd:        "EXPIRE",
			key:        "key9",
			time:       1000,
			expireOpts: ExpireOptions{GT: true},
			presetValues: map[string]internal.KeyData{
				"key9": {Value: "value9", ExpireAt: time.Time{}},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:       "Set expiry time when the provided time is before the current expiry time when LT flag is provided",
			cmd:        "EXPIRE",
			key:        "key10",
			time:       1000,
			expireOpts: ExpireOptions{LT: true},
			presetValues: map[string]internal.KeyData{
				"key10": {Value: "value10", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:       "Return 0 when LT flag is passed and current expiry time is less than provided time",
			cmd:        "EXPIRE",
			key:        "key11",
			time:       50000,
			expireOpts: ExpireOptions{LT: true},
			presetValues: map[string]internal.KeyData{
				"key11": {Value: "value11", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValues != nil {
				for k, d := range tt.presetValues {
					presetKeyData(server, context.Background(), k, d)
				}
			}
			var got int
			var err error
			if strings.EqualFold(tt.cmd, "PEXPIRE") {
				got, err = server.PExpire(tt.key, tt.time, tt.pexpireOpts)
			} else {
				got, err = server.Expire(tt.key, tt.time, tt.expireOpts)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("(P)EXPIRE() error = %v, wantErr %v, key %s", err, tt.wantErr, tt.key)
				return
			}
			if got != tt.want {
				t.Errorf("(P)EXPIRE() got = %v, want %v, key %s", got, tt.want, tt.key)
			}
		})
	}
}

func TestEchoVault_EXPIREAT(t *testing.T) {
	mockClock := clock.NewClock()

	server := createEchoVault()

	tests := []struct {
		name          string
		presetValues  map[string]internal.KeyData
		cmd           string
		key           string
		time          int
		expireAtOpts  ExpireAtOptions
		pexpireAtOpts PExpireAtOptions
		want          int
		wantErr       bool
	}{
		{
			name:         "Set new expire by unix seconds",
			cmd:          "EXPIREAT",
			key:          "key1",
			expireAtOpts: ExpireAtOptions{},
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			presetValues: map[string]internal.KeyData{
				"key1": {Value: "value1", ExpireAt: time.Time{}},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:          "Set new expire by milliseconds",
			cmd:           "PEXPIREAT",
			key:           "key2",
			pexpireAtOpts: PExpireAtOptions{},
			time:          int(mockClock.Now().Add(1000 * time.Second).UnixMilli()),
			presetValues: map[string]internal.KeyData{
				"key2": {Value: "value2", ExpireAt: time.Time{}},
			},
			want:    1,
			wantErr: false,
		},
		{ // 3.
			name:         "Set new expire only when key does not have an expiry time with NX flag",
			cmd:          "EXPIREAT",
			key:          "key3",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{NX: true},
			presetValues: map[string]internal.KeyData{
				"key3": {Value: "value3", ExpireAt: time.Time{}},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:         "Return 0, when NX flag is provided and key already has an expiry time",
			cmd:          "EXPIREAT",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{NX: true},
			key:          "key4",
			presetValues: map[string]internal.KeyData{
				"key4": {Value: "value4", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:         "Set new expire time from now key only when the key already has an expiry time with XX flag",
			cmd:          "EXPIREAT",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			key:          "key5",
			expireAtOpts: ExpireAtOptions{XX: true},
			presetValues: map[string]internal.KeyData{
				"key5": {Value: "value5", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:         "Return 0 when key does not have an expiry and the XX flag is provided",
			cmd:          "EXPIREAT",
			key:          "key6",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{XX: true},
			presetValues: map[string]internal.KeyData{
				"key6": {Value: "value6", ExpireAt: time.Time{}},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:         "Set expiry time when the provided time is after the current expiry time when GT flag is provided",
			cmd:          "EXPIREAT",
			key:          "key7",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{GT: true},
			presetValues: map[string]internal.KeyData{
				"key7": {Value: "value7", ExpireAt: mockClock.Now().Add(30 * time.Second)},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:         "Return 0 when GT flag is passed and current expiry time is greater than provided time",
			cmd:          "EXPIREAT",
			key:          "key8",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{GT: true},
			presetValues: map[string]internal.KeyData{
				"key8": {Value: "value8", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:         "Return 0 when GT flag is passed and key does not have an expiry time",
			cmd:          "EXPIREAT",
			key:          "key9",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{GT: true},
			presetValues: map[string]internal.KeyData{
				"key9": {Value: "value9", ExpireAt: time.Time{}},
			},
			want: 0,
		},
		{
			name:         "Set expiry time when the provided time is before the current expiry time when LT flag is provided",
			cmd:          "EXPIREAT",
			key:          "key10",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{LT: true},
			presetValues: map[string]internal.KeyData{
				"key10": {Value: "value10", ExpireAt: mockClock.Now().Add(3000 * time.Second)},
			},
			want:    1,
			wantErr: false,
		},
		{
			name:         "Return 0 when LT flag is passed and current expiry time is less than provided time",
			cmd:          "EXPIREAT",
			key:          "key11",
			time:         int(mockClock.Now().Add(3000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{LT: true},
			presetValues: map[string]internal.KeyData{
				"key11": {Value: "value11", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			want:    0,
			wantErr: false,
		},
		{
			name:         "Return 0 when LT flag is passed and key does not have an expiry time",
			cmd:          "EXPIREAT",
			key:          "key12",
			time:         int(mockClock.Now().Add(1000 * time.Second).Unix()),
			expireAtOpts: ExpireAtOptions{LT: true},
			presetValues: map[string]internal.KeyData{
				"key12": {Value: "value12", ExpireAt: time.Time{}},
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValues != nil {
				for k, d := range tt.presetValues {
					presetKeyData(server, context.Background(), k, d)
				}
			}
			var got int
			var err error
			if strings.EqualFold(tt.cmd, "PEXPIREAT") {
				got, err = server.PExpireAt(tt.key, tt.time, tt.pexpireAtOpts)
			} else {
				got, err = server.ExpireAt(tt.key, tt.time, tt.expireAtOpts)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("(P)EXPIREAT() error = %v, wantErr %v, KEY %s", err, tt.wantErr, tt.key)
				return
			}
			if got != tt.want {
				t.Errorf("(P)EXPIREAT() got = %v, want %v, KEY %s", got, tt.want, tt.key)
			}
		})
	}
}

func TestEchoVault_EXPIRETIME(t *testing.T) {
	mockClock := clock.NewClock()

	server := createEchoVault()

	tests := []struct {
		name           string
		presetValues   map[string]internal.KeyData
		key            string
		expiretimeFunc func(key string) (int, error)
		want           int
		wantErr        bool
	}{
		{
			name: "Return expire time in seconds",
			key:  "key1",
			presetValues: map[string]internal.KeyData{
				"key1": {Value: "value1", ExpireAt: mockClock.Now().Add(100 * time.Second)},
			},
			expiretimeFunc: server.ExpireTime,
			want:           int(mockClock.Now().Add(100 * time.Second).Unix()),
			wantErr:        false,
		},
		{
			name: "Return expire time in milliseconds",
			key:  "key2",
			presetValues: map[string]internal.KeyData{
				"key2": {Value: "value2", ExpireAt: mockClock.Now().Add(4096 * time.Millisecond)},
			},
			expiretimeFunc: server.PExpireTime,
			want:           int(mockClock.Now().Add(4096 * time.Millisecond).UnixMilli()),
			wantErr:        false,
		},
		{
			name: "If the key is non-volatile, return -1",
			key:  "key3",
			presetValues: map[string]internal.KeyData{
				"key3": {Value: "value3", ExpireAt: time.Time{}},
			},
			expiretimeFunc: server.PExpireTime,
			want:           -1,
			wantErr:        false,
		},
		{
			name:           "If the key is non-existent return -2",
			presetValues:   nil,
			expiretimeFunc: server.PExpireTime,
			want:           -2,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValues != nil {
				for k, d := range tt.presetValues {
					presetKeyData(server, context.Background(), k, d)
				}
			}
			got, err := tt.expiretimeFunc(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("(P)EXPIRETIME() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("(P)EXPIRETIME() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_GET(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        string
		wantErr     bool
	}{
		{
			name:        "Return string from existing key",
			presetValue: "value1",
			key:         "key1",
			want:        "value1",
			wantErr:     false,
		},
		{
			name:        "Return empty string if the key does not exist",
			presetValue: nil,
			key:         "key2",
			want:        "",
			wantErr:     false,
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
			got, err := server.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GET() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GET() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_MGET(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]interface{}
		keys         []string
		want         []string
		wantErr      bool
	}{
		{
			name:         "Get all values in the same order the keys were provided in",
			presetValues: map[string]interface{}{"key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4"},
			keys:         []string{"key1", "key4", "key2", "key3", "key1"},
			want:         []string{"value1", "value4", "value2", "value3", "value1"},
			wantErr:      false,
		},
		{
			name:         "Return empty strings for non-existent keys",
			presetValues: map[string]interface{}{"key5": "value5", "key6": "value6", "key7": "value7"},
			keys:         []string{"key5", "key6", "non-existent", "non-existent", "key7", "non-existent"},
			want:         []string{"value5", "value6", "", "", "value7", ""},
			wantErr:      false,
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
			got, err := server.MGet(tt.keys...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MGET() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("MGET() got = %v, want %v", got, tt.want)
			}
			for _, g := range got {
				if !slices.Contains(tt.want, g) {
					t.Errorf("MGET() got = %v, want %v", got, tt.want)
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MGET() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SET(t *testing.T) {
	mockClock := clock.NewClock()

	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]internal.KeyData
		key          string
		value        string
		options      SetOptions
		want         string
		wantErr      bool
	}{
		{
			name:         "Set normal value",
			presetValues: nil,
			key:          "key1",
			value:        "value1",
			options:      SetOptions{},
			want:         "OK",
			wantErr:      false,
		},
		{
			name:         "Only set the value if the key does not exist",
			presetValues: nil,
			key:          "key2",
			value:        "value2",
			options:      SetOptions{NX: true},
			want:         "OK",
			wantErr:      false,
		},
		{
			name: "Throw error when value already exists with NX flag passed",
			presetValues: map[string]internal.KeyData{
				"key3": {
					Value:    "preset-value3",
					ExpireAt: time.Time{},
				},
			},
			key:     "key3",
			value:   "value3",
			options: SetOptions{NX: true},
			want:    "",
			wantErr: true,
		},
		{
			name: "Set new key value when key exists with XX flag passed",
			presetValues: map[string]internal.KeyData{
				"key4": {
					Value:    "preset-value4",
					ExpireAt: time.Time{},
				},
			},
			key:     "key4",
			value:   "value4",
			options: SetOptions{XX: true},
			want:    "OK",
			wantErr: false,
		},
		{
			name:         "Return error when setting non-existent key with XX flag",
			presetValues: nil,
			key:          "key5",
			value:        "value5",
			options:      SetOptions{XX: true},
			want:         "",
			wantErr:      true,
		},
		{
			name:         "Set expiry time on the key to 100 seconds from now",
			presetValues: nil,
			key:          "key6",
			value:        "value6",
			options:      SetOptions{EX: 100},
			want:         "OK",
			wantErr:      false,
		},
		{
			name:         "Set expiry time on the key in unix milliseconds",
			presetValues: nil,
			key:          "key7",
			value:        "value7",
			options:      SetOptions{PX: 4096},
			want:         "OK",
			wantErr:      false,
		},
		{
			name:         "Set exact expiry time in seconds from unix epoch",
			presetValues: nil,
			key:          "key8",
			value:        "value8",
			options:      SetOptions{EXAT: int(mockClock.Now().Add(200 * time.Second).Unix())},
			want:         "OK",
			wantErr:      false,
		},
		{
			name:         "Set exact expiry time in milliseconds from unix epoch",
			key:          "key9",
			value:        "value9",
			options:      SetOptions{PXAT: int(mockClock.Now().Add(4096 * time.Millisecond).UnixMilli())},
			presetValues: nil,
			want:         "OK",
			wantErr:      false,
		},
		{
			name: "Get the previous value when GET flag is passed",
			presetValues: map[string]internal.KeyData{
				"key10": {
					Value:    "previous-value",
					ExpireAt: time.Time{},
				},
			},
			key:     "key10",
			value:   "value10",
			options: SetOptions{GET: true, EX: 1000},
			want:    "previous-value",
			wantErr: false,
		},
		{
			name:         "Return nil when GET value is passed and no previous value exists",
			presetValues: nil,
			key:          "key11",
			value:        "value11",
			options:      SetOptions{GET: true, EX: 1000},
			want:         "",
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValues != nil {
				for k, d := range tt.presetValues {
					presetKeyData(server, context.Background(), k, d)
				}
			}
			got, err := server.Set(tt.key, tt.value, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("SET() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SET() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_MSET(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name    string
		kvPairs map[string]string
		want    string
		wantErr bool
	}{
		{
			name:    "Set multiple keys",
			kvPairs: map[string]string{"key1": "value1", "key2": "10", "key3": "3.142"},
			want:    "OK",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := server.MSet(tt.kvPairs)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSET() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MSET() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_PERSIST(t *testing.T) {
	mockClock := clock.NewClock()

	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]internal.KeyData
		key          string
		want         bool
		wantErr      bool
	}{
		{
			name: "Successfully persist a volatile key",
			key:  "key1",
			presetValues: map[string]internal.KeyData{
				"key1": {Value: "value1", ExpireAt: mockClock.Now().Add(1000 * time.Second)},
			},
			want:    true,
			wantErr: false,
		},
		{
			name:         "Return false when trying to persist a non-existent key",
			key:          "key2",
			presetValues: nil,
			want:         false,
			wantErr:      false,
		},
		{
			name: "Return false when trying to persist a non-volatile key",
			key:  "key3",
			presetValues: map[string]internal.KeyData{
				"key3": {Value: "value3", ExpireAt: time.Time{}},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValues != nil {
				for k, d := range tt.presetValues {
					presetKeyData(server, context.Background(), k, d)
				}
			}
			got, err := server.Persist(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("PERSIST() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PERSIST() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_TTL(t *testing.T) {
	mockClock := clock.NewClock()

	server := createEchoVault()

	tests := []struct {
		name         string
		presetValues map[string]internal.KeyData
		key          string
		ttlFunc      func(key string) (int, error)
		want         int
		wantErr      bool
	}{
		{
			name: "Return TTL time in seconds",
			key:  "key1",
			presetValues: map[string]internal.KeyData{
				"key1": {Value: "value1", ExpireAt: mockClock.Now().Add(100 * time.Second)},
			},
			ttlFunc: server.TTL,
			want:    100,
			wantErr: false,
		},
		{
			name:    "Return TTL time in milliseconds",
			key:     "key2",
			ttlFunc: server.PTTL,
			presetValues: map[string]internal.KeyData{
				"key2": {Value: "value2", ExpireAt: mockClock.Now().Add(4096 * time.Millisecond)},
			},
			want:    4096,
			wantErr: false,
		},
		{
			name:    "If the key is non-volatile, return -1",
			key:     "key3",
			ttlFunc: server.TTL,
			presetValues: map[string]internal.KeyData{
				"key3": {Value: "value3", ExpireAt: time.Time{}},
			},
			want:    -1,
			wantErr: false,
		},
		{
			name:         "If the key is non-existent return -2",
			key:          "key4",
			ttlFunc:      server.TTL,
			presetValues: nil,
			want:         -2,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValues != nil {
				for k, d := range tt.presetValues {
					presetKeyData(server, context.Background(), k, d)
				}
			}
			got, err := tt.ttlFunc(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("TTL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("TTL() got = %v, want %v", got, tt.want)
			}
		})
	}
}
