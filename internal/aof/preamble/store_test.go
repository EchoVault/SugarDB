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

package preamble_test

import (
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/aof/preamble"
	"github.com/echovault/echovault/internal/clock"
	"os"
	"path"
	"testing"
	"time"
)

func Test_PreambleStore(t *testing.T) {
	directory := "./testdata/preamble"
	tests := []struct {
		name               string
		directory          string
		state              map[string]internal.KeyData
		preambleReadWriter preamble.PreambleReadWriter
		wantState          map[string]internal.KeyData
	}{
		{
			name:      "1. Preamble store with no preamble read writer passed should trigger one to be created upon initialization",
			directory: directory,
			state: map[string]internal.KeyData{
				"key1": {
					Value:    "value1",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key2": {
					Value:    "value2",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key3": {
					Value:    "value3",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
			},
			preambleReadWriter: nil,
			wantState: map[string]internal.KeyData{
				"key1": {
					Value:    "value1",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key2": {
					Value:    "value2",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key3": {
					Value:    "value3",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
			},
		},
		{
			name:      "2. Pass a pre-existing preamble read writer to constructor",
			directory: directory,
			state: map[string]internal.KeyData{
				"key4": {
					Value:    "value4",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key5": {
					Value:    "value5",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key6": {
					Value:    "value6",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
			},
			preambleReadWriter: func() preamble.PreambleReadWriter {
				if err := os.MkdirAll(path.Join("./testdata/preamble", "aof"), os.ModePerm); err != nil {
					t.Error(err)
				}
				f, err := os.OpenFile(path.Join("./testdata/preamble", "aof", "preamble.bin"),
					os.O_RDWR|os.O_CREATE, os.ModePerm)
				if err != nil {
					t.Error(err)
				}
				return f
			}(),
			wantState: map[string]internal.KeyData{
				"key4": {
					Value:    "value4",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key5": {
					Value:    "value5",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key6": {
					Value:    "value6",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
			},
		},
		{
			name:      "3. Skip expired keys when saving/loading state from preamble read writer",
			directory: directory,
			state: map[string]internal.KeyData{
				"key7": {
					Value:    "value7",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key8": {
					Value:    "value8",
					ExpireAt: clock.NewClock().Now().Add(-10 * time.Second),
				},
				"key9": {
					Value:    "value9",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key10": {
					Value:    "value10",
					ExpireAt: clock.NewClock().Now().Add(-10 * time.Second),
				},
			},
			preambleReadWriter: nil,
			wantState: map[string]internal.KeyData{
				"key7": {
					Value:    "value7",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
				"key9": {
					Value:    "value9",
					ExpireAt: clock.NewClock().Now().Add(10 * time.Second),
				},
			},
		},
	}

	for _, test := range tests {
		options := []func(store *preamble.PreambleStore){
			preamble.WithClock(clock.NewClock()),
			preamble.WithDirectory(test.directory),
			preamble.WithGetStateFunc(func() map[string]internal.KeyData {
				return test.state
			}),
			preamble.WithSetKeyDataFunc(func(key string, data internal.KeyData) {
				entry, ok := test.wantState[key]
				if !ok {
					t.Errorf("could not find element: %v", key)
				}
				if entry.Value != data.Value {
					t.Errorf("expected value %v for key %s, got %v", entry.Value, key, data.Value)
				}
				if !entry.ExpireAt.Equal(data.ExpireAt) {
					t.Errorf("expected expireAt %v for key %s, got %v", entry.ExpireAt, key, data.ExpireAt)
				}
			}),
		}

		store, err := preamble.NewPreambleStore(options...)
		if err != nil {
			t.Error(err)
		}

		if err = store.CreatePreamble(); err != nil {
			t.Error(err)
		}

		if err = store.Restore(); err != nil {
			t.Error(err)
		}
	}

	_ = os.RemoveAll("./testdata")
}
