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

package str

import (
	"context"
	"github.com/echovault/echovault/echovault"
	"github.com/echovault/echovault/internal/config"
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

func TestEchoVault_SUBSTR(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		substrFunc  func(key string, start int, end int) (string, error)
		key         string
		start       int
		end         int
		want        string
		wantErr     bool
	}{
		{
			name:        "Return substring within the range of the string",
			key:         "key1",
			substrFunc:  server.SUBSTR,
			presetValue: "Test String One",
			start:       5,
			end:         10,
			want:        "String",
			wantErr:     false,
		},
		{
			name:        "Return substring at the end of the string with exact end index",
			key:         "key2",
			substrFunc:  server.SUBSTR,
			presetValue: "Test String Two",
			start:       12,
			end:         14,
			want:        "Two",
			wantErr:     false,
		},
		{
			name:        "Return substring at the end of the string with end index greater than length",
			key:         "key3",
			substrFunc:  server.SUBSTR,
			presetValue: "Test String Three",
			start:       12,
			end:         75,
			want:        "Three",
		},
		{
			name:        "Return the substring at the start of the string with 0 start index",
			key:         "key4",
			substrFunc:  server.SUBSTR,
			presetValue: "Test String Four",
			start:       0,
			end:         3,
			want:        "Test",
			wantErr:     false,
		},
		{
			// Return the substring with negative start index.
			// Substring should begin abs(start) from the end of the string when start is negative.
			name:        "Return the substring with negative start index",
			key:         "key5",
			substrFunc:  server.SUBSTR,
			presetValue: "Test String Five",
			start:       -11,
			end:         10,
			want:        "String",
			wantErr:     false,
		},
		{
			// Return reverse substring with end index smaller than start index.
			// When end index is smaller than start index, the 2 indices are reversed.
			name:        "Return reverse substring with end index smaller than start index",
			key:         "key6",
			substrFunc:  server.SUBSTR,
			presetValue: "Test String Six",
			start:       4,
			end:         0,
			want:        "tseT",
		},
		{
			name:        "Return substring within the range of the string",
			key:         "key7",
			substrFunc:  server.GETRANGE,
			presetValue: "Test String One",
			start:       5,
			end:         10,
			want:        "String",
			wantErr:     false,
		},
		{
			name:        "Return substring at the end of the string with exact end index",
			key:         "key8",
			substrFunc:  server.GETRANGE,
			presetValue: "Test String Two",
			start:       12,
			end:         14,
			want:        "Two",
			wantErr:     false,
		},
		{
			name:        "Return substring at the end of the string with end index greater than length",
			key:         "key9",
			substrFunc:  server.GETRANGE,
			presetValue: "Test String Three",
			start:       12,
			end:         75,
			want:        "Three",
		},
		{
			name:        "Return the substring at the start of the string with 0 start index",
			key:         "key10",
			substrFunc:  server.GETRANGE,
			presetValue: "Test String Four",
			start:       0,
			end:         3,
			want:        "Test",
			wantErr:     false,
		},
		{
			// Return the substring with negative start index.
			// Substring should begin abs(start) from the end of the string when start is negative.
			name:        "Return the substring with negative start index",
			key:         "key11",
			substrFunc:  server.GETRANGE,
			presetValue: "Test String Five",
			start:       -11,
			end:         10,
			want:        "String",
			wantErr:     false,
		},
		{
			// Return reverse substring with end index smaller than start index.
			// When end index is smaller than start index, the 2 indices are reversed.
			name:        "Return reverse substring with end index smaller than start index",
			key:         "key12",
			substrFunc:  server.GETRANGE,
			presetValue: "Test String Six",
			start:       4,
			end:         0,
			want:        "tseT",
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
			got, err := tt.substrFunc(tt.key, tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("GETRANGE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GETRANGE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_SETRANGE(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		offset      int
		new         string
		want        int
		wantErr     bool
	}{
		{
			name:        "Test that SETRANGE on non-existent string creates new string",
			key:         "key1",
			presetValue: "",
			offset:      10,
			new:         "New String Value",
			want:        len("New String Value"),
			wantErr:     false,
		},
		{
			name:        "Test SETRANGE with an offset that leads to a longer resulting string",
			key:         "key2",
			presetValue: "Original String Value",
			offset:      16,
			new:         "Portion Replaced With This New String",
			want:        len("Original String Portion Replaced With This New String"),
			wantErr:     false,
		},
		{
			name:        "SETRANGE with negative offset prepends the string",
			key:         "key3",
			presetValue: "This is a preset value",
			offset:      -10,
			new:         "Prepended ",
			want:        len("Prepended This is a preset value"),
			wantErr:     false,
		},
		{
			name:        "SETRANGE with offset that embeds new string inside the old string",
			key:         "key4",
			presetValue: "This is a preset value",
			offset:      0,
			new:         "That",
			want:        len("That is a preset value"),
			wantErr:     false,
		},
		{
			name:        "SETRANGE with offset longer than original lengths appends the string",
			key:         "key5",
			presetValue: "This is a preset value",
			offset:      100,
			new:         " Appended",
			want:        len("This is a preset value Appended"),
			wantErr:     false,
		},
		{
			name:        "SETRANGE with offset on the last character replaces last character with new string",
			key:         "key6",
			presetValue: "This is a preset value",
			offset:      len("This is a preset value") - 1,
			new:         " replaced",
			want:        len("This is a preset valu replaced"),
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
			got, err := server.SETRANGE(tt.key, tt.offset, tt.new)
			if (err != nil) != tt.wantErr {
				t.Errorf("SETRANGE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SETRANGE() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEchoVault_STRLEN(t *testing.T) {
	server := createEchoVault()

	tests := []struct {
		name        string
		presetValue interface{}
		key         string
		want        int
		wantErr     bool
	}{
		{
			name:        "Return the correct string length for an existing string",
			key:         "key1",
			presetValue: "Test String",
			want:        len("Test String"),
			wantErr:     false,
		},
		{
			name:        "If the string does not exist, return 0",
			key:         "key2",
			presetValue: "",
			want:        0,
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
			got, err := server.STRLEN(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("STRLEN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("STRLEN() got = %v, want %v", got, tt.want)
			}
		})
	}
}
