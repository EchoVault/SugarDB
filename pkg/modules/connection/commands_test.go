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

package connection

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/echovault"
	"github.com/echovault/echovault/pkg/utils"
	"github.com/tidwall/resp"
	"testing"
)

var mockServer *echovault.EchoVault

func init() {
	mockServer = echovault.NewEchoVault(
		echovault.WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: utils.NoEviction,
		}),
	)
}

func Test_HandlePing(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		command     []string
		expected    string
		expectedErr error
	}{
		{
			command:     []string{"PING"},
			expected:    "PONG",
			expectedErr: nil,
		},
		{
			command:     []string{"PING", "Hello, world!"},
			expected:    "Hello, world!",
			expectedErr: nil,
		},
		{
			command:     []string{"PING", "Hello, world!", "Once more"},
			expected:    "",
			expectedErr: errors.New(utils.WrongArgsResponse),
		},
	}

	for _, test := range tests {
		res, err := handlePing(ctx, test.command, mockServer, nil)
		if test.expectedErr != nil && err != nil {
			if err.Error() != test.expectedErr.Error() {
				t.Errorf("expected error %s, got: %s", test.expectedErr.Error(), err.Error())
			}
			continue
		}
		if err != nil {
			t.Error(err)
		}
		rd := resp.NewReader(bytes.NewBuffer(res))
		v, _, err := rd.ReadValue()
		if err != nil {
			t.Error(err)
		}
		if v.String() != test.expected {
			t.Errorf("expected %s, got: %s", test.expected, v.String())
		}
	}
}
