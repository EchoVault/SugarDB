package connection

import (
	"bytes"
	"context"
	"errors"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_HandlePing(t *testing.T) {
	ctx := context.Background()
	mockServer := server.NewServer(server.Opts{})

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
