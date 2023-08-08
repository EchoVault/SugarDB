package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/kelvinmwinuka/memstore/server/utils"
)

func TestHandleCommandSuccess(t *testing.T) {
	server := &utils.MockServer{}

	tests := []struct {
		cmd      []string
		expected []byte
	}{
		{[]string{"ping"}, []byte("+PONG\r\n\n")},
		{[]string{"ping", "Ping Test"}, []byte("+Ping Test\r\n\n")},
	}

	for _, tt := range tests {
		res, _ := Plugin.HandleCommand(tt.cmd, server)
		if !bytes.Equal(tt.expected, res) {
			t.Errorf("Expected %s, Got %s", strings.TrimSpace(string(tt.expected)), strings.TrimSpace(string(res)))
		}
	}
}

func TestHandleCommandError(t *testing.T) {
	server := &utils.MockServer{}

	tests := []struct {
		cmd      []string
		expected error
	}{
		{[]string{"ping", "Ping Test", "Error"}, errors.New("wrong number of arguments for PING command")},
	}

	for _, tt := range tests {
		_, err := Plugin.HandleCommand(tt.cmd, server)
		if tt.expected.Error() != err.Error() {
			t.Errorf("Expected %s, Got %s", tt.expected.Error(), err.Error())
		}
	}
}
