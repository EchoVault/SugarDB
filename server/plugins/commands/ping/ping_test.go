package main

import (
	"bufio"
	"strings"
	"testing"

	"github.com/kelvinmwinuka/memstore/utils"
)

func TestHandleCommand(t *testing.T) {
	server := &utils.MockServer{}

	cw := &utils.CustomWriter{}
	writer := bufio.NewWriter(cw)

	tests := []struct {
		cmd      []string
		expected string
	}{
		{[]string{"ping"}, "+PONG\r\n\n"},
		{[]string{"ping", "Ping Test"}, "+Ping Test\r\n\n"},
		{[]string{"ping", "Ping Test", "Error"}, "-Error wrong number of arguments for PING command\r\n\n"},
	}

	for _, tt := range tests {
		cw.Buf.Reset()
		Plugin.HandleCommand(tt.cmd, server, writer)
		if tt.expected != cw.Buf.String() {
			t.Errorf("Expected %s, Got %s", strings.TrimSpace(tt.expected), strings.TrimSpace(cw.Buf.String()))
		}
	}
}
