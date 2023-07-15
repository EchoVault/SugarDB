package main

import (
	"bufio"
	"strings"
	"sync"
	"testing"

	"github.com/kelvinmwinuka/memstore/utils"
)

const (
	OK = "+OK\r\n\n"
)

func TestHandleCommand(t *testing.T) {
	server := utils.MockServer{
		Data: utils.MockData{
			Mu:   sync.Mutex{},
			Data: make(map[string]interface{}),
		},
	}

	cw := utils.CustomWriter{}
	writer := bufio.NewWriter(&cw)

	tests := []struct {
		cmd      []string
		expected string
	}{
		// SET test cases
		{[]string{"set", "key1", "value1"}, OK},
		{[]string{"set", "key2", "30"}, OK},
		{[]string{"set", "key3", "3.142"}, OK},
		{[]string{"set", "key4", "part1", "part2", "part3"}, "-Error wrong number of args for SET command\r\n\n"},
		{[]string{"set"}, "-Error wrong number of args for SET command\r\n\n"},

		// GET test cases
		{[]string{"get", "key1"}, "+value1\r\n\n"},
		{[]string{"get", "key2"}, ":30\r\n\n"},
		{[]string{"get", "key3"}, "+3.142\r\n\n"},
		{[]string{"get", "key4"}, "+nil\r\n\n"},
		{[]string{"get"}, "-Error wrong number of args for GET command\r\n\n"},
		{[]string{"get", "key1", "key2"}, "-Error wrong number of args for GET command\r\n\n"},

		// MGET test cases
		{[]string{"mget", "key1", "key2", "key3", "key4"}, "*4\r\n$6\r\nvalue1\r\n$2\r\n30\r\n$5\r\n3.142\r\n$3\r\nnil\r\n\n"},
		{[]string{"mget", "key5", "key6"}, "*2\r\n$3\r\nnil\r\n$3\r\nnil\r\n\n"},
		{[]string{"mget"}, "-Error wrong number of args for MGET command\r\n\n"},
	}

	for _, tt := range tests {
		cw.Buf.Reset()
		Plugin.HandleCommand(tt.cmd, &server, writer)
		if tt.expected != cw.Buf.String() {
			t.Errorf("Expected %s, Got %s", strings.TrimSpace(tt.expected), strings.TrimSpace(cw.Buf.String()))
		}
	}
}
