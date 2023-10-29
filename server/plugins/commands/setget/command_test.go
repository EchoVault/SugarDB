package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/kelvinmwinuka/memstore/server/utils"
)

const (
	OK = "+OK\r\n\n"
)

func TestHandleCommand(t *testing.T) {
	server := utils.MockServer{}

	tests := []struct {
		cmd      []string
		expected interface{}
	}{
		// SET test cases
		{[]string{"set", "key1", "value1"}, []byte(OK)},
		{[]string{"set", "key2", "30"}, []byte(OK)},
		{[]string{"set", "key3", "3.142"}, []byte(OK)},
		{[]string{"set", "key4", "part1", "part2", "part3"}, errors.New("wrong number of args for SET command")},
		{[]string{"set"}, errors.New("wrong number of args for SET command")},

		// GET test cases
		{[]string{"get", "key1"}, []byte("+value1\r\n\n")},
		{[]string{"get", "key2"}, []byte("+30\r\n\n")},
		{[]string{"get", "key3"}, []byte("+3.142\r\n\n")},
		{[]string{"get", "key4"}, []byte("+nil\r\n\n")},
		{[]string{"get"}, errors.New("wrong number of args for GET command")},
		{[]string{"get", "key1", "key2"}, errors.New("wrong number of args for GET command")},

		// MGET test cases
		{[]string{"mget", "key1", "key2", "key3", "key4"}, []byte("*4\r\n$6\r\nvalue1\r\n$2\r\n30\r\n$5\r\n3.142\r\n$3\r\nnil\r\n\n")},
		{[]string{"mget", "key5", "key6"}, []byte("*2\r\n$3\r\nnil\r\n$3\r\nnil\r\n\n")},
		{[]string{"mget"}, errors.New("wrong number of args for MGET command")},
	}

	for _, tt := range tests {
		res, err := Plugin.HandleCommand(tt.cmd, &server)

		if err != nil {
			if tt.expected.(error).Error() != err.Error() {
				t.Errorf("Expected %s, Got %s", tt.expected.(error).Error(), err.Error())
			}
		} else {
			if !bytes.Equal(tt.expected.([]byte), res) {
				t.Errorf("Expected %s, Got %s", strings.TrimSpace(string(tt.expected.([]byte))), strings.TrimSpace(string(res)))
			}
		}
	}
}
