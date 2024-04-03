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
	"github.com/echovault/echovault/internal"
	"strconv"
)

// SETRANGE replaces a portion of the string at the provided key starting at the offset with a new string.
// If the string does not exist, a new string is created.
//
// Returns: The length of the new string as an integers.
//
// Errors:
//
// - "value at key <key> is not a string" when the key provided does not hold a string.
func (server *EchoVault) SETRANGE(key string, offset int, new string) (int, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SETRANGE", key, strconv.Itoa(offset), new}),
		nil,
		false,
	)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// STRLEN returns the length of the string at the provided key.
//
// Returns: The length of the string as an integer.
//
// Errors:
//
// - "value at key <key> is not a string" - when the value at the keys is not a string.
func (server *EchoVault) STRLEN(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"STRLEN", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SUBSTR returns a substring from the string at the key.
// The start and end indices are integers that specify the lower and upper bound respectively.
//
// Returns: The substring from the start index to the end index.
//
// Errors:
//
// - "key <key> does not exist" - when the key does not exist.
//
// - "value at key <key> is not a string" - when the value at the keys is not a string.
func (server *EchoVault) SUBSTR(key string, start, end int) (string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SUBSTR", key, strconv.Itoa(start), strconv.Itoa(end)}),
		nil,
		false,
	)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// GETRANGE works the same as SUBSTR.
func (server *EchoVault) GETRANGE(key string, start, end int) (string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"GETRANGE", key, strconv.Itoa(start), strconv.Itoa(end)}),
		nil,
		false,
	)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
