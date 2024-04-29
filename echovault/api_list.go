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
	"fmt"
	"github.com/echovault/echovault/internal"
	"strconv"
)

// LLen returns the length of the list.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// Returns: The length of the list as an integer. Returns 0 if the key does not exist.
//
// Errors:
//
// "LLen command on non-list item" - when the provided key exists but is not a list.
func (server *EchoVault) LLen(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LLEN", key}), nil, false, true)
	fmt.Println(key, string(b), err)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// LRange returns the elements within the index range provided.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `start` - int - the start index. If start index is less than end index, the returned sub-list will be reversed.
//
// `end` - int - the end index. When -1 is passed for end index, the function will return the list from start
// index to the end of the list.
//
// Returns: A string slice containing the elements within the given indices.
//
// Errors:
//
// "LRange command on non-list item" - when the provided key exists but is not a list.
//
// "start index must be within list boundary" - when the start index is not within the list boundaries.
//
// "end index must be within list range or -1" - when end index is not within the list boundaries.
func (server *EchoVault) LRange(key string, start, end int) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LRANGE", key, strconv.Itoa(start), strconv.Itoa(end)}), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// LIndex retrieves the element at the provided index from the list without removing it.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `index` - int - the index to retrieve from.
//
// Returns: The element at the given index as a string.
//
// Errors:
//
// "LIndex command on non-list item" - when the provided key exists but is not a list.
//
// "index must be within list range" - when the index is not within the list boundary.
func (server *EchoVault) LIndex(key string, index uint) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LINDEX", key, strconv.Itoa(int(index))}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LSet updates the value at the given index of a list.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `index` - int - the index to retrieve from.
//
// `value` - string - the new value to place at the given index.
//
// Returns: "OK" if the update is successful.
//
// Errors:
//
// "LSet command on non-list item" - when the provided key exists but is not a list.
//
// "index must be within list range" - when the index is not within the list boundary.
func (server *EchoVault) LSet(key string, index int, value string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LSET", key, strconv.Itoa(index), value}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LTrim work similarly to LRange but instead of returning the new list, it replaces the original list with the
// trimmed list.
func (server *EchoVault) LTrim(key string, start int, end int) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LTRIM", key, strconv.Itoa(start), strconv.Itoa(end)}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LRem removes 'count' instances of the specified element from the list.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `count` - int - the number of instances of the element to remove.
//
// `value` - string - the element to remove.
//
// Returns: "OK" if the removal was successful.
//
// Errors:
//
// "LRem command on non-list item" - when the provided key exists but is not a list.
func (server *EchoVault) LRem(key string, count int, value string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LREM", key, strconv.Itoa(count), value}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LMove moves an element from one list to another.
//
// Parameters:
//
// `source` - string - the key to the source list.
//
// `destination` - string - the key to the destination list.
//
// `whereFrom` - string - either "LEFT" or "RIGHT". If "LEFT", the element is removed from the beginning of the source list.
// If "RIGHT", the element is removed from the end of the source list.
//
// `whereTo` - string - either "LEFT" or "RIGHT". If "LEFT", the element is added to the beginning of the destination list.
// If "RIGHT", the element is added to the end of the destination list.
//
// Returns: "OK" if the removal was successful.
//
// Errors:
//
// "both source and destination must be lists" - when either source or destination are not lists.
//
// "wherefrom and whereto arguments must be either LEFT or RIGHT" - if whereFrom or whereTo are not either "LEFT" or "RIGHT".
func (server *EchoVault) LMove(source, destination, whereFrom, whereTo string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LMOVE", source, destination, whereFrom, whereTo}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LPop pops an element from the start of the list and return it.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// Returns: The popped element as a string.
//
// Errors:
//
// "LPop command on non-list item" - when the provided key is not a list.
func (server *EchoVault) LPop(key string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"LPOP", key}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// RPop pops an element from the end of the list and return it.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// Returns: The popped element as a string.
//
// Errors:
//
// "RPop command on non-list item" - when the provided key is not a list.
func (server *EchoVault) RPop(key string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"RPOP", key}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// LPush pushed 1 or more values to the beginning of a list. If the list does not exist, a new list is created
// wth the passed elements as its members.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `values` - ...string - the list of elements to add to push to the beginning of the list.
//
// Returns: An integer with the length of the new list.
//
// Errors:
//
// "LPush command on non-list item" - when the provided key is not a list.
func (server *EchoVault) LPush(key string, values ...string) (int, error) {
	cmd := append([]string{"LPUSH", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// LPushX pushed 1 or more values to the beginning of an existing list. The command only succeeds on a pre-existing list.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `values` - ...string - the list of elements to add to push to the beginning of the list.
//
// Returns: An integer with the length of the new list.
//
// Errors:
//
// "LPushX command on non-list item" - when the provided key is not a list or doesn't exist.
func (server *EchoVault) LPushX(key string, values ...string) (int, error) {
	cmd := append([]string{"LPUSHX", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// RPush pushed 1 or more values to the end of a list. If the list does not exist, a new list is created
// wth the passed elements as its members.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `values` - ...string - the list of elements to add to push to the end of the list.
//
// Returns: An integer with the length of the new list.
//
// Errors:
//
// "RPush command on non-list item" - when the provided key is not a list.
func (server *EchoVault) RPush(key string, values ...string) (int, error) {
	cmd := append([]string{"RPUSH", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// RPushX pushed 1 or more values to the end of an existing list. The command only succeeds on a pre-existing list.
//
// Parameters:
//
// `key` - string - the key to the list.
//
// `values` - ...string - the list of elements to add to push to the end of the list.
//
// Returns: An integer with the length of the new list.
//
// Errors:
//
// "RPushX command on non-list item" - when the provided key is not a list or doesn't exist.
func (server *EchoVault) RPushX(key string, values ...string) (int, error) {
	cmd := append([]string{"RPUSHX", key}, values...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}
