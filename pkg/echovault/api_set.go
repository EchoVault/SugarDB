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

// SADD adds member(s) to a set. If the set does not exist, a new sorted set is created with the
// member(s).
//
// Parameters:
//
// `key` - string - the key to update.
//
// `members` - ...string - a list of members to add to the set.
//
// Returns: The number of members added.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SADD(key string, members ...string) (int, error) {
	cmd := append([]string{"SADD", key}, members...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SCARD Returns the cardinality of the set.
//
// Parameters:
//
// `key` - string - the key to update.
//
// Returns: The cardinality of a set. Returns 0 if the key does not exist.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SCARD(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SCARD", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SDIFF Calculates the difference between the provided sets. Keys that don't exist or that are not sets
// will be skipped.
//
// Parameters:
//
// `keys` - ...string - the keys of the sets from which to calculate the difference.
//
// Returns: A string slice representing the elements resulting from calculating the difference.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
//
// "key for base set <key> does not exist" - if the first key is not a set.
func (server *EchoVault) SDIFF(keys ...string) ([]string, error) {
	cmd := append([]string{"SDIFF"}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// SDIFFSTORE works like SDIFF but instead of returning the resulting set elements, the resulting set is stored
// at the 'destination' key.
func (server *EchoVault) SDIFFSTORE(destination string, keys ...string) (int, error) {
	cmd := append([]string{"SDIFFSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SINTER Calculates the intersection between the provided sets. If any of the keys does not exist,
// then there is no intersection.
//
// Parameters:
//
// `keys` - ...string - the keys of the sets from which to calculate the intersection.
//
// Returns: A string slice representing the elements resulting from calculating the intersection.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
//
// "not enough sets in the keys provided" - when only one of the provided keys is a valid set.
func (server *EchoVault) SINTER(keys ...string) ([]string, error) {
	cmd := append([]string{"SINTER"}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// SINTERCARD Calculates the cardinality of the intersection between the sets provided.
//
// Parameters:
//
// `keys` - []string - The keys of the sets from which to calculate the intersection.
//
// `limit` - int - When limit is > 0, the intersection calculation will be terminated as soon as the limit is reached.
//
// Returns: The cardinality of the calculated intersection.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
//
// "not enough sets in the keys provided" - when only one of the provided keys is a valid set.
func (server *EchoVault) SINTERCARD(keys []string, limit uint) (int, error) {
	cmd := append([]string{"SINTERCARD"}, keys...)
	if limit > 0 {
		cmd = append(cmd, []string{"LIMIT", strconv.Itoa(int(limit))}...)
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SINTERSTORE works the same as SINTER but instead of returning the elements in the resulting set, it is stored
// at the 'destination' key and the cardinality of the resulting set is returned.
func (server *EchoVault) SINTERSTORE(destination string, keys ...string) (int, error) {
	cmd := append([]string{"SINTERSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SISMEMBER Returns if member is contained in the set.
//
// Parameters:
//
// `key` - string - The key of the set.
//
// `member` - string - The member whose membership status will be checked.
//
// Returns: true if the member exists in the set, false otherwise.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SISMEMBER(key, member string) (bool, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SISMEMBER", key, member}), nil, false)
	if err != nil {
		return false, err
	}
	return internal.ParseBooleanResponse(b)
}

// SMEMBERS Returns all the members of the specified set.
//
// Parameters:
//
// `key` - string - The key of the set.
//
// Returns: A string slice of all the members in the set.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SMEMBERS(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"SMEMBERS", key}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// SMISMEMBER Returns the membership status of all the specified members.
//
// Parameters:
//
// `key` - string - The key of the set.
//
// `members` - ...string - The members whose membership in the set will be checked.
//
// Returns: A boolean slices with true/false based on whether the member in the corresponding index is
// present in the set.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SMISMEMBER(key string, members ...string) ([]bool, error) {
	cmd := append([]string{"SMISMEMBER", key}, members...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseBooleanArrayResponse(b)
}

// SMOVE Move the specified member from 'source' set to 'destination' set.
//
// Parameters:
//
// `source` - string - The key of the set to remove the element from.
//
// `destination` - string - The key of the set to move the element to.
//
// `member` - string - The member to move from the source set to destination set.
//
// Returns: true if the member was successfully moved, false otherwise.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
//
// "source is not a set" - when the source key does not hold a set.
//
// "destination is not a set" - when the destination key does not hold a set.
func (server *EchoVault) SMOVE(source, destination, member string) (bool, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SMOVE", source, destination, member}),
		nil,
		false,
	)
	if err != nil {
		return false, err
	}
	return internal.ParseBooleanResponse(b)
}

// SPOP Pop one or more elements from the set.
//
// Parameters:
//
// `key` - string - The key of the set.
//
// `count` - uint - number of elements to pop.
//
// Returns: A string slice containing all the popped elements. If the key does not exist, an empty array is returned.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SPOP(key string, count uint) ([]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SPOP", key, strconv.Itoa(int(count))}),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// SRANDMEMBER Returns one or more random members from the set without removing them.
//
// Parameters:
//
// `key` - string - The key of the set.
//
// `count` - int - number of elements to return. If count is negative, repeated elements are allowed.
// If the count is positive, all returned elements will be distinct.
//
// Returns: A string slice containing the random elements. If the key does not exist, an empty array is returned.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SRANDMEMBER(key string, count int) ([]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"SRANDMEMBER", key, strconv.Itoa(count)}),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// SREM Remove one or more members from a set.
//
// Parameters:
//
// `key` - string - The key of the set.
//
// `members` - ...string - List of members to remove. If the key does not exist, 0 is returned.
//
// Returns: The number of elements successfully removed.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SREM(key string, members ...string) (int, error) {
	cmd := append([]string{"SREM", key}, members...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// SUNION Calculates the union between the provided sets. Keys that don't exist or that are not sets
// will be skipped.
//
// Parameters:
//
// `keys` - ...string - the keys of the sets from which to calculate the union.
//
// Returns: A string slice representing the elements resulting from calculating the union.
//
// Errors:
//
// "value at <key> is not a set" - when the provided key exists but is not a set.
func (server *EchoVault) SUNION(keys ...string) ([]string, error) {
	cmd := append([]string{"SUNION"}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// SUNIONSTORE store works like SUNION but instead of returning the resulting elements, it stores the resulting
// set at the 'destination' key. The return value is an integer representing the cardinality of the new set.
func (server *EchoVault) SUNIONSTORE(destination string, keys ...string) (int, error) {
	cmd := append([]string{"SUNIONSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}
