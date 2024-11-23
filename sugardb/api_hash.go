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

package sugardb

import (
	"fmt"
	"strconv"

	"github.com/echovault/sugardb/internal"
)

// HRandFieldOptions modifies the behaviour of the HRandField function.
//
// Count determines the number of random fields to return. If set to 0, an empty slice will be returned.
//
// WithValues determines whether the returned map should contain the values as well as the fields.
type HRandFieldOptions struct {
	Count      uint
	WithValues bool
}

// HSet creates or modifies a hash map with the values provided. If the hash map does not exist it will be created.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `fieldValuePairs` - map[string]string - a hash used to update or create the hash. Existing fields will be updated
// with the new values. Non-existent fields will be created.
//
// Returns: The number of fields that were updated/created.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key exists but is not a hash.
func (server *SugarDB) HSet(key string, fieldValuePairs map[string]string) (int, error) {
	cmd := []string{"HSET", key}

	for k, v := range fieldValuePairs {
		cmd = append(cmd, []string{k, v}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// HSetNX modifies an existing hash map with the values provided. This function will only be successful if the
// hash map already exists.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `fieldValuePairs` - map[string]string - a hash used to update the hash. Existing fields will be updated
// with the new values. Non-existent fields will be created.
//
// Returns: The number of fields that were updated/created.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HSetNX(key string, fieldValuePairs map[string]string) (int, error) {
	cmd := []string{"HSETNX", key}

	for k, v := range fieldValuePairs {
		cmd = append(cmd, []string{k, v}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// HGet retrieves the values corresponding to the provided fields.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `fields` - ...string - the list of fields to fetch.
//
// Returns: A string slice of the values corresponding to the fields in the same order the fields were provided.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HGet(key string, fields ...string) ([]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand(append([]string{"HGET", key}, fields...)),
		nil,
		false,
		true,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// HMGet retrieves the values corresponding to the provided fields.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `fields` - ...string - the list of fields to fetch.
//
// Returns: A string slice of the values corresponding to the fields in the same order the fields were provided.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HMGet(key string, fields ...string) ([]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand(append([]string{"HMGET", key}, fields...)),
		nil,
		false,
		true,
	)
	if err != nil {
		return nil, err
	}

	return internal.ParseStringArrayResponse(b)
}

// HStrLen returns the length of the values held at the specified fields of a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `fields` - ...string - the list of fields to whose values lengths will be checked.
//
// Returns: and integer slice representing the lengths of the strings at the corresponding fields index.
// Non-existent fields will have length 0. If the key does not exist, an empty slice is returned.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HStrLen(key string, fields ...string) ([]int, error) {
	cmd := append([]string{"HSTRLEN", key}, fields...)

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	return internal.ParseIntegerArrayResponse(b)
}

// HVals returns all the values in a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// Returns: a string slice with all the values of the hash map. If the key does not exist, an empty slice is returned.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HVals(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HVALS", key}), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// HRandField returns a random list of fields from the hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `options` - HRandFieldOptions
//
// Returns: a string slice containing random fields of the hash map. If the key does not exist, an empty slice is returned.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HRandField(key string, options HRandFieldOptions) ([]string, error) {
	cmd := []string{"HRANDFIELD", key}

	if options.Count == 0 {
		cmd = append(cmd, strconv.Itoa(1))
	} else {
		cmd = append(cmd, strconv.Itoa(int(options.Count)))
	}

	if options.WithValues {
		cmd = append(cmd, "WITHVALUES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	return internal.ParseStringArrayResponse(b)
}

// HLen returns the length of the hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// Returns: an integer representing the length of the hash map. If the key does not exist, 0 is returned.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HLen(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HLEN", key}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// HKeys returns all the keys in a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// Returns: a string slice with all the keys of the hash map. If the key does not exist, an empty slice is returned.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HKeys(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HKEYS", key}), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// HIncrBy increment the value of the hash map at the given field by an integer. If the hash map does not exist,
// a new hash map is created with the field and increment as the value.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `field` - string - the field of the value to increment.
//
// Returns: a float representing the new value of the field.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
//
// "value at field <field> is not a number" - when the field holds a value that is not a number.
func (server *SugarDB) HIncrBy(key, field string, increment int) (float64, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HINCRBY", key, field, strconv.Itoa(increment)}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseFloatResponse(b)
}

// HIncrByFloat behaves like HIncrBy but with a float increment instead of an integer increment.
func (server *SugarDB) HIncrByFloat(key, field string, increment float64) (float64, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HINCRBYFLOAT", key, field, strconv.FormatFloat(increment, 'f', -1, 64)}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseFloatResponse(b)
}

// HGetAll returns a flattened slice of all keys and values in a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// Returns: a flattened string slice where every second element is a value preceded by its corresponding key. If the
// key does not exist, an empty slice is returned.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HGetAll(key string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HGETALL", key}), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// HExists checks if a field exists in a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `field` - string - the field to check.
//
// Returns: a boolean representing whether the field exists in the hash map. Returns 0 if the hash map does not exist.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HExists(key, field string) (bool, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"HEXISTS", key, field}), nil, false, true)
	if err != nil {
		return false, err
	}
	return internal.ParseBooleanResponse(b)
}

// HDel delete 1 or more fields from a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `fields` - ...string - a list of fields to delete.
//
// Returns: an integer representing the number of fields deleted.
//
// Errors:
//
// "value at <key> is not a hash" - when the provided key does not exist or is not a hash.
func (server *SugarDB) HDel(key string, fields ...string) (int, error) {
	cmd := append([]string{"HDEL", key}, fields...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// HExpire sets the expiration for the provided field(s) in a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `seconds` - int - number of seconds until expiration.
//
// `ExOpt` - ExpireOptions - One of NX, XX, GT, LT.
//
// `fields` - ...string - a list of fields to set expiration of.
//
// Returns: an integer array representing the outcome of the commmand for each field.
//   - Integer reply: -2 if no such field exists in the provided hash key, or the provided key does not exist.
//   - Integer reply: 0 if the specified NX | XX | GT | LT condition has not been met.
//   - Integer reply: 1 if the expiration time was set/updated.
//   - Integer reply: 2 when HEXPIRE/HPEXPIRE is called with 0 seconds
//
// Errors:
//
// "value of key <key> is not a hash" - when the provided key is not a hash.
func (server *SugarDB) HExpire(key string, seconds int, ExOpt ExpireOptions, fields ...string) ([]int, error) {
	secs := fmt.Sprintf("%v", seconds)
	cmd := []string{"HEXPIRE", key, secs}
	if ExOpt != nil {
		ExpireOption := fmt.Sprintf("%v", ExOpt)
		cmd = append(cmd, ExpireOption)
	}

	numFields := fmt.Sprintf("%v", len(fields))
	fieldsArray := append([]string{"FIELDS", numFields}, fields...)

	cmd = append(cmd, fieldsArray...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseIntegerArrayResponse(b)
}

// HTTL gets the expiration for the provided field(s) in a hash map.
//
// Parameters:
//
// `key` - string - the key to the hash map.
//
// `fields` - ...string - a list of fields to get TTL for.
//
// Returns: an integer array representing the outcome of the commmand for each field.
//   - Integer reply: the TTL in seconds.
//   - Integer reply: -2 if no such field exists in the provided hash key, or the provided key does not exist.
//   - Integer reply: -1 if the field exists but has no associated expiration set.
//
// Errors:
//
// "value of key <key> is not a hash" - when the provided key is not a hash.
func (server *SugarDB) HTTL(key string, fields ...string) ([]int, error) {
	numFields := fmt.Sprintf("%v", len(fields))

	cmd := append([]string{"HTTL", key, "FIELDS", numFields}, fields...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseIntegerArrayResponse(b)
}
