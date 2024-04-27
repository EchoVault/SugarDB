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

// SetOptions modifies the behaviour for the Set command
//
// NX - Only set if the key does not exist. NX is higher priority than XX.
//
// XX - Only set if the key exists.
//
// GET - Return the old value stored at key, or nil if the value does not exist.
//
// EX - Expire the key after the specified number of seconds (positive integer).
// EX has the highest priority
//
// PX - Expire the key after the specified number of milliseconds (positive integer).
// PX has the second-highest priority.
//
// EXAT - Expire at the exact time in unix seconds (positive integer).
// EXAT has the third-highest priority.
//
// PXAT - Expire at the exat time in unix milliseconds (positive integer).
// PXAT has the least priority.
type SetOptions struct {
	NX   bool
	XX   bool
	GET  bool
	EX   int
	PX   int
	EXAT int
	PXAT int
}

// ExpireOptions modifies the behaviour of the Expire, PExpire, ExpireAt, PExpireAt.
//
// NX - Only set the expiry time if the key has no associated expiry.
//
// XX - Only set the expiry time if the key already has an expiry time.
//
// GT - Only set the expiry time if the new expiry time is greater than the current one.
//
// LT - Only set the expiry time if the new expiry time is less than the current one.
type ExpireOptions struct {
	NX bool
	XX bool
	LT bool
	GT bool
}
type PExpireOptions ExpireOptions
type ExpireAtOptions ExpireOptions
type PExpireAtOptions ExpireOptions

// Set creates or modifies the value at the given key.
//
// Parameters:
//
// `key` - string - the key to create or update.
//
// `value` - string - the value to place at the key.
//
// `options` - SetOptions.
//
// Returns: "OK" if the set is successful, If the "Get" flag in SetOptions is set to true, the previous value is returned.
//
// Errors:
//
// "key <key> does not exist"" - when the XX flag is set to true and the key does not exist.
//
// "key <key> does already exists" - when the NX flag is set to true and the key already exists.
func (server *EchoVault) Set(key, value string, options SetOptions) (string, error) {
	cmd := []string{"SET", key, value}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	}

	switch {
	case options.EX != 0:
		cmd = append(cmd, []string{"EX", strconv.Itoa(options.EX)}...)
	case options.PX != 0:
		cmd = append(cmd, []string{"PX", strconv.Itoa(options.PX)}...)
	case options.EXAT != 0:
		cmd = append(cmd, []string{"EXAT", strconv.Itoa(options.EXAT)}...)
	case options.PXAT != 0:
		cmd = append(cmd, []string{"PXAT", strconv.Itoa(options.PXAT)}...)
	}

	if options.GET {
		cmd = append(cmd, "GET")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

// MSet set multiple values at multiple keys with one command. Existing keys are overwritten and non-existent
// keys are created.
//
// Parameters:
//
// `kvPairs` - map[string]string - a map representing all the keys and values to be set.
//
// Returns: "OK" if the set is successful.
//
// Errors:
//
// "key <key> does already exists" - when the NX flag is set to true and the key already exists.
func (server *EchoVault) MSet(kvPairs map[string]string) (string, error) {
	cmd := []string{"MSET"}

	for k, v := range kvPairs {
		cmd = append(cmd, []string{k, v}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

// Get retrieves the value at the provided key.
//
// Parameters:
//
// `key` - string - the key whose value should be retrieved.
//
// Returns: A string representing the value at the specified key. If the value does not exist, an empty
// string is returned.
func (server *EchoVault) Get(key string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"GET", key}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// MGet get multiple values from the list of provided keys. The index of each value corresponds to the index of its key
// in the parameter slice. Values that do not exist will be an empty string.
//
// Parameters:
//
// `keys` - []string - a string slice of all the keys.
//
// Returns: a string slice of all the values.
func (server *EchoVault) MGet(keys ...string) ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand(append([]string{"MGET"}, keys...)), nil, false, true)
	if err != nil {
		return []string{}, err
	}
	return internal.ParseStringArrayResponse(b)
}

// Del removes the given keys from the store.
//
// Parameters:
//
// `keys` - []string - the keys to delete from the store.
//
// Returns: The number of keys that were successfully deleted.
func (server *EchoVault) Del(keys ...string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand(append([]string{"DEL"}, keys...)), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// Persist removes the expiry associated with a key and makes it permanent.
// Has no effect on a key that is already persistent.
//
// Parameters:
//
// `key` - string - the key to persist.
//
// Returns: true if the keys is successfully persisted.
func (server *EchoVault) Persist(key string) (bool, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PERSIST", key}), nil, false, true)
	if err != nil {
		return false, err
	}
	return internal.ParseBooleanResponse(b)
}

// ExpireTime return the current key's expiry time in unix epoch seconds.
//
// Parameters:
//
// `key` - string.
//
// Returns: -2 if the keys does not exist, -1 if the key exists but has no expiry time, seconds if the key has an expiry.
func (server *EchoVault) ExpireTime(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"EXPIRETIME", key}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// PExpireTime return the current key's expiry time in unix epoch milliseconds.
//
// Parameters:
//
// `key` - string.
//
// Returns: -2 if the keys does not exist, -1 if the key exists but has no expiry time, seconds if the key has an expiry.
func (server *EchoVault) PExpireTime(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PEXPIRETIME", key}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// TTL return the current key's expiry time from now in seconds.
//
// Parameters:
//
// `key` - string.
//
// Returns: -2 if the keys does not exist, -1 if the key exists but has no expiry time, seconds if the key has an expiry.
func (server *EchoVault) TTL(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"TTL", key}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// PTTL return the current key's expiry time from now in milliseconds.
//
// Parameters:
//
// `key` - string.
//
// Returns: -2 if the keys does not exist, -1 if the key exists but has no expiry time, seconds if the key has an expiry.
func (server *EchoVault) PTTL(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"PTTL", key}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// Expire set the given key's expiry in seconds from now.
// This command turns a persistent key into a volatile one.
//
// Parameters:
//
// `key` - string.
//
// `seconds` - int - number of seconds from now.
//
// `options` - ExpireOptions
//
// Returns: true if the key's expiry was successfully updated.
func (server *EchoVault) Expire(key string, seconds int, options ExpireOptions) (int, error) {
	cmd := []string{"EXPIRE", key, strconv.Itoa(seconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// PExpire set the given key's expiry in milliseconds from now.
// This command turns a persistent key into a volatile one.
//
// Parameters:
//
// `key` - string.
//
// `milliseconds` - int - number of seconds from now.
//
// `options` - PExpireOptions
//
// Returns: true if the key's expiry was successfully updated.
func (server *EchoVault) PExpire(key string, milliseconds int, options PExpireOptions) (int, error) {
	cmd := []string{"PEXPIRE", key, strconv.Itoa(milliseconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// ExpireAt set the given key's expiry in unix epoch seconds.
// This command turns a persistent key into a volatile one.
//
// Parameters:
//
// `key` - string.
//
// `unixSeconds` - int - number of seconds from now.
//
// `options` - ExpireAtOptions
//
// Returns: true if the key's expiry was successfully updated.
func (server *EchoVault) ExpireAt(key string, unixSeconds int, options ExpireAtOptions) (int, error) {
	cmd := []string{"EXPIREAT", key, strconv.Itoa(unixSeconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// PExpireAt set the given key's expiry in unix epoch milliseconds.
// This command turns a persistent key into a volatile one.
//
// Parameters:
//
// `key` - string.
//
// `unixMilliseconds` - int - number of seconds from now.
//
// `options` - PExpireAtOptions
//
// Returns: true if the key's expiry was successfully updated.
func (server *EchoVault) PExpireAt(key string, unixMilliseconds int, options PExpireAtOptions) (int, error) {
	cmd := []string{"PEXPIREAT", key, strconv.Itoa(unixMilliseconds)}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	case options.LT:
		cmd = append(cmd, "LT")
	case options.GT:
		cmd = append(cmd, "GT")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}
