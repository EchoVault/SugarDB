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
	"strconv"
	"strings"

	"github.com/echovault/echovault/internal"
)

// SetWriteOption constants
type SetWriteOpt string

const (
	SETNX SetWriteOpt = "NX"
	SETXX SetWriteOpt = "XX"
)

// SetWriteOption modifies the behavior of Set.
//
// SETNX - Only set if the key does not exist. NX is higher priority than XX.
//
// SETXX - Only set if the key exists.
type SetWriteOption interface {
	IsSetWriteOpt() SetWriteOpt
}

func (w SetWriteOpt) IsSetWriteOpt() SetWriteOpt { return w }

// SetExOption constants
type SetExOpt string

const (
	SETEX   SetExOpt = "EX"
	SETPX   SetExOpt = "PX"
	SETEXAT SetExOpt = "EXAT"
	SETPXAT SetExOpt = "PXAT"
)

// SetExOption modifies the behavior of Set.
//
// SETEX - Expire the key after the specified number of seconds (positive integer).
//
// SETPX - Expire the key after the specified number of milliseconds (positive integer).
//
// SETEXAT - Expire at the exact time in unix seconds (positive integer).
//
// SETPXAT - Expire at the exat time in unix milliseconds (positive integer).
type SetExOption interface {
	IsSetExOpt() SetExOpt
}

func (x SetExOpt) IsSetExOpt() SetExOpt { return x }

// ExpireOptions constants
type ExOpt string

const (
	NX ExOpt = "NX"
	XX ExOpt = "XX"
	LT ExOpt = "LT"
	GT ExOpt = "GT"
)

// ExpireOptions modifies the behavior of Expire, PExpire, ExpireAt, PExpireAt.
//
// NX - Only set the expiry time if the key has no associated expiry.
//
// XX - Only set the expiry time if the key already has an expiry time.
//
// GT - Only set the expiry time if the new expiry time is greater than the current one.
//
// LT - Only set the expiry time if the new expiry time is less than the current one.
//
// NX, GT, and LT are mutually exclusive. XX can additionally be passed in with either GT or LT.
type ExpireOptions interface {
	IsExOpt() ExOpt
}

func (x ExOpt) IsExOpt() ExOpt { return x }

// GetExOption constants
type GetExOpt string

const (
	EX      GetExOpt = "EX"
	PX      GetExOpt = "PX"
	EXAT    GetExOpt = "EXAT"
	PXAT    GetExOpt = "PXAT"
	PERSIST GetExOpt = "PERSIST"
)

// GetExOption modifies the behavior of GetEx.
//
// EX - Set the specified expire time, in seconds.
//
// PX - Set the specified expire time, in milliseconds.
//
// EXAT - Set the specified Unix time at which the key will expire, in seconds.
//
// PXAT - Set the specified Unix time at which the key will expire, in milliseconds.
//
// PERSIST - Remove the time to live associated with the key.
type GetExOption interface {
	isGetExOpt() GetExOpt
}

func (x GetExOpt) isGetExOpt() GetExOpt { return x }

// Set creates or modifies the value at the given key.
//
// Parameters:
//
// `key` - string - the key to create or update.
//
// `value` - string - the value to place at the key.
//
// `writeOpt` - SetWriteOption - One of NX or XX.
//
// `exOpt` - SetExOption - One of EX, PX, EXAT, or PXAT.
//
// `exTime` - int - Time in seconds or milliseconds depending on what exOpt was passed.
//
// `GET` - bool - Whether or not to return previous value if there was one.
//
// Returns: true if the set is successful, If the "Get" flag in SetOptions is set to true, the previous value is returned.
//
// Errors:
//
// "key <key> does not exist"" - when the XX flag is set to true and the key does not exist.
//
// "key <key> does already exists" - when the NX flag is set to true and the key already exists.
func (server *EchoVault) Set(key, value string, writeOpt SetWriteOption, exOpt SetExOption, exTime int, GET bool) (string, bool, error) {
	cmd := []string{"SET", key, value}

	if writeOpt != nil {
		cmd = append(cmd, fmt.Sprint(writeOpt))
	}

	if exOpt != nil {
		cmd = append(cmd, []string{fmt.Sprint(exOpt), strconv.Itoa(exTime)}...)
	}

	if GET {
		cmd = append(cmd, "GET")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return "", false, err
	}

	previousValue, err := internal.ParseStringResponse(b)
	if err != nil {
		return "", false, err
	}
	if !GET {
		previousValue = ""
	}

	return previousValue, true, nil
}

// MSet set multiple values at multiple keys with one command. Existing keys are overwritten and non-existent
// keys are created.
//
// Parameters:
//
// `kvPairs` - map[string]string - a map representing all the keys and values to be set.
//
// Returns: true if the set is successful.
//
// Errors:
//
// "key <key> already exists" - when the NX flag is set to true and the key already exists.
func (server *EchoVault) MSet(kvPairs map[string]string) (bool, error) {
	cmd := []string{"MSET"}

	for k, v := range kvPairs {
		cmd = append(cmd, []string{k, v}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return false, err
	}

	s, err := internal.ParseStringResponse(b)
	if err != nil {
		return false, err
	}

	return strings.EqualFold(s, "ok"), nil
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
// `options` - ExpireOptions - One of NX, GT, LT. XX can be passed with GT OR LT optionally.
//
// Returns: true if the key's expiry was successfully updated.
func (server *EchoVault) Expire(key string, seconds int, options ...ExpireOptions) (bool, error) {
	cmd := []string{"EXPIRE", key, strconv.Itoa(seconds)}

	if options != nil {
		for _, opt := range options {
			cmd = append(cmd, fmt.Sprint(opt))
		}
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return false, err
	}

	return internal.ParseBooleanResponse(b)
}

// PExpire set the given key's expiry in milliseconds from now.
// This command turns a persistent key into a volatile one.
//
// Parameters:
//
// `key` - string.
//
// `milliseconds` - int - number of milliseconds from now.
//
// `options` - PExpireOptions
//
// Returns: true if the key's expiry was successfully updated.
func (server *EchoVault) PExpire(key string, milliseconds int, options ...ExpireOptions) (bool, error) {
	cmd := []string{"PEXPIRE", key, strconv.Itoa(milliseconds)}

	if options != nil {
		for _, opt := range options {
			cmd = append(cmd, fmt.Sprint(opt))
		}
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return false, err
	}

	return internal.ParseBooleanResponse(b)
}

// ExpireAt sets the given key's expiry in unix epoch seconds.
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
func (server *EchoVault) ExpireAt(key string, unixSeconds int, options ...ExpireOptions) (int, error) {
	cmd := []string{"EXPIREAT", key, strconv.Itoa(unixSeconds)}

	if options != nil {
		for _, opt := range options {
			cmd = append(cmd, fmt.Sprint(opt))
		}
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
func (server *EchoVault) PExpireAt(key string, unixMilliseconds int, options ...ExpireOptions) (int, error) {
	cmd := []string{"PEXPIREAT", key, strconv.Itoa(unixMilliseconds)}
	if options != nil {
		for _, opt := range options {
			cmd = append(cmd, fmt.Sprint(opt))
		}
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// Incr increments the value at the given key if it's an integer.
// If the key does not exist, it's created with an initial value of 0 before incrementing.
//
// Parameters:
//
// `key` - string
//
// Returns: The new value as an integer.
func (server *EchoVault) Incr(key string) (int, error) {
	// Construct the command
	cmd := []string{"INCR", key}

	// Execute the command
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	// Parse the integer response
	return internal.ParseIntegerResponse(b)
}

// Decr decrements the value at the given key if it's an integer.
// If the key does not exist, it's created with an initial value of 0 before incrementing.
//
// Parameters:
//
// `key` - string
//
// Returns: The new value as an integer.
func (server *EchoVault) Decr(key string) (int, error) {
	// Construct the command
	cmd := []string{"DECR", key}

	// Execute the command
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	// Parse the integer response
	return internal.ParseIntegerResponse(b)
}

// IncrBy increments the integer value of the specified key by the given increment.
// If the key does not exist, it is created with an initial value of 0 before incrementing.
// If the value stored at the key is not an integer, an error is returned.
//
// Parameters:
//
// `key` - string -  The key whose value is to be incremented.
//
// `increment` - int -  The amount by which to increment the key's value. This can be a positive or negative integer.
//
// Returns: The new value of the key after the increment operation as an integer.
func (server *EchoVault) IncrBy(key string, value string) (int, error) {
	// Construct the command
	cmd := []string{"INCRBY", key, value}
	// Execute the command
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	// Parse the integer response
	return internal.ParseIntegerResponse(b)
}

// IncrByFloat increments the floating-point value of the specified key by the given increment.
// If the key does not exist, it is created with an initial value of 0 before incrementing.
// If the value stored at the key is not a float, an error is returned.
//
// Parameters:
//
// `key` - string - The key whose value is to be incremented.
//
// `increment` - float64 - The amount by which to increment the key's value. This can be a positive or negative float.
//
// Returns: The new value of the key after the increment operation as a float64.
func (server *EchoVault) IncrByFloat(key string, value string) (float64, error) {
	// Construct the command
	cmd := []string{"INCRBYFLOAT", key, value}
	// Execute the command
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	// Parse the float response
	return internal.ParseFloatResponse(b)
}

// DecrBy decrements the integer value of the specified key by the given increment.
// If the key does not exist, it is created with an initial value of 0 before decrementing.
// If the value stored at the key is not an integer, an error is returned.
//
// Parameters:
//
// `key` - string - The key whose value is to be decremented.
//
// `increment` - int - The amount by which to decrement the key's value. This can be a positive or negative integer.
//
// Returns: The new value of the key after the decrement operation as an integer.
func (server *EchoVault) DecrBy(key string, value string) (int, error) {
	// Construct the command
	cmd := []string{"DECRBY", key, value}
	// Execute the command
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	// Parse the integer response
	return internal.ParseIntegerResponse(b)
}

// Rename renames the key from oldKey to newKey.
// If the oldKey does not exist, an error is returned.
//
// Parameters:
//
// `oldKey` - string - The key to be renamed.
//
// `newKey` - string - The new name for the key.
//
// Returns: A string indicating the success of the operation.
func (server *EchoVault) Rename(oldKey string, newKey string) (string, error) {
	// Construct the command
	cmd := []string{"RENAME", oldKey, newKey}
	// Execute the command
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return "", err
	}
	// Parse the simple string response
	return internal.ParseStringResponse(b)
}

// RandomKey returns a random key from the current active database.
// If no keys present in db returns an empty string.
func (server *EchoVault) RandomKey() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"RANDOMKEY"}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// GetDel retrieves the value at the provided key and deletes that key.
//
// Parameters:
//
// `key` - string - the key whose value should be retrieved and then deleted.
//
// Returns: A string representing the value at the specified key. If the value does not exist, an empty
// string is returned.
func (server *EchoVault) GetDel(key string) (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"GETDEL", key}), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// GetEx retrieves the value of the provided key and optionally sets its expiration
//
// Parameters:
//
// `key` - string - the key whose value should be retrieved and expiry set.
//
// `option` - GetExOption - one of EX, PX, EXAT, PXAT, PERSIST. Can be nil.
//
// `unixtime` - Number of seconds or miliseconds from now.
//
// Returns: A string representing the value at the specified key. If the value does not exist, an empty string is returned.
func (server *EchoVault) GetEx(key string, option GetExOption, unixtime int) (string, error) {

	cmd := make([]string, 2)

	cmd[0] = "GETEX"
	cmd[1] = key

	if option != nil {
		opt := fmt.Sprint(option)
		cmd = append(cmd, opt)
	}

	if unixtime != 0 {
		cmd = append(cmd, strconv.Itoa(unixtime))
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
