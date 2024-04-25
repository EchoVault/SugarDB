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

package generic

import (
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/pkg/constants"
	"log"
	"strconv"
	"strings"
	"time"
)

type KeyObject struct {
	value  interface{}
	locked bool
}

func handleSet(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := setKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	value := params.Command[2]
	res := []byte(constants.OkResponse)
	clock := params.GetClock()

	options, err := getSetCommandOptions(clock, params.Command[3:], SetOptions{})
	if err != nil {
		return nil, err
	}

	// If GET is provided, the response should be the current stored value.
	// If there's no current value, then the response should be nil.
	if options.get {
		if !params.KeyExists(params.Context, key) {
			res = []byte("$-1\r\n")
		} else {
			res = []byte(fmt.Sprintf("+%v\r\n", params.GetValue(params.Context, key)))
		}
	}

	if "xx" == strings.ToLower(options.exists) {
		// If XX is specified, make sure the key exists.
		if !params.KeyExists(params.Context, key) {
			return nil, fmt.Errorf("key %s does not exist", key)
		}
		_, err = params.KeyLock(params.Context, key)
	} else if "nx" == strings.ToLower(options.exists) {
		// If NX is specified, make sure that the key does not currently exist.
		if params.KeyExists(params.Context, key) {
			return nil, fmt.Errorf("key %s already exists", key)
		}
		_, err = params.CreateKeyAndLock(params.Context, key)
	} else {
		// Neither XX not NX are specified, lock or create the lock
		if !params.KeyExists(params.Context, key) {
			// Key does not exist, create it
			_, err = params.CreateKeyAndLock(params.Context, key)
		} else {
			// Key exists, acquire the lock
			_, err = params.KeyLock(params.Context, key)
		}
	}
	if err != nil {
		return nil, err
	}
	defer params.KeyUnlock(params.Context, key)

	if err = params.SetValue(params.Context, key, internal.AdaptType(value)); err != nil {
		return nil, err
	}

	// If expiresAt is set, set the key's expiry time as well
	if options.expireAt != nil {
		params.SetExpiry(params.Context, key, options.expireAt.(time.Time), false)
	}

	return res, nil
}

func handleMSet(params internal.HandlerFuncParams) ([]byte, error) {
	_, err := msetKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	entries := make(map[string]KeyObject)

	// Release all acquired key locks
	defer func() {
		for k, v := range entries {
			if v.locked {
				params.KeyUnlock(params.Context, k)
				entries[k] = KeyObject{
					value:  v.value,
					locked: false,
				}
			}
		}
	}()

	// Extract all the key/value pairs
	for i, key := range params.Command[1:] {
		if i%2 == 0 {
			entries[key] = KeyObject{
				value:  internal.AdaptType(params.Command[1:][i+1]),
				locked: false,
			}
		}
	}

	// Acquire all the locks for each key first
	// If any key cannot be acquired, abandon transaction and release all currently held keys
	for k, v := range entries {
		if params.KeyExists(params.Context, k) {
			if _, err := params.KeyLock(params.Context, k); err != nil {
				return nil, err
			}
			entries[k] = KeyObject{value: v.value, locked: true}
			continue
		}
		if _, err := params.CreateKeyAndLock(params.Context, k); err != nil {
			return nil, err
		}
		entries[k] = KeyObject{value: v.value, locked: true}
	}

	// Set all the values
	for k, v := range entries {
		if err := params.SetValue(params.Context, k, v.value); err != nil {
			return nil, err
		}
	}

	return []byte(constants.OkResponse), nil
}

func handleGet(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := getKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}
	key := keys.ReadKeys[0]

	if !params.KeyExists(params.Context, key) {
		return []byte("$-1\r\n"), nil
	}

	_, err = params.KeyRLock(params.Context, key)
	if err != nil {
		return nil, err
	}
	defer params.KeyRUnlock(params.Context, key)

	value := params.GetValue(params.Context, key)

	return []byte(fmt.Sprintf("+%v\r\n", value)), nil
}

func handleMGet(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := mgetKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)

	locks := make(map[string]bool)
	for _, key := range keys.ReadKeys {
		if _, ok := values[key]; ok {
			// Skip if we have already locked this key
			continue
		}
		if params.KeyExists(params.Context, key) {
			_, err = params.KeyRLock(params.Context, key)
			if err != nil {
				return nil, fmt.Errorf("could not obtain lock for %s key", key)
			}
			locks[key] = true
			continue
		}
		values[key] = ""
	}
	defer func() {
		for key, locked := range locks {
			if locked {
				params.KeyRUnlock(params.Context, key)
				locks[key] = false
			}
		}
	}()

	for key, _ := range locks {
		values[key] = fmt.Sprintf("%v", params.GetValue(params.Context, key))
	}

	bytes := []byte(fmt.Sprintf("*%d\r\n", len(params.Command[1:])))

	for _, key := range params.Command[1:] {
		if values[key] == "" {
			bytes = append(bytes, []byte("$-1\r\n")...)
			continue
		}
		bytes = append(bytes, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(values[key]), values[key]))...)
	}

	return bytes, nil
}

func handleDel(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := delKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}
	count := 0
	for _, key := range keys.WriteKeys {
		err = params.DeleteKey(params.Context, key)
		if err != nil {
			log.Printf("could not delete key %s due to error: %+v\n", key, err)
			continue
		}
		count += 1
	}
	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handlePersist(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := persistKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]

	if !params.KeyExists(params.Context, key) {
		return []byte(":0\r\n"), nil
	}

	if _, err = params.KeyLock(params.Context, key); err != nil {
		return nil, err
	}
	defer params.KeyUnlock(params.Context, key)

	expireAt := params.GetExpiry(params.Context, key)
	if expireAt == (time.Time{}) {
		return []byte(":0\r\n"), nil
	}

	params.SetExpiry(params.Context, key, time.Time{}, false)

	return []byte(":1\r\n"), nil
}

func handleExpireTime(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := expireTimeKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]

	if !params.KeyExists(params.Context, key) {
		return []byte(":-2\r\n"), nil
	}

	if _, err = params.KeyRLock(params.Context, key); err != nil {
		return nil, err
	}
	defer params.KeyRUnlock(params.Context, key)

	expireAt := params.GetExpiry(params.Context, key)

	if expireAt == (time.Time{}) {
		return []byte(":-1\r\n"), nil
	}

	t := expireAt.Unix()
	if strings.ToLower(params.Command[0]) == "pexpiretime" {
		t = expireAt.UnixMilli()
	}

	return []byte(fmt.Sprintf(":%d\r\n", t)), nil
}

func handleTTL(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := ttlKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]

	clock := params.GetClock()

	if !params.KeyExists(params.Context, key) {
		return []byte(":-2\r\n"), nil
	}

	if _, err = params.KeyRLock(params.Context, key); err != nil {
		return nil, err
	}
	defer params.KeyRUnlock(params.Context, key)

	expireAt := params.GetExpiry(params.Context, key)

	if expireAt == (time.Time{}) {
		return []byte(":-1\r\n"), nil
	}

	t := expireAt.Unix() - clock.Now().Unix()
	if strings.ToLower(params.Command[0]) == "pttl" {
		t = expireAt.UnixMilli() - clock.Now().UnixMilli()
	}

	if t <= 0 {
		return []byte(":0\r\n"), nil
	}

	return []byte(fmt.Sprintf(":%d\r\n", t)), nil
}

func handleExpire(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := expireKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]

	// Extract time
	n, err := strconv.ParseInt(params.Command[2], 10, 64)
	if err != nil {
		return nil, errors.New("expire time must be integer")
	}
	expireAt := params.GetClock().Now().Add(time.Duration(n) * time.Second)
	if strings.ToLower(params.Command[0]) == "pexpire" {
		expireAt = params.GetClock().Now().Add(time.Duration(n) * time.Millisecond)
	}

	if !params.KeyExists(params.Context, key) {
		return []byte(":0\r\n"), nil
	}

	if _, err = params.KeyLock(params.Context, key); err != nil {
		return nil, err
	}
	defer params.KeyUnlock(params.Context, key)

	if len(params.Command) == 3 {
		params.SetExpiry(params.Context, key, expireAt, true)
		return []byte(":1\r\n"), nil
	}

	currentExpireAt := params.GetExpiry(params.Context, key)

	switch strings.ToLower(params.Command[3]) {
	case "nx":
		if currentExpireAt != (time.Time{}) {
			return []byte(":0\r\n"), nil
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	case "xx":
		if currentExpireAt == (time.Time{}) {
			return []byte(":0\r\n"), nil
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	case "gt":
		if currentExpireAt == (time.Time{}) {
			return []byte(":0\r\n"), nil
		}
		if expireAt.Before(currentExpireAt) {
			return []byte(":0\r\n"), nil
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	case "lt":
		if currentExpireAt != (time.Time{}) {
			if currentExpireAt.Before(expireAt) {
				return []byte(":0\r\n"), nil
			}
			params.SetExpiry(params.Context, key, expireAt, false)
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	default:
		return nil, fmt.Errorf("unknown option %s", strings.ToUpper(params.Command[3]))
	}

	return []byte(":1\r\n"), nil
}

func handleExpireAt(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := expireKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]

	// Extract time
	n, err := strconv.ParseInt(params.Command[2], 10, 64)
	if err != nil {
		return nil, errors.New("expire time must be integer")
	}
	expireAt := time.Unix(n, 0)
	if strings.ToLower(params.Command[0]) == "pexpireat" {
		expireAt = time.UnixMilli(n)
	}

	if !params.KeyExists(params.Context, key) {
		return []byte(":0\r\n"), nil
	}

	if _, err = params.KeyLock(params.Context, key); err != nil {
		return nil, err
	}
	defer params.KeyUnlock(params.Context, key)

	if len(params.Command) == 3 {
		params.SetExpiry(params.Context, key, expireAt, true)
		return []byte(":1\r\n"), nil
	}

	currentExpireAt := params.GetExpiry(params.Context, key)

	switch strings.ToLower(params.Command[3]) {
	case "nx":
		if currentExpireAt != (time.Time{}) {
			return []byte(":0\r\n"), nil
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	case "xx":
		if currentExpireAt == (time.Time{}) {
			return []byte(":0\r\n"), nil
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	case "gt":
		if currentExpireAt == (time.Time{}) {
			return []byte(":0\r\n"), nil
		}
		if expireAt.Before(currentExpireAt) {
			return []byte(":0\r\n"), nil
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	case "lt":
		if currentExpireAt != (time.Time{}) {
			if currentExpireAt.Before(expireAt) {
				return []byte(":0\r\n"), nil
			}
			params.SetExpiry(params.Context, key, expireAt, false)
		}
		params.SetExpiry(params.Context, key, expireAt, false)
	default:
		return nil, fmt.Errorf("unknown option %s", strings.ToUpper(params.Command[3]))
	}

	return []byte(":1\r\n"), nil
}

func Commands() []internal.Command {
	return []internal.Command{
		{
			Command:    "set",
			Module:     constants.GenericModule,
			Categories: []string{constants.WriteCategory, constants.SlowCategory},
			Description: `
(SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds]) 
Set the value of a key, considering the value's type.
NX - Only set if the key does not exist.
XX - Only set if the key exists.
GET - Return the old value stored at key, or nil if the value does not exist.
EX - Expire the key after the specified number of seconds (positive integer).
PX - Expire the key after the specified number of milliseconds (positive integer).
EXAT - Expire at the exact time in unix seconds (positive integer).
PXAT - Expire at the exat time in unix milliseconds (positive integer).`,
			Sync:              true,
			KeyExtractionFunc: setKeyFunc,
			HandlerFunc:       handleSet,
		},
		{
			Command:           "mset",
			Module:            constants.GenericModule,
			Categories:        []string{constants.WriteCategory, constants.SlowCategory},
			Description:       "(MSET key value [key value ...]) Automatically generic or modify multiple key/value pairs.",
			Sync:              true,
			KeyExtractionFunc: msetKeyFunc,
			HandlerFunc:       handleMSet,
		},
		{
			Command:           "get",
			Module:            constants.GenericModule,
			Categories:        []string{constants.ReadCategory, constants.FastCategory},
			Description:       "(GET key) Get the value at the specified key.",
			Sync:              false,
			KeyExtractionFunc: getKeyFunc,
			HandlerFunc:       handleGet,
		},
		{
			Command:           "mget",
			Module:            constants.GenericModule,
			Categories:        []string{constants.ReadCategory, constants.FastCategory},
			Description:       "(MGET key [key ...]) Get multiple values from the specified keys.",
			Sync:              false,
			KeyExtractionFunc: mgetKeyFunc,
			HandlerFunc:       handleMGet,
		},
		{
			Command:           "del",
			Module:            constants.GenericModule,
			Categories:        []string{constants.KeyspaceCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(DEL key [key ...]) Removes one or more keys from the store.",
			Sync:              true,
			KeyExtractionFunc: delKeyFunc,
			HandlerFunc:       handleDel,
		},
		{
			Command:    "persist",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(PERSIST key) Removes the TTl associated with a key, 
turning it from a volatile key to a persistent key.`,
			Sync:              true,
			KeyExtractionFunc: persistKeyFunc,
			HandlerFunc:       handlePersist,
		},
		{
			Command:    "expiretime",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.ReadCategory, constants.FastCategory},
			Description: `(EXPIRETIME key) Returns the absolute unix time in seconds when the key will expire.
Return -1 if the key exists but has no associated expiry time.
Returns -2 if the key does not exist.`,
			Sync:              false,
			KeyExtractionFunc: expireTimeKeyFunc,
			HandlerFunc:       handleExpireTime,
		},
		{
			Command:    "pexpiretime",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.ReadCategory, constants.FastCategory},
			Description: `(PEXPIRETIME key) Returns the absolute unix time in milliseconds when the key will expire.
Return -1 if the key exists but has no associated expiry time.
Returns -2 if the key does not exist.`,
			Sync:              false,
			KeyExtractionFunc: expireTimeKeyFunc,
			HandlerFunc:       handleExpireTime,
		},
		{
			Command:    "ttl",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.ReadCategory, constants.FastCategory},
			Description: `(TTL key) Returns the remaining time to live for a key that has an expiry time in seconds.
If the key exists but does not have an associated expiry time, -1 is returned.
If the key does not exist, -2 is returned.`,
			Sync:              false,
			KeyExtractionFunc: ttlKeyFunc,
			HandlerFunc:       handleTTL,
		},
		{
			Command:    "pttl",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.ReadCategory, constants.FastCategory},
			Description: `(PTTL key) Returns the remaining time to live for a key that has an expiry time in milliseconds.
If the key exists but does not have an associated expiry time, -1 is returned.
If the key does not exist, -2 is returned.`,
			Sync:              false,
			KeyExtractionFunc: ttlKeyFunc,
			HandlerFunc:       handleTTL,
		},
		{
			Command:    "expire",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(EXPIRE key seconds [NX | XX | GT | LT])
Expire the key in the specified number of seconds. This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the new expiry time is greater than the current one.
LT - Only set the expiry time if the new expiry time is less than the current one.`,
			Sync:              true,
			KeyExtractionFunc: expireKeyFunc,
			HandlerFunc:       handleExpire,
		},
		{
			Command:    "pexpire",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(PEXPIRE key milliseconds [NX | XX | GT | LT])
Expire the key in the specified number of milliseconds. This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the new expiry time is greater than the current one.
LT - Only set the expiry time if the new expiry time is less than the current one.`,
			Sync:              true,
			KeyExtractionFunc: expireKeyFunc,
			HandlerFunc:       handleExpire,
		},
		{
			Command:    "expireat",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(EXPIREAT key unix-time-seconds [NX | XX | GT | LT])
Expire the key in at the exact unix time in seconds. 
This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the new expiry time is greater than the current one.
LT - Only set the expiry time if the new expiry time is less than the current one.`,
			Sync:              true,
			KeyExtractionFunc: expireAtKeyFunc,
			HandlerFunc:       handleExpireAt,
		},
		{
			Command:    "pexpireat",
			Module:     constants.GenericModule,
			Categories: []string{constants.KeyspaceCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT])
Expire the key in at the exact unix time in milliseconds. 
This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the new expiry time is greater than the current one.
LT - Only set the expiry time if the new expiry time is less than the current one.`,
			Sync:              true,
			KeyExtractionFunc: expireAtKeyFunc,
			HandlerFunc:       handleExpireAt,
		},
	}
}
