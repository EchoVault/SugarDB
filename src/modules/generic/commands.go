package generic

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"log"
	"net"
	"strings"
	"time"
)

type KeyObject struct {
	value  interface{}
	locked bool
}

func handleSet(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	keys, err := setKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	value := cmd[2]
	res := []byte(utils.OkResponse)

	params, err := getSetCommandParams(cmd[3:], SetParams{})
	if err != nil {
		return nil, err
	}

	// If GET is provided, the response should be the current stored value.
	// If there's no current value, then the response should be nil.
	if params.get {
		if !server.KeyExists(key) {
			res = []byte("$-1\r\n")
		} else {
			res = []byte(fmt.Sprintf("+%v\r\n", server.GetValue(ctx, key)))
		}
	}

	if "xx" == strings.ToLower(params.exists) {
		// If XX is specified, make sure the key exists.
		if !server.KeyExists(key) {
			return nil, fmt.Errorf("key %s does not exist", key)
		}
		_, err = server.KeyLock(ctx, key)
	} else if "nx" == strings.ToLower(params.exists) {
		// If NX is specified, make sure that the key does not currently exist.
		if server.KeyExists(key) {
			return nil, fmt.Errorf("key %s already exists", key)
		}
		_, err = server.CreateKeyAndLock(ctx, key)
	} else {
		// Neither XX not NX are specified, lock or create the lock
		if !server.KeyExists(key) {
			// Key does not exist, create it
			_, err = server.CreateKeyAndLock(ctx, key)
		} else {
			// Key exists, acquire the lock
			_, err = server.KeyLock(ctx, key)
		}
	}
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	if err = server.SetValue(ctx, key, utils.AdaptType(value)); err != nil {
		return nil, err
	}

	// If expiresAt is set, set the key's expiry time as well
	if params.expireAt != nil {
		server.SetKeyExpiry(ctx, key, params.expireAt.(time.Time), false)
	}

	return res, nil
}

func handleMSet(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	if _, err := msetKeyFunc(cmd); err != nil {
		return nil, err
	}

	entries := make(map[string]KeyObject)

	// Release all acquired key locks
	defer func() {
		for k, v := range entries {
			if v.locked {
				server.KeyUnlock(k)
				entries[k] = KeyObject{
					value:  v.value,
					locked: false,
				}
			}
		}
	}()

	// Extract all the key/value pairs
	for i, key := range cmd[1:] {
		if i%2 == 0 {
			entries[key] = KeyObject{
				value:  utils.AdaptType(cmd[1:][i+1]),
				locked: false,
			}
		}
	}

	// Acquire all the locks for each key first
	// If any key cannot be acquired, abandon transaction and release all currently held keys
	for k, v := range entries {
		if server.KeyExists(k) {
			if _, err := server.KeyLock(ctx, k); err != nil {
				return nil, err
			}
			entries[k] = KeyObject{value: v.value, locked: true}
			continue
		}
		if _, err := server.CreateKeyAndLock(ctx, k); err != nil {
			return nil, err
		}
		entries[k] = KeyObject{value: v.value, locked: true}
	}

	// Set all the values
	for k, v := range entries {
		if err := server.SetValue(ctx, k, v.value); err != nil {
			return nil, err
		}
	}

	return []byte(utils.OkResponse), nil
}

func handleGet(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	keys, err := getKeyFunc(cmd)
	if err != nil {
		return nil, err
	}
	key := keys[0]

	if !server.KeyExists(key) {
		return []byte("$-1\r\n"), nil
	}

	_, err = server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	value := server.GetValue(ctx, key)

	return []byte(fmt.Sprintf("+%v\r\n", value)), nil
}

func handleMGet(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	keys, err := mgetKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)

	locks := make(map[string]bool)
	for _, key := range keys {
		if _, ok := values[key]; ok {
			// Skip if we have already locked this key
			continue
		}
		if server.KeyExists(key) {
			_, err = server.KeyRLock(ctx, key)
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
				server.KeyRUnlock(key)
				locks[key] = false
			}
		}
	}()

	for key, _ := range locks {
		values[key] = fmt.Sprintf("%v", server.GetValue(ctx, key))
	}

	bytes := []byte(fmt.Sprintf("*%d\r\n", len(cmd[1:])))

	for _, key := range cmd[1:] {
		if values[key] == "" {
			bytes = append(bytes, []byte("$-1\r\n")...)
			continue
		}
		bytes = append(bytes, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(values[key]), values[key]))...)
	}

	return bytes, nil
}

func handleDel(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	keys, err := delKeyFunc(cmd)
	if err != nil {
		return nil, err
	}
	count := 0
	for _, key := range keys {
		err = server.DeleteKey(ctx, key)
		if err != nil {
			log.Printf("could not delete key %s due to error: %+v\n", key, err)
			continue
		}
		count += 1
	}
	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handlePersist(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	return nil, errors.New("command not implemented yet")
}

func handleExpireTime(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	// Handle EXPIRETIME and PEXPIRETIME
	return nil, errors.New("command not implemented yet")
}

func handleTTL(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	// Handle TTL and PTTL
	return nil, errors.New("command not implemented yet")
}

func handleExpire(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	// Handle EXPIRE and PEXPIRE
	return nil, errors.New("command not implemented yet")
}

func handleExpireAt(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	// Handle EXPIREAT and PEXPIREAT
	return nil, errors.New("command not implemented yet")
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:    "set",
			Categories: []string{utils.WriteCategory, utils.SlowCategory},
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
			Categories:        []string{utils.WriteCategory, utils.SlowCategory},
			Description:       "(MSET key value [key value ...]) Automatically generic or modify multiple key/value pairs.",
			Sync:              true,
			KeyExtractionFunc: msetKeyFunc,
			HandlerFunc:       handleMSet,
		},
		{
			Command:           "get",
			Categories:        []string{utils.ReadCategory, utils.FastCategory},
			Description:       "(GET key) Get the value at the specified key.",
			Sync:              false,
			KeyExtractionFunc: getKeyFunc,
			HandlerFunc:       handleGet,
		},
		{
			Command:           "mget",
			Categories:        []string{utils.ReadCategory, utils.FastCategory},
			Description:       "(MGET key1 [key2]) Get multiple values from the specified keys.",
			Sync:              false,
			KeyExtractionFunc: mgetKeyFunc,
			HandlerFunc:       handleMGet,
		},
		{
			Command:           "del",
			Categories:        []string{utils.KeyspaceCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(DEL) Removes one or more keys from the store.",
			Sync:              true,
			KeyExtractionFunc: delKeyFunc,
			HandlerFunc:       handleDel,
		},
		{
			Command:    "persist",
			Categories: []string{utils.KeyspaceCategory, utils.WriteCategory, utils.FastCategory},
			Description: `(PERSIST key) Removes the TTl associated with a key, 
turning it from a volatile key to a persistent key.`,
			Sync:              true,
			KeyExtractionFunc: persistKeyFunc,
			HandlerFunc:       handlePersist,
		},
		{
			Command:    "expiretime",
			Categories: []string{utils.KeyspaceCategory, utils.ReadCategory, utils.FastCategory},
			Description: `(EXPIRETIME key) Returns the absolute unix time in seconds when the key will expire.
Return -1 if the key exists but has no associated expiry time.
Returns -2 if the key does not exist.`,
			Sync:              true,
			KeyExtractionFunc: expireTimeKeyFunc,
			HandlerFunc:       handleExpireTime,
		},
		{
			Command:    "pexpiretime",
			Categories: []string{utils.KeyspaceCategory, utils.ReadCategory, utils.FastCategory},
			Description: `(PEXPIRETIME key) Returns the absolute unix time in milliseconds when the key will expire.
Return -1 if the key exists but has no associated expiry time.
Returns -2 if the key does not exist.`,
			Sync:              true,
			KeyExtractionFunc: expireTimeKeyFunc,
			HandlerFunc:       handleExpireTime,
		},
		{
			Command:    "ttl",
			Categories: []string{utils.KeyspaceCategory, utils.ReadCategory, utils.FastCategory},
			Description: `(TTL key) Returns the remaining time to live for a key that has an expiry time in seconds.
If the key exists but does not have an associated expiry time, -1 is returned.
If the key does not exist, -2 is returned.`,
			Sync:              true,
			KeyExtractionFunc: ttlKeyFunc,
			HandlerFunc:       handleTTL,
		},
		{
			Command:    "pttl",
			Categories: []string{utils.KeyspaceCategory, utils.ReadCategory, utils.FastCategory},
			Description: `(PTTL key) Returns the remaining time to live for a key that has an expiry time in milliseconds.
If the key exists but does not have an associated expiry time, -1 is returned.
If the key does not exist, -2 is returned.`,
			Sync:              true,
			KeyExtractionFunc: ttlKeyFunc,
			HandlerFunc:       handleTTL,
		},
		{
			Command:    "expire",
			Categories: []string{utils.KeyspaceCategory, utils.WriteCategory, utils.FastCategory},
			Description: `(EXPIRE key seconds [NX | XX | GT | LT])
Expire the key in the specified number of seconds. This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the current expiry time is greater than the specified expiry time.
LT - Only set the expiry time if the current expiry time is less than the specified expiry time.`,
			Sync:              true,
			KeyExtractionFunc: expireKeyFunc,
			HandlerFunc:       handleExpire,
		},
		{
			Command:    "pexpire",
			Categories: []string{utils.KeyspaceCategory, utils.WriteCategory, utils.FastCategory},
			Description: `(PEXPIRE key milliseconds [NX | XX | GT | LT])
Expire the key in the specified number of milliseconds. This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the current expiry time is greater than the specified expiry time.
LT - Only set the expiry time if the current expiry time is less than the specified expiry time.`,
			Sync:              true,
			KeyExtractionFunc: expireKeyFunc,
			HandlerFunc:       handleExpire,
		},
		{
			Command:    "expireat",
			Categories: []string{utils.KeyspaceCategory, utils.WriteCategory, utils.FastCategory},
			Description: `(EXPIREAT key unix-time-seconds [NX | XX | GT | LT])
Expire the key in at the exact unix time in seconds. 
This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the current expiry time is greater than the specified expiry time.
LT - Only set the expiry time if the current expiry time is less than the specified expiry time.`,
			Sync:              true,
			KeyExtractionFunc: expireAtKeyFunc,
			HandlerFunc:       handleExpireAt,
		},
		{
			Command:    "pexpireat",
			Categories: []string{utils.KeyspaceCategory, utils.WriteCategory, utils.FastCategory},
			Description: `(PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT])
Expire the key in at the exact unix time in milliseconds. 
This commands turns a key into a volatile one.
NX - Only set the expiry time if the key has no associated expiry.
XX - Only set the expiry time if the key already has an expiry time.
GT - Only set the expiry time if the current expiry time is greater than the specified expiry time.
LT - Only set the expiry time if the current expiry time is less than the specified expiry time.`,
			Sync:              true,
			KeyExtractionFunc: expireAtKeyFunc,
			HandlerFunc:       handleExpireAt,
		},
	}
}
