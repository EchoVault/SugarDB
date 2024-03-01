package etc

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

type KeyObject struct {
	value  interface{}
	locked bool
}

func handleSet(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := setKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	if !server.KeyExists(key) {
		_, err := server.CreateKeyAndLock(ctx, key)
		if err != nil {
			return nil, err
		}
		server.SetValue(ctx, key, utils.AdaptType(cmd[2]))
		server.KeyUnlock(key)
		return []byte(utils.OkResponse), nil
	}

	if _, err := server.KeyLock(ctx, key); err != nil {
		return nil, err
	}

	server.SetValue(ctx, key, utils.AdaptType(cmd[2]))
	server.KeyUnlock(key)

	return []byte(utils.OkResponse), nil
}

func handleSetNX(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := setNXKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	if server.KeyExists(key) {
		return nil, fmt.Errorf("key %s already exists", key)
	}
	if _, err = server.CreateKeyAndLock(ctx, key); err != nil {
		return nil, err
	}
	server.SetValue(ctx, key, utils.AdaptType(cmd[2]))
	server.KeyUnlock(key)

	return []byte(utils.OkResponse), nil
}

func handleMSet(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
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
		server.SetValue(ctx, k, v.value)
	}

	return []byte(utils.OkResponse), nil
}

func handleCopy(ctx context.Context, cmd []string, server *utils.Server, _ *net.Conn) ([]byte, error) {
	return nil, errors.New("command not yet implemented")
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:           "set",
			Categories:        []string{utils.WriteCategory, utils.SlowCategory},
			Description:       "(SET key value) Set the value of a key, considering the value's type.",
			Sync:              true,
			KeyExtractionFunc: setKeyFunc,
			HandlerFunc:       handleSet,
		},
		{
			Command:           "setnx",
			Categories:        []string{utils.WriteCategory, utils.SlowCategory},
			Description:       "(SETNX key value) Set the key/value only if the key doesn't exist.",
			Sync:              true,
			KeyExtractionFunc: setNXKeyFunc,
			HandlerFunc:       handleSetNX,
		},
		{
			Command:           "mset",
			Categories:        []string{utils.WriteCategory, utils.SlowCategory},
			Description:       "(MSET key value [key value ...]) Automatically etc or modify multiple key/value pairs.",
			Sync:              true,
			KeyExtractionFunc: msetKeyFunc,
			HandlerFunc:       handleMSet,
		},
	}
}
