package main

import (
	"context"
	"fmt"
	"strconv"
)

var Command string = "Module.Set"

var Categories []string = []string{"write", "fast"}

var Description string = `(Module.Set key value) This module stores the given value at the specified key.
The value must be an integer`

var Sync bool = true

func KeyExtractionFunc(cmd []string, args ...string) ([]string, []string, error) {
	if len(cmd) != 3 {
		return nil, nil, fmt.Errorf("wrong no of args for %s command", Command)
	}
	return []string{}, cmd[1:2], nil
}

func HandlerFunc(
	ctx context.Context,
	command []string,
	keyExists func(ctx context.Context, key string) bool,
	keyLock func(ctx context.Context, key string) (bool, error),
	keyUnlock func(ctx context.Context, key string),
	keyRLock func(ctx context.Context, key string) (bool, error),
	keyRUnlock func(ctx context.Context, key string),
	createKeyAndLock func(ctx context.Context, key string) (bool, error),
	getValue func(ctx context.Context, key string) interface{},
	setValue func(ctx context.Context, key string, value interface{}) error,
	args ...string) ([]byte, error) {

	_, writeKeys, err := KeyExtractionFunc(command, args...)
	if err != nil {
		return nil, err
	}
	key := writeKeys[0]

	if !keyExists(ctx, key) {
		_, err := createKeyAndLock(ctx, key)
		if err != nil {
			return nil, err
		}
	} else {
		_, err := keyLock(ctx, key)
		if err != nil {
			return nil, err
		}
	}
	defer keyUnlock(ctx, key)

	value, err := strconv.ParseInt(command[2], 10, 64)
	if err != nil {
		return nil, err
	}

	err = setValue(ctx, key, value)
	if err != nil {
		return nil, err
	}

	return []byte("+OK\r\n"), nil
}
