package main

import (
	"context"
	"fmt"
)

var Command string = "Module.Get"

var Categories []string = []string{"read", "fast"}

var Description string = `(Module.Get key) This module fetches the integer value from the key and returns the value ^ 2.
0 is returned if the key does not exist. An error is returned if the value is not an integer.`

var Sync bool = false

func KeyExtractionFunc(cmd []string, args ...string) ([]string, []string, error) {
	if len(cmd) != 2 {
		return nil, nil, fmt.Errorf("wrong no of args for %s command", Command)
	}
	return cmd[1:], []string{}, nil
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

	readKeys, _, err := KeyExtractionFunc(command, args...)
	if err != nil {
		return nil, err
	}
	key := readKeys[0]

	if !keyExists(ctx, key) {
		return []byte(":0\r\n"), nil
	}

	_, err = keyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer keyRUnlock(ctx, key)

	val, ok := getValue(ctx, key).(int64)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not an integer", key)
	}

	return []byte(fmt.Sprintf(":%d\r\n", val*val)), nil
}
