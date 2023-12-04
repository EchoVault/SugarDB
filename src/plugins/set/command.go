package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type Server interface {
	KeyLock(ctx context.Context, key string) (bool, error)
	KeyUnlock(key string)
	KeyRLock(ctx context.Context, key string) (bool, error)
	KeyRUnlock(key string)
	KeyExists(key string) bool
	CreateKeyAndLock(ctx context.Context, key string) (bool, error)
	GetValue(key string) interface{}
	SetValue(ctx context.Context, key string, value interface{})
}

type plugin struct {
	name        string
	commands    []string
	description string
}

var Plugin plugin

func (p *plugin) Name() string {
	return p.name
}

func (p *plugin) Commands() []string {
	return p.commands
}

func (p *plugin) Description() string {
	return p.description
}

func (p *plugin) HandleCommandWithConnection(ctx context.Context, cmd []string, server interface{}, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (p *plugin) HandleCommand(ctx context.Context, cmd []string, server interface{}) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "set":
		return handleSet(ctx, cmd, server.(Server))
	case "setnx":
		return handleSetNX(ctx, cmd, server.(Server))
	case "mset":
		return handleMSet(ctx, cmd, server.(Server))
	}
}

func handleSet(ctx context.Context, cmd []string, s Server) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch x := len(cmd); {
	default:
		return nil, errors.New("wrong number of args for SET command")
	case x == 3:
		key := cmd[1]

		if !s.KeyExists(key) {
			// TODO: Retry CreateKeyAndLock until we manage to obtain the key
			s.CreateKeyAndLock(ctx, key)
			s.SetValue(ctx, key, AdaptType(cmd[2]))
			s.KeyUnlock(key)
			return []byte("+OK\r\n\n"), nil
		}

		if _, err := s.KeyLock(ctx, key); err != nil {
			return nil, err
		}

		s.SetValue(ctx, key, AdaptType(cmd[2]))
		s.KeyUnlock(key)
		return []byte("+OK\r\n\n"), nil
	}
}

func handleSetNX(ctx context.Context, cmd []string, s Server) ([]byte, error) {
	switch x := len(cmd); {
	default:
		return nil, errors.New("wrong number of args for SETNX command")
	case x == 3:
		key := cmd[1]
		if s.KeyExists(key) {
			return nil, fmt.Errorf("key %s already exists", cmd[1])
		}
		// TODO: Retry CreateKeyAndLock until we manage to obtain the key
		s.CreateKeyAndLock(ctx, key)
		s.SetValue(ctx, key, AdaptType(cmd[2]))
		s.KeyUnlock(key)
	}
	return []byte("+OK\r\n\n"), nil
}

func handleMSet(ctx context.Context, cmd []string, s Server) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	// Check if key, value pairs are complete
	if len(cmd[1:])%2 != 0 {
		return nil, errors.New("each key must have a matching value")
	}

	// Extract all the key, value pairs
	type KeyObject struct {
		value  interface{}
		locked bool
	}

	entries := make(map[string]KeyObject)

	// Release all acquired key locks
	defer func() {
		for k, v := range entries {
			if v.locked {
				s.KeyUnlock(k)
				entries[k] = KeyObject{
					value:  v.value,
					locked: false,
				}
				fmt.Println("UNLOCKED KEY: ", k)
			}
		}
	}()

	for i, key := range cmd[1:] {
		if i%2 == 0 {
			entries[key] = KeyObject{
				value:  AdaptType(cmd[1:][i+1]),
				locked: false,
			}
		}
	}

	// Acquire all the locks for each key first
	// If any key cannot be acquired, abandon transaction and release all currently held keys
	for k, v := range entries {
		if s.KeyExists(k) {
			if _, err := s.KeyLock(ctx, k); err != nil {
				return nil, err
			}
			entries[k] = KeyObject{value: v.value, locked: true}
			continue
		}
		if _, err := s.CreateKeyAndLock(ctx, k); err != nil {
			return nil, err
		}
		entries[k] = KeyObject{value: v.value, locked: true}
	}

	// Set all the values
	for k, v := range entries {
		s.SetValue(ctx, k, v.value)
	}

	return []byte("+OK\r\n\n"), nil
}

func init() {
	Plugin.name = "SetCommand"
	Plugin.commands = []string{
		"set",      // (SET key value) Set the value of a key, considering the value's type.
		"setnx",    // (SETNX key value) Set the key/value only if the key doesn't exist.
		"mset",     // (MSET key value [key value ...]) Automatically set or modify multiple key/value pairs.
		"msetnx",   // (MSETNX key value [key value ...]) Automatically set the values of one or more keys only when all keys don't exist.
		"setrange", // (SETRANGE key offset value) Overwrites part of a string value with another by offset. Creates the key if it doesn't exist.
		"strlen",   // (STRLEN key) Returns length of the key's value if it's a string.
		"substr",   // (SUBSTR key start end) Returns a substring from the string value.
	}
	Plugin.description = "Handle basic SET commands"
}
