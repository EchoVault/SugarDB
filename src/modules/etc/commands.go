package etc

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"strings"
	"time"
)

type KeyObject struct {
	value  interface{}
	locked bool
}

type Plugin struct {
	name        string
	commands    []utils.Command
	description string
}

var SetModule Plugin

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() []utils.Command {
	return p.commands
}

func (p Plugin) Description() string {
	return p.description
}

func (p Plugin) HandleCommand(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "set":
		return handleSet(ctx, cmd, server)
	case "setnx":
		return handleSetNX(ctx, cmd, server)
	case "mset":
		return handleMSet(ctx, cmd, server)
	}
}

func handleSet(ctx context.Context, cmd []string, s utils.Server) ([]byte, error) {
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
			s.SetValue(ctx, key, utils.AdaptType(cmd[2]))
			s.KeyUnlock(key)
			return []byte("+OK\r\n\n"), nil
		}

		if _, err := s.KeyLock(ctx, key); err != nil {
			return nil, err
		}

		s.SetValue(ctx, key, utils.AdaptType(cmd[2]))
		s.KeyUnlock(key)
		return []byte("+OK\r\n\n"), nil
	}
}

func handleSetNX(ctx context.Context, cmd []string, s utils.Server) ([]byte, error) {
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
		s.SetValue(ctx, key, utils.AdaptType(cmd[2]))
		s.KeyUnlock(key)
	}
	return []byte("+OK\r\n\n"), nil
}

func handleMSet(ctx context.Context, cmd []string, s utils.Server) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	// Check if key/value pairs are complete
	if len(cmd[1:])%2 != 0 {
		return nil, errors.New("each key must have a matching value")
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

func NewModule() Plugin {
	SetModule := Plugin{
		name: "SetCommands",
		commands: []utils.Command{
			{
				Command:     "set",
				Categories:  []string{},
				Description: "(SET key value) Set the value of a key, considering the value's type.",
				Sync:        true,
			},
			{
				Command:     "setnx",
				Categories:  []string{},
				Description: "(SETNX key value) Set the key/value only if the key doesn't exist.",
				Sync:        true,
			},
			{
				Command:     "mset",
				Categories:  []string{},
				Description: "(MSET key value [key value ...]) Automatically etc or modify multiple key/value pairs.",
				Sync:        true,
			},
		},
		description: "Handle basic SET commands",
	}

	return SetModule
}
