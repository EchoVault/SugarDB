package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"strings"
)

type Server interface {
	KeyLock(ctx context.Context, key string) (bool, error)
	KeyUnlock(key string)
	KeyRLock(ctx context.Context, key string) (bool, error)
	KeyRUnlock(key string)
	KeyExists(key string) bool
	CreateKey(key string, value interface{})
	GetValue(key string) interface{}
	SetValue(key string, value interface{})
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
		if s.KeyExists(cmd[1]) {
			s.KeyLock(ctx, cmd[1])
			s.SetValue(cmd[1], utils.AdaptType(cmd[2]))
			s.KeyUnlock(cmd[1])
		} else {
			s.CreateKey(cmd[1], utils.AdaptType(cmd[2]))
		}
		return []byte("+OK\r\n\n"), nil
	}
}

func handleSetNX(ctx context.Context, cmd []string, s Server) ([]byte, error) {
	switch x := len(cmd); {
	default:
		return nil, errors.New("wrong number of args for SETNX command")
	case x == 3:
		if s.KeyExists(cmd[1]) {
			return nil, fmt.Errorf("key %s already exists", cmd[1])
		}
		s.CreateKey(cmd[1], utils.AdaptType(cmd[2]))
	}
	return []byte("+OK\r\n\n"), nil
}

func handleMSet(ctx context.Context, cmd []string, s Server) ([]byte, error) {
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
