package main

import (
	"context"
	"errors"
	"fmt"
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
	case "get":
		return handleGet(ctx, cmd, server.(Server))
	case "mget":
		return handleMGet(ctx, cmd, server.(Server))
	}
}

func handleGet(ctx context.Context, cmd []string, s Server) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New("wrong number of args for GET command")
	}

	s.KeyRLock(ctx, cmd[1])
	value := s.GetValue(cmd[1])
	s.KeyRUnlock(cmd[1])

	switch value.(type) {
	default:
		return []byte(fmt.Sprintf("+%v\r\n\n", value)), nil
	case nil:
		return []byte("+nil\r\n\n"), nil
	}
}

func handleMGet(ctx context.Context, cmd []string, s Server) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New("wrong number of args for MGET command")
	}

	vals := []string{}

	for _, key := range cmd[1:] {
		s.KeyRLock(ctx, key)
		switch s.GetValue(key).(type) {
		default:
			vals = append(vals, fmt.Sprintf("%v", s.GetValue(key)))
		case nil:
			vals = append(vals, "nil")
		}
		s.KeyRUnlock(key)
	}

	var bytes []byte = []byte(fmt.Sprintf("*%d\r\n", len(vals)))

	for _, val := range vals {
		bytes = append(bytes, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))...)
	}

	bytes = append(bytes, []byte("\n")...)

	return bytes, nil
}

func init() {
	Plugin.name = "GetCommand"
	Plugin.commands = []string{"get", "mget"}
	Plugin.description = "Handle basic GET and MGET commands"
}
