package main

import (
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"strings"
)

type Server interface {
	KeyLock(key string)
	KeyUnlock(key string)
	KeyRLock(key string)
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

func (p *plugin) HandleCommand(cmd []string, server interface{}) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "set":
		return handleSet(cmd, server.(Server))
	}
}

func handleSet(cmd []string, s Server) ([]byte, error) {
	switch x := len(cmd); {
	default:
		return nil, errors.New("wrong number of args for SET command")
	case x == 3:
		if s.KeyExists(cmd[1]) {
			s.KeyLock(cmd[1])
			s.SetValue(cmd[1], utils.AdaptType(cmd[2]))
			s.KeyUnlock(cmd[1])
		} else {
			s.CreateKey(cmd[1], utils.AdaptType(cmd[2]))
		}
		return []byte("+OK\r\n\n"), nil
	}
}

func init() {
	Plugin.name = "SetCommand"
	Plugin.commands = []string{"set"}
	Plugin.description = "Handle basic SET commands"
}
