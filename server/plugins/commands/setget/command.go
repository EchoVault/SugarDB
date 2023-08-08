package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kelvinmwinuka/memstore/server/utils"
)

type Server interface {
	Lock()
	Unlock()
	GetData(key string) interface{}
	SetData(key string, value interface{})
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
	case "get":
		return handleGet(cmd, server.(Server))
	case "set":
		return handleSet(cmd, server.(Server))
	case "mget":
		return handleMGet(cmd, server.(Server))
	}
}

func handleGet(cmd []string, s Server) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New("wrong number of args for GET command")
	}

	s.Lock()
	value := s.GetData(cmd[1])
	s.Unlock()

	switch value.(type) {
	default:
		return []byte(fmt.Sprintf("+%v\r\n\n", value)), nil
	case nil:
		return []byte("+nil\r\n\n"), nil
	}
}

func handleMGet(cmd []string, s Server) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New("wrong number of args for MGET command")
	}

	vals := []string{}

	s.Lock()

	for _, key := range cmd[1:] {
		switch s.GetData(key).(type) {
		default:
			vals = append(vals, fmt.Sprintf("%v", s.GetData(key)))
		case nil:
			vals = append(vals, "nil")
		}
	}

	s.Unlock()

	var bytes []byte = []byte(fmt.Sprintf("*%d\r\n", len(vals)))

	for _, val := range vals {
		bytes = append(bytes, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))...)
	}

	bytes = append(bytes, []byte("\n")...)

	return bytes, nil
}

func handleSet(cmd []string, s Server) ([]byte, error) {
	switch x := len(cmd); {
	default:
		return nil, errors.New("wrong number of args for SET command")
	case x == 3:
		s.Lock()
		s.SetData(cmd[1], utils.AdaptType(cmd[2]))
		s.Unlock()
		return []byte("+OK\r\n\n"), nil
	}
}

func init() {
	Plugin.name = "GetCommand"
	Plugin.commands = []string{"set", "get", "mget"}
	Plugin.description = "Handle basic SET, GET and MGET commands"
}
