package get

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"strings"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	categories  []string
	description string
}

var GetModule Plugin

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
	case "get":
		return handleGet(ctx, cmd, server)
	case "mget":
		return handleMGet(ctx, cmd, server)
	}
}

func handleGet(ctx context.Context, cmd []string, s utils.Server) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New("wrong number of args for GET command")
	}

	key := cmd[1]

	s.KeyRLock(ctx, key)
	value := s.GetValue(key)
	s.KeyRUnlock(key)

	switch value.(type) {
	default:
		return []byte(fmt.Sprintf("+%v\r\n\n", value)), nil
	case nil:
		return []byte("+nil\r\n\n"), nil
	}
}

func handleMGet(ctx context.Context, cmd []string, s utils.Server) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New("wrong number of args for MGET command")
	}

	vals := []string{}

	for _, key := range cmd[1:] {
		func(key string) {
			s.KeyRLock(ctx, key)
			switch s.GetValue(key).(type) {
			default:
				vals = append(vals, fmt.Sprintf("%v", s.GetValue(key)))
			case nil:
				vals = append(vals, "nil")
			}
			s.KeyRUnlock(key)

		}(key)
	}

	bytes := []byte(fmt.Sprintf("*%d\r\n", len(vals)))

	for _, val := range vals {
		bytes = append(bytes, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))...)
	}

	bytes = append(bytes, []byte("\n")...)

	return bytes, nil
}

func NewModule() Plugin {
	GetModule := Plugin{
		name: "GetCommands",
		commands: []utils.Command{
			{
				Command:     "get",
				Categories:  []string{},
				Description: "",
				Sync:        false,
			},
			{
				Command:     "mget",
				Categories:  []string{},
				Description: "",
				Sync:        true,
			},
		},
		description: "Handle basic GET and MGET commands",
	}
	return GetModule
}
