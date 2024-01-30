package get

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	categories  []string
	description string
}

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() []utils.Command {
	return p.commands
}

func (p Plugin) Description() string {
	return p.description
}

func handleGet(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte("+nil\r\n\r\n"), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	value := server.GetValue(key)
	server.KeyRUnlock(key)

	switch value.(type) {
	default:
		return []byte(fmt.Sprintf("+%v\r\n\r\n", value)), nil
	case nil:
		return []byte("+nil\r\n\r\n"), nil
	}
}

func handleMGet(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	vals := []string{}

	for _, key := range cmd[1:] {
		func(key string) {
			if !server.KeyExists(key) {
				vals = append(vals, "nil")
				return
			}
			server.KeyRLock(ctx, key)
			switch server.GetValue(key).(type) {
			default:
				vals = append(vals, fmt.Sprintf("%v", server.GetValue(key)))
			case nil:
				vals = append(vals, "nil")
			}
			server.KeyRUnlock(key)

		}(key)
	}

	bytes := []byte(fmt.Sprintf("*%d\r\n", len(vals)))

	for _, val := range vals {
		bytes = append(bytes, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))...)
	}

	bytes = append(bytes, []byte("\r\n")...)

	return bytes, nil
}

func NewModule() Plugin {
	GetModule := Plugin{
		name: "GetCommands",
		commands: []utils.Command{
			{
				Command:     "get",
				Categories:  []string{utils.ReadCategory, utils.FastCategory},
				Description: "(GET key) Get the value at the specified key.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleGet,
			},
			{
				Command:     "mget",
				Categories:  []string{utils.ReadCategory, utils.FastCategory},
				Description: "(MGET key1 [key2]) Get multiple values from the specified keys.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleMGet,
			},
		},
		description: "Handle basic GET and MGET commands",
	}
	return GetModule
}
