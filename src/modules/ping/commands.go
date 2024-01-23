package ping

import (
	"context"
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
)

type Plugin struct {
	name        string
	commands    []utils.Command
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

func handlePing(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	switch len(cmd) {
	default:
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	case 1:
		return []byte("+PONG\r\n\r\n"), nil
	case 2:
		return []byte("+" + cmd[1] + "\r\n\r\n"), nil
	}
}

func NewModule() Plugin {
	PingModule := Plugin{
		name: "PingCommands",
		commands: []utils.Command{
			{
				Command:     "ping",
				Categories:  []string{utils.FastCategory, utils.ConnectionCategory},
				Description: "(PING [value]) Ping the server. If a value is provided, the value will be echoed.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
				HandlerFunc: handlePing,
			},
			{
				Command:     "ack",
				Categories:  []string{},
				Description: "",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return []byte("$-1\r\n\r\n"), nil
				},
			},
		},
		description: "Handle PING command",
	}
	return PingModule
}
