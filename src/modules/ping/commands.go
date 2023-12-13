package ping

import (
	"context"
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"strings"
)

const (
	OK = "+OK\r\n\n"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	description string
}

var PingModule Plugin

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
		return nil, errors.New("not implemented")
	case "ping":
		return handlePing(ctx, cmd, server)
	case "ack":
		return []byte("$-1\r\n\n"), nil
	}
}

func handlePing(ctx context.Context, cmd []string, s utils.Server) ([]byte, error) {
	switch len(cmd) {
	default:
		return nil, errors.New("wrong number of arguments for PING command")
	case 1:
		return []byte("+PONG\r\n\n"), nil
	case 2:
		return []byte("+" + cmd[1] + "\r\n\n"), nil
	}
}

func NewModule() Plugin {
	PingModule := Plugin{
		name: "PingCommands",
		commands: []utils.Command{
			{
				Command:     "ping",
				Categories:  []string{},
				Description: "(PING [value]) Ping the server. If a value is provided, the value will be echoed.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "ack",
				Categories:  []string{},
				Description: "",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
		},
		description: "Handle PING command",
	}
	return PingModule
}
