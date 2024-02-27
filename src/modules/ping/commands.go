package ping

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

func handlePing(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	switch len(cmd) {
	default:
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	case 1:
		return []byte("+PONG\r\n"), nil
	case 2:
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd[1]), cmd[1])), nil
	}
}

func Commands() []utils.Command {
	return []utils.Command{
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
				return []byte("$-1\r\n"), nil
			},
		},
	}
}
