package admin

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

func handleGetAllCommands(ctx context.Context, cmd []string, server utils.Server, _ *net.Conn) ([]byte, error) {
	commands := server.GetAllCommands(ctx)

	res := ""
	commandCount := 0

	for _, c := range commands {
		if c.SubCommands == nil || len(c.SubCommands) <= 0 {
			res += "*6\r\n"
			// Command name
			res += fmt.Sprintf("+command\r\n*1\r\n$%d\r\n%s\r\n", len(c.Command), c.Command)
			// Command categories
			res += fmt.Sprintf("+categories\r\n*%d\r\n", len(c.Categories))
			for _, category := range c.Categories {
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(category), category)
			}
			// Description
			res += fmt.Sprintf("+description\r\n*1\r\n$%d\r\n%s\r\n", len(c.Description), c.Description)

			commandCount += 1
			continue
		}
		// There are sub-commands
		for _, sc := range c.SubCommands {
			res += "*6\r\n"
			// Command name
			command := fmt.Sprintf("%s %s", c.Command, sc.Command)
			res += fmt.Sprintf("+command\r\n*1\r\n$%d\r\n%s\r\n", len(command), command)
			// Command categories
			res += fmt.Sprintf("+categories\r\n*%d\r\n", len(sc.Categories))
			for _, category := range sc.Categories {
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(category), category)
			}
			// Description
			res += fmt.Sprintf("+description\r\n*1\r\n$%d\r\n%s\r\n", len(sc.Description), sc.Description)

			commandCount += 1
		}
	}

	res = fmt.Sprintf("*%d\r\n%s\r\n", commandCount, res)

	return []byte(res), nil
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:           "commands",
			Categories:        []string{utils.AdminCategory, utils.SlowCategory},
			Description:       "Get a list of all the commands in available on the server with categories and descriptions",
			Sync:              false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
			HandlerFunc:       handleGetAllCommands,
		},
		{
			Command:     "save",
			Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
			Description: "(SAVE) Trigger a snapshot save",
			Sync:        true,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
				if err := server.TakeSnapshot(); err != nil {
					return nil, err
				}
				return []byte(utils.OK_RESPONSE), nil
			},
		},
		{
			Command:     "lastsave",
			Categories:  []string{utils.AdminCategory, utils.FastCategory, utils.DangerousCategory},
			Description: "(LASTSAVE) Get unix timestamp for the latest snapshot in milliseconds.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
				msec := server.GetLatestSnapshot()
				if msec == 0 {
					return nil, errors.New("no snapshot")
				}
				return []byte(fmt.Sprintf(":%d\r\n\r\n", msec)), nil
			},
		},
		{
			Command:     "rewriteaof",
			Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
			Description: "(REWRITEAOF) Trigger re-writing of append process",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
				if err := server.RewriteAOF(); err != nil {
					return nil, err
				}
				return []byte(utils.OK_RESPONSE), nil
			},
		},
	}
}
