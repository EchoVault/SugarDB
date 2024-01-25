package admin

import (
	"context"
	"github.com/echovault/echovault/src/utils"
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

func NewModule() Plugin {
	return Plugin{
		name:        "AdminCommands",
		description: "Handle admin/server management commands",
		commands: []utils.Command{
			{
				Command:     "bgsave",
				Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
				Description: "(BGSAVE) Trigger a snapshot save",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return []byte(utils.OK_RESPONSE), nil
				},
			},
		},
	}
}
