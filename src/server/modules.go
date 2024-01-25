package server

import (
	"context"
	"fmt"
	"github.com/echovault/echovault/src/modules/acl"
	"github.com/echovault/echovault/src/modules/admin"
	"github.com/echovault/echovault/src/modules/etc"
	"github.com/echovault/echovault/src/modules/get"
	"github.com/echovault/echovault/src/modules/hash"
	"github.com/echovault/echovault/src/modules/list"
	"github.com/echovault/echovault/src/modules/ping"
	"github.com/echovault/echovault/src/modules/pubsub"
	"github.com/echovault/echovault/src/modules/set"
	"github.com/echovault/echovault/src/modules/sorted_set"
	str "github.com/echovault/echovault/src/modules/string"
	"github.com/echovault/echovault/src/utils"
	"strings"
)

func (server *Server) LoadCommands(plugin utils.Plugin) {
	commands := plugin.Commands()
	for _, command := range commands {
		server.commands = append(server.commands, command)
	}
}

func (server *Server) LoadModules(ctx context.Context) {
	server.LoadCommands(admin.NewModule())
	server.LoadCommands(acl.NewModule())
	server.LoadCommands(pubsub.NewModule())
	server.LoadCommands(ping.NewModule())
	server.LoadCommands(get.NewModule())
	server.LoadCommands(list.NewModule())
	server.LoadCommands(str.NewModule())
	server.LoadCommands(etc.NewModule())
	server.LoadCommands(set.NewModule())
	server.LoadCommands(sorted_set.NewModule())
	server.LoadCommands(hash.NewModule())
}

func (server *Server) GetAllCommands(ctx context.Context) []utils.Command {
	return server.commands
}

func (server *Server) GetACL() interface{} {
	return server.ACL
}

func (server *Server) GetPubSub() interface{} {
	return server.PubSub
}

func (server *Server) getCommand(cmd string) (utils.Command, error) {
	for _, command := range server.commands {
		if strings.EqualFold(command.Command, cmd) {
			return command, nil
		}
	}
	return utils.Command{}, fmt.Errorf("command %s not supported", cmd)
}
