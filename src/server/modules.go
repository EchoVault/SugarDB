package server

import (
	"context"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/modules/acl"
	"github.com/kelvinmwinuka/memstore/src/modules/etc"
	"github.com/kelvinmwinuka/memstore/src/modules/get"
	"github.com/kelvinmwinuka/memstore/src/modules/hash"
	"github.com/kelvinmwinuka/memstore/src/modules/list"
	"github.com/kelvinmwinuka/memstore/src/modules/ping"
	"github.com/kelvinmwinuka/memstore/src/modules/pubsub"
	"github.com/kelvinmwinuka/memstore/src/modules/set"
	"github.com/kelvinmwinuka/memstore/src/modules/sorted_set"
	str "github.com/kelvinmwinuka/memstore/src/modules/string"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"strings"
)

func (server *Server) LoadCommands(plugin utils.Plugin) {
	commands := plugin.Commands()
	for _, command := range commands {
		server.commands = append(server.commands, command)
	}
}

func (server *Server) LoadModules(ctx context.Context) {
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
