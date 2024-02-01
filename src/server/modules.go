package server

import (
	"context"
	"errors"
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
	"net"
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

func (server *Server) handleCommand(ctx context.Context, message []byte, conn *net.Conn, replay bool) ([]byte, error) {
	cmd, err := utils.Decode(message)
	if err != nil {
		return nil, err
	}

	command, err := server.getCommand(cmd[0])
	if err != nil {
		return nil, err
	}

	synchronize := command.Sync
	handler := command.HandlerFunc

	subCommand, ok := utils.GetSubCommand(command, cmd).(utils.SubCommand)
	if ok {
		synchronize = subCommand.Sync
		handler = subCommand.HandlerFunc
	}

	if conn != nil {
		// Authorize connection if it's provided
		if err = server.ACL.AuthorizeConnection(conn, cmd, command, subCommand); err != nil {
			return nil, err
		}
	}

	// If we're not in cluster mode and command/subcommand is a write command, wait for state copy to finish.
	if utils.IsWriteCommand(command, subCommand) {
		for {
			if !server.StateCopyInProgress.Load() {
				server.StateMutationInProgress.Store(true)
				break
			}
		}
	}

	if !server.IsInCluster() || !synchronize {
		res, err := handler(ctx, cmd, server, conn)
		if err != nil {
			return nil, err
		}

		if utils.IsWriteCommand(command, subCommand) && !replay {
			go server.AOFEngine.QueueCommand(message)
		}

		server.StateMutationInProgress.Store(false)

		return res, err
	}

	// Handle other commands that need to be synced across the cluster
	if server.raft.IsRaftLeader() {
		res, err := server.raftApply(ctx, cmd)
		if err != nil {
			return nil, err
		}
		return res, err
	}

	// Forward message to leader and return immediate OK response
	if server.Config.ForwardCommand {
		server.memberList.ForwardDataMutation(ctx, message)
		return []byte(utils.OK_RESPONSE), nil
	}

	return nil, errors.New("not cluster leader, cannot carry out command")
}
