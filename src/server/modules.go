package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
	"strings"
)

func (server *Server) GetAllCommands(ctx context.Context) []utils.Command {
	return server.Commands
}

func (server *Server) GetACL() interface{} {
	return server.ACL
}

func (server *Server) GetPubSub() interface{} {
	return server.PubSub
}

func (server *Server) getCommand(cmd string) (utils.Command, error) {
	for _, command := range server.Commands {
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
		return []byte(utils.OkResponse), nil
	}

	return nil, errors.New("not cluster leader, cannot carry out command")
}
