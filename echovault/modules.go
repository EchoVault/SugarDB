// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package echovault

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/constants"
	"net"
	"strings"
)

func (server *EchoVault) getCommand(cmd string) (internal.Command, error) {
	server.commandsRWMut.RLock()
	defer server.commandsRWMut.RUnlock()
	for _, command := range server.commands {
		if strings.EqualFold(command.Command, cmd) {
			return command, nil
		}
	}
	return internal.Command{}, fmt.Errorf("command %s not supported", cmd)
}

func (server *EchoVault) getHandlerFuncParams(ctx context.Context, cmd []string, conn *net.Conn) internal.HandlerFuncParams {
	return internal.HandlerFuncParams{
		Context:               ctx,
		Command:               cmd,
		Connection:            conn,
		KeyExists:             server.KeyExists,
		CreateKeyAndLock:      server.CreateKeyAndLock,
		KeyLock:               server.KeyLock,
		KeyRLock:              server.KeyRLock,
		KeyUnlock:             server.KeyUnlock,
		KeyRUnlock:            server.KeyRUnlock,
		GetValue:              server.GetValue,
		SetValue:              server.SetValue,
		GetExpiry:             server.GetExpiry,
		SetExpiry:             server.SetExpiry,
		DeleteKey:             server.DeleteKey,
		TakeSnapshot:          server.takeSnapshot,
		GetLatestSnapshotTime: server.getLatestSnapshotTime,
		RewriteAOF:            server.rewriteAOF,
		LoadModule:            server.LoadModule,
		UnloadModule:          server.UnloadModule,
		ListModules:           server.ListModules,
		GetClock:              server.getClock,
		GetPubSub:             server.getPubSub,
		GetACL:                server.getACL,
		GetAllCommands:        server.getCommands,
	}
}

func (server *EchoVault) handleCommand(ctx context.Context, message []byte, conn *net.Conn, replay bool, embedded bool) ([]byte, error) {
	cmd, err := internal.Decode(message)
	if err != nil {
		return nil, err
	}

	command, err := server.getCommand(cmd[0])
	if err != nil {
		return nil, err
	}

	synchronize := command.Sync
	handler := command.HandlerFunc

	sc, err := internal.GetSubCommand(command, cmd)
	if err != nil {
		return nil, err
	}
	subCommand, ok := sc.(internal.SubCommand)
	if ok {
		synchronize = subCommand.Sync
		handler = subCommand.HandlerFunc
	}

	if conn != nil && server.acl != nil && !embedded {
		// Authorize connection if it's provided and if ACL module is present
		// and the embedded parameter is false.
		if err = server.acl.AuthorizeConnection(conn, cmd, command, subCommand); err != nil {
			return nil, err
		}
	}

	// If the command is a write command, wait for state copy to finish.
	if internal.IsWriteCommand(command, subCommand) {
		for {
			if !server.stateCopyInProgress.Load() {
				server.stateMutationInProgress.Store(true)
				break
			}
		}
	}

	if !server.isInCluster() || !synchronize {
		res, err := handler(server.getHandlerFuncParams(ctx, cmd, conn))
		if err != nil {
			return nil, err
		}

		if internal.IsWriteCommand(command, subCommand) && !replay {
			go server.aofEngine.QueueCommand(message)
		}

		server.stateMutationInProgress.Store(false)

		return res, err
	}

	// Handle other commands that need to be synced across the cluster
	if server.raft.IsRaftLeader() {
		// fmt.Println("WE ARE HERE")
		var res []byte
		res, err = server.raftApplyCommand(ctx, cmd)
		if err != nil {
			return nil, err
		}
		return res, err
	}

	// Forward message to leader and return immediate OK response
	if server.config.ForwardCommand {
		server.memberList.ForwardDataMutation(ctx, message)
		return []byte(constants.OkResponse), nil
	}

	return nil, errors.New("not cluster leader, cannot carry out command")
}
