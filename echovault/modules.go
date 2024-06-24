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
	"github.com/echovault/echovault/internal/clock"
	"github.com/echovault/echovault/internal/constants"
	"github.com/echovault/echovault/internal/eviction"
	"io"
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
		KeysExist:             server.keysExist,
		GetExpiry:             server.getExpiry,
		GetValues:             server.getValues,
		SetValues:             server.setValues,
		SetExpiry:             server.setExpiry,
		TakeSnapshot:          server.takeSnapshot,
		GetLatestSnapshotTime: server.getLatestSnapshotTime,
		RewriteAOF:            server.rewriteAOF,
		LoadModule:            server.LoadModule,
		UnloadModule:          server.UnloadModule,
		ListModules:           server.ListModules,
		GetPubSub:             server.getPubSub,
		GetACL:                server.getACL,
		GetAllCommands:        server.getCommands,
		GetClock:              server.getClock,
		DeleteKey: func(ctx context.Context, key string) error {
			server.storeLock.Lock()
			defer server.storeLock.Unlock()
			return server.deleteKey(ctx, key)
		},
		GetConnectionInfo: func(conn *net.Conn) internal.ConnectionInfo {
			server.connInfo.mut.RLock()
			defer server.connInfo.mut.RUnlock()
			return server.connInfo.tcpClients[conn]
		},
		SetConnectionInfo: func(conn *net.Conn, clientname string, protocol int, database int) {
			server.connInfo.mut.Lock()
			defer server.connInfo.mut.Unlock()

			info := server.connInfo.tcpClients[conn]

			// Set protocol.
			info.Protocol = protocol

			// Set connection name.
			if clientname != "" {
				info.Name = clientname
			}

			// If the database index does not exist, create the new database.
			server.storeLock.Lock()
			if server.store[database] == nil {
				// Database does not exist.
				server.store[database] = make(map[string]internal.KeyData)
				// Create volatile key tracker for the database.
				server.keysWithExpiry.rwMutex.Lock()
				server.keysWithExpiry.keys[database] = make([]string, 0)
				server.keysWithExpiry.rwMutex.Unlock()
				// Create LFU cache for the database.
				server.lfuCache.mutex.Lock()
				server.lfuCache.cache[database] = eviction.NewCacheLFU()
				server.lfuCache.mutex.Unlock()
				// Create LRU cache for the database.
				server.lruCache.mutex.Lock()
				server.lruCache.cache[database] = eviction.NewCacheLRU()
				server.lruCache.mutex.Unlock()
			}
			server.storeLock.Unlock()

			// Set database index for the current connection.
			info.Database = database

			server.connInfo.tcpClients[conn] = info
		},
		GetServerInfo: func() internal.ServerInfo {
			return internal.ServerInfo{
				Server:  "echovault",
				Version: constants.Version,
				Id:      server.config.ServerID,
				Mode: func() string {
					if server.isInCluster() {
						return "cluster"
					}
					return "standalone"
				}(),
				Role: func() string {
					if !server.isInCluster() {
						return "master"
					}
					if server.raft.IsRaftLeader() {
						return "master"
					}
					return "replica"
				}(),
				Modules: server.ListModules(),
			}
		},
	}
}

func (server *EchoVault) handleCommand(ctx context.Context, message []byte, conn *net.Conn, replay bool, embedded bool) ([]byte, error) {
	// Prepare context before processing the command.
	server.connInfo.mut.RLock()
	if embedded {
		// The call is triggered via the embedded API.
		// Add embedded connection info to the context of the request.
		ctx = context.WithValue(ctx, "ConnectionName", server.connInfo.embedded.Name)
		ctx = context.WithValue(ctx, "Protocol", server.connInfo.embedded.Protocol)
		ctx = context.WithValue(ctx, "Database", server.connInfo.embedded.Database)
	} else {
		// The call is triggered by a TCP connection.
		// Add TCP connection info to the context of the request.
		ctx = context.WithValue(ctx, "ConnectionName", server.connInfo.tcpClients[conn].Name)
		ctx = context.WithValue(ctx, "Protocol", server.connInfo.tcpClients[conn].Protocol)
		ctx = context.WithValue(ctx, "Database", server.connInfo.tcpClients[conn].Database)
	}
	server.connInfo.mut.RUnlock()

	cmd, err := internal.Decode(message)
	if err != nil {
		return nil, err
	}

	if len(cmd) == 0 {
		return nil, errors.New("empty command")
	}

	// If quit command is passed, EOF error.
	if strings.EqualFold(cmd[0], "quit") {
		return nil, io.EOF
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
			// TODO: Enable this when AOF engine has support for multiple databases.
			// go server.aofEngine.QueueCommand(message)
		}

		server.stateMutationInProgress.Store(false)

		return res, err
	}

	// Handle other commands that need to be synced across the cluster
	if server.raft.IsRaftLeader() {
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

func (server *EchoVault) getCommands() []internal.Command {
	return server.commands
}

func (server *EchoVault) getACL() interface{} {
	return server.acl
}

func (server *EchoVault) getPubSub() interface{} {
	return server.pubSub
}

func (server *EchoVault) getClock() clock.Clock {
	return server.clock
}
