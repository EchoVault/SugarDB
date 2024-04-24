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

package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/types"
	"github.com/hashicorp/raft"
	"io"
	"log"
	"strings"
)

type FSMOpts struct {
	Config                config.Config
	EchoVault             types.EchoVault
	GetState              func() map[string]internal.KeyData
	GetCommand            func(command string) (types.Command, error)
	DeleteKey             func(ctx context.Context, key string) error
	StartSnapshot         func()
	FinishSnapshot        func()
	SetLatestSnapshotTime func(msec int64)
}

type FSM struct {
	options FSMOpts
}

func NewFSM(opts FSMOpts) raft.FSM {
	return raft.FSM(&FSM{
		options: opts,
	})
}

// Apply Implements raft.FSM interface
func (fsm *FSM) Apply(log *raft.Log) interface{} {
	switch log.Type {
	default:
		// No-Op
	case raft.LogCommand:
		var request internal.ApplyRequest

		if err := json.Unmarshal(log.Data, &request); err != nil {
			return internal.ApplyResponse{
				Error:    err,
				Response: nil,
			}
		}

		ctx := context.WithValue(context.Background(), internal.ContextServerID("ServerID"), request.ServerID)
		ctx = context.WithValue(ctx, internal.ContextConnID("ConnectionID"), request.ConnectionID)

		switch strings.ToLower(request.Type) {
		default:
			return internal.ApplyResponse{
				Error:    fmt.Errorf("unsupported raft command type %s", request.Type),
				Response: nil,
			}

		case "delete-key":
			if err := fsm.options.DeleteKey(ctx, request.Key); err != nil {
				return internal.ApplyResponse{
					Error:    err,
					Response: nil,
				}
			}
			return internal.ApplyResponse{
				Error:    nil,
				Response: []byte("OK"),
			}

		case "command":
			// TODO: Re-Implement Command handling with dependency injection
			// Handle command
			// command, err := fsm.options.GetCommand(request.CMD[0])
			// if err != nil {
			// 	return internal.ApplyResponse{
			// 		Error:    err,
			// 		Response: nil,
			// 	}
			// }
			//
			// handler := command.HandlerFunc
			//
			// subCommand, ok := internal.GetSubCommand(command, request.CMD).(types.SubCommand)
			// if ok {
			// 	handler = subCommand.HandlerFunc
			// }
			//
			// if res, err := handler(ctx, request.CMD, fsm.options.EchoVault, nil); err != nil {
			// 	return internal.ApplyResponse{
			// 		Error:    err,
			// 		Response: nil,
			// 	}
			// } else {
			// 	return internal.ApplyResponse{
			// 		Error:    nil,
			// 		Response: res,
			// 	}
			// }
		}
	}

	return nil
}

// Snapshot implements raft.FSM interface
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return NewFSMSnapshot(SnapshotOpts{
		config:                fsm.options.Config,
		startSnapshot:         fsm.options.StartSnapshot,
		finishSnapshot:        fsm.options.FinishSnapshot,
		setLatestSnapshotTime: fsm.options.SetLatestSnapshotTime,
		data:                  fsm.options.GetState(),
	}), nil
}

// Restore implements raft.FSM interface
func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	b, err := io.ReadAll(snapshot)

	if err != nil {
		log.Fatal(err)
		return err
	}

	data := internal.SnapshotObject{
		State:                      make(map[string]internal.KeyData),
		LatestSnapshotMilliseconds: 0,
	}

	if err = json.Unmarshal(b, &data); err != nil {
		log.Fatal(err)
		return err
	}

	// Set state
	ctx := context.Background()
	for k, v := range internal.FilterExpiredKeys(data.State) {
		if _, err = fsm.options.EchoVault.CreateKeyAndLock(ctx, k); err != nil {
			log.Fatal(err)
		}
		if err = fsm.options.EchoVault.SetValue(ctx, k, v.Value); err != nil {
			log.Fatal(err)
		}
		fsm.options.EchoVault.SetExpiry(ctx, k, v.ExpireAt, false)
		fsm.options.EchoVault.KeyUnlock(ctx, k)
	}
	// Set latest snapshot milliseconds
	fsm.options.SetLatestSnapshotTime(data.LatestSnapshotMilliseconds)

	return nil
}
