package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"github.com/hashicorp/raft"
	"io"
	"log"
	"strings"
)

type FSMOpts struct {
	Config     utils.Config
	Server     utils.Server
	GetCommand func(command string) (utils.Command, error)
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
		var request utils.ApplyRequest

		if err := json.Unmarshal(log.Data, &request); err != nil {
			return utils.ApplyResponse{
				Error:    err,
				Response: nil,
			}
		}

		ctx := context.WithValue(context.Background(), utils.ContextServerID("ServerID"), request.ServerID)
		ctx = context.WithValue(ctx, utils.ContextConnID("ConnectionID"), request.ConnectionID)

		switch strings.ToLower(request.Type) {
		default:
			return utils.ApplyResponse{
				Error:    fmt.Errorf("unsupported raft command type %s", request.Type),
				Response: nil,
			}

		case "delete-key":
			// TODO: Handle key deletion

		case "command":
			// Handle command
			command, err := fsm.options.GetCommand(request.CMD[0])
			if err != nil {
				return utils.ApplyResponse{
					Error:    err,
					Response: nil,
				}
			}

			handler := command.HandlerFunc

			subCommand, ok := utils.GetSubCommand(command, request.CMD).(utils.SubCommand)
			if ok {
				handler = subCommand.HandlerFunc
			}

			if res, err := handler(ctx, request.CMD, fsm.options.Server, nil); err != nil {
				return utils.ApplyResponse{
					Error:    err,
					Response: nil,
				}
			} else {
				return utils.ApplyResponse{
					Error:    nil,
					Response: res,
				}
			}
		}
	}

	return nil
}

// Snapshot implements raft.FSM interface
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return NewFSMSnapshot(SnapshotOpts{
		config:            fsm.options.Config,
		data:              fsm.options.Server.GetState(),
		startSnapshot:     fsm.options.Server.StartSnapshot,
		finishSnapshot:    fsm.options.Server.FinishSnapshot,
		setLatestSnapshot: fsm.options.Server.SetLatestSnapshot,
	}), nil
}

// Restore implements raft.FSM interface
func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	b, err := io.ReadAll(snapshot)

	if err != nil {
		log.Fatal(err)
		return err
	}

	data := utils.SnapshotObject{
		State:                      make(map[string]interface{}),
		LatestSnapshotMilliseconds: 0,
	}

	if err = json.Unmarshal(b, &data); err != nil {
		log.Fatal(err)
		return err
	}

	// Set state
	ctx := context.Background()
	for k, v := range data.State {
		if _, err = fsm.options.Server.CreateKeyAndLock(ctx, k); err != nil {
			log.Fatal(err)
		}
		if err = fsm.options.Server.SetValue(ctx, k, v); err != nil {
			log.Fatal(err)
		}
		fsm.options.Server.KeyUnlock(ctx, k)
	}
	// Set latest snapshot milliseconds
	fsm.options.Server.SetLatestSnapshot(data.LatestSnapshotMilliseconds)

	return nil
}
