package raft

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"io"
)

type FSMOpts struct {
	Config     utils.Config
	Server     utils.Server
	Snapshot   raft.FSMSnapshot
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

	return nil
}

// Snapshot implements raft.FSM interface
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return fsm.options.Snapshot, nil
}

// Restore implements raft.FSM interface
func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	b, err := io.ReadAll(snapshot)

	if err != nil {
		return err
	}

	data := make(map[string]interface{})

	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}

	// for k, v := range data {
	// 	server.keyLocks[k].Lock()
	// 	server.SetValue(context.Background(), k, v)
	// 	server.keyLocks[k].Unlock()
	// }

	return nil
}
