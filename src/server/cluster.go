package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"time"
)

func (server *Server) IsInCluster() bool {
	return server.Config.BootstrapCluster || server.Config.JoinAddr != ""
}

func (server *Server) raftApplyDeleteKey(ctx context.Context, key string) error {
	serverId, _ := ctx.Value(utils.ContextServerID("ServerID")).(string)

	deleteKeyRequest := utils.ApplyRequest{
		Type:         "delete-key",
		ServerID:     serverId,
		ConnectionID: "nil",
		Key:          key,
	}

	b, err := json.Marshal(deleteKeyRequest)
	if err != nil {
		return fmt.Errorf("could not parse delete key request for key: %s", key)
	}

	applyFuture := server.raft.Apply(b, 500*time.Millisecond)

	if err = applyFuture.Error(); err != nil {
		return err
	}

	r, ok := applyFuture.Response().(utils.ApplyResponse)

	if !ok {
		return fmt.Errorf("unprocessable entity %v", r)
	}

	if r.Error != nil {
		return r.Error
	}

	return nil
}

func (server *Server) raftApplyCommand(ctx context.Context, cmd []string) ([]byte, error) {
	serverId, _ := ctx.Value(utils.ContextServerID("ServerID")).(string)
	connectionId, _ := ctx.Value(utils.ContextConnID("ConnectionID")).(string)

	applyRequest := utils.ApplyRequest{
		Type:         "command",
		ServerID:     serverId,
		ConnectionID: connectionId,
		CMD:          cmd,
	}

	b, err := json.Marshal(applyRequest)
	if err != nil {
		return nil, fmt.Errorf("could not parse command request for commad: %+v", cmd)
	}

	applyFuture := server.raft.Apply(b, 500*time.Millisecond)

	if err = applyFuture.Error(); err != nil {
		return nil, err
	}

	r, ok := applyFuture.Response().(utils.ApplyResponse)

	if !ok {
		return nil, fmt.Errorf("unprocessable entity %v", r)
	}

	if r.Error != nil {
		return nil, r.Error
	}

	return r.Response, nil
}
