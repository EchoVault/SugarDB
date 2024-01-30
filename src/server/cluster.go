package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"time"
)

func (server *Server) IsInCluster() bool {
	return server.Config.BootstrapCluster || server.Config.JoinAddr != ""
}

func (server *Server) raftApply(ctx context.Context, cmd []string) ([]byte, error) {
	serverId, _ := ctx.Value(utils.ContextServerID("ServerID")).(string)
	connectionId, _ := ctx.Value(utils.ContextConnID("ConnectionID")).(string)

	applyRequest := utils.ApplyRequest{
		ServerID:     serverId,
		ConnectionID: connectionId,
		CMD:          cmd,
	}

	b, err := json.Marshal(applyRequest)

	if err != nil {
		return nil, errors.New("could not parse request")
	}

	applyFuture := server.raft.Apply(b, 500*time.Millisecond)

	if err := applyFuture.Error(); err != nil {
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
