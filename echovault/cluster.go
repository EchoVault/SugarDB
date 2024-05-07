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
	"encoding/json"
	"fmt"
	"github.com/echovault/echovault/internal"
	"time"
)

func (server *EchoVault) isInCluster() bool {
	return server.config.BootstrapCluster || server.config.JoinAddr != ""
}

func (server *EchoVault) raftApplyDeleteKey(ctx context.Context, key string) error {
	serverId, _ := ctx.Value("ServerID").(string)

	deleteKeyRequest := internal.ApplyRequest{
		Type:         "delete-key",
		ServerID:     serverId,
		ConnectionID: "",
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

	r, ok := applyFuture.Response().(internal.ApplyResponse)

	if !ok {
		return fmt.Errorf("unprocessable entity %v", r)
	}

	if r.Error != nil {
		return r.Error
	}

	return nil
}

func (server *EchoVault) raftApplyCommand(ctx context.Context, cmd []string) ([]byte, error) {
	serverId := ctx.Value("ServerID").(string)

	// Extract ConnectionID from the context.
	// If the command is executed in embedded mode, this value will be nil
	connectionIdContextValue := ctx.Value("ConnectionID")
	connectionId := "embedded"
	if connectionIdContextValue != nil {
		connectionId = connectionIdContextValue.(string)
	}

	applyRequest := internal.ApplyRequest{
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

	r, ok := applyFuture.Response().(internal.ApplyResponse)

	if !ok {
		return nil, fmt.Errorf("unprocessable entity %v", r)
	}

	if r.Error != nil {
		return nil, r.Error
	}

	return r.Response, nil
}
