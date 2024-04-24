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

package admin

import (
	"bytes"
	"context"
	"fmt"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/echovault"
	"github.com/echovault/echovault/pkg/types"
	"github.com/tidwall/resp"
	"net"
	"strings"
	"testing"
)

var mockServer *echovault.EchoVault

func init() {
	mockServer, _ = echovault.NewEchoVault(
		echovault.WithConfig(config.Config{
			DataDir:        "",
			EvictionPolicy: constants.NoEviction,
		}),
	)
}

func getHandler(commands ...string) types.HandlerFunc {
	if len(commands) == 0 {
		return nil
	}
	for _, c := range mockServer.GetAllCommands() {
		if strings.EqualFold(commands[0], c.Command) && len(commands) == 1 {
			// Get command handler
			return c.HandlerFunc
		}
		if strings.EqualFold(commands[0], c.Command) {
			// Get sub-command handler
			for _, sc := range c.SubCommands {
				if strings.EqualFold(commands[1], sc.Command) {
					return sc.HandlerFunc
				}
			}
		}
	}
	return nil
}

func getHandlerFuncParams(ctx context.Context, cmd []string, conn *net.Conn) types.HandlerFuncParams {
	return types.HandlerFuncParams{
		Context:        ctx,
		Command:        cmd,
		Connection:     conn,
		GetAllCommands: mockServer.GetAllCommands,
	}
}

func Test_CommandsHandler(t *testing.T) {
	res, err := getHandler("COMMANDS")(getHandlerFuncParams(context.Background(), []string{"commands"}, nil))
	if err != nil {
		t.Error(err)
	}

	rd := resp.NewReader(bytes.NewReader(res))
	rv, _, err := rd.ReadValue()
	if err != nil {
		t.Error(err)
	}

	for _, element := range rv.Array() {
		fmt.Println(element)
	}
}
