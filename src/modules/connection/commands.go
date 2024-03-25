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

package connection

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

func handlePing(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	switch len(cmd) {
	default:
		return nil, errors.New(utils.WrongArgsResponse)
	case 1:
		return []byte("+PONG\r\n"), nil
	case 2:
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd[1]), cmd[1])), nil
	}
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:     "connection",
			Categories:  []string{utils.FastCategory, utils.ConnectionCategory},
			Description: "(PING [value]) Ping the echovault. If a value is provided, the value will be echoed.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: handlePing,
		},
	}
}
