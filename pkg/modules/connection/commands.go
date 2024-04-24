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
	"errors"
	"fmt"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/types"
)

func handlePing(params types.HandlerFuncParams) ([]byte, error) {
	switch len(params.Command) {
	default:
		return nil, errors.New(constants.WrongArgsResponse)
	case 1:
		return []byte("+PONG\r\n"), nil
	case 2:
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(params.Command[1]), params.Command[1])), nil
	}
}

func Commands() []types.Command {
	return []types.Command{
		{
			Command:     "ping",
			Module:      constants.ConnectionModule,
			Categories:  []string{constants.FastCategory, constants.ConnectionCategory},
			Description: "(PING [value]) Ping the echovault. If a value is provided, the value will be echoed.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) (types.AccessKeys, error) {
				return types.AccessKeys{
					Channels:  make([]string, 0),
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handlePing,
		},
	}
}
