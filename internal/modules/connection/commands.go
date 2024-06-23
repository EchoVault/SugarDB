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
	"github.com/echovault/echovault/internal/modules/acl"
	"slices"
	"strconv"

	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/constants"
)

func handleAuth(params internal.HandlerFuncParams) ([]byte, error) {
	if len(params.Command) < 2 || len(params.Command) > 3 {
		return nil, errors.New(constants.WrongArgsResponse)
	}
	accessControlList, ok := params.GetACL().(*acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}
	accessControlList.LockUsers()
	defer accessControlList.UnlockUsers()

	if err := accessControlList.AuthenticateConnection(params.Context, params.Connection, params.Command); err != nil {
		return nil, err
	}
	return []byte(constants.OkResponse), nil
}

func handlePing(params internal.HandlerFuncParams) ([]byte, error) {
	switch len(params.Command) {
	default:
		return nil, errors.New(constants.WrongArgsResponse)
	case 1:
		return []byte("+PONG\r\n"), nil
	case 2:
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(params.Command[1]), params.Command[1])), nil
	}
}

func handleEcho(params internal.HandlerFuncParams) ([]byte, error) {
	if len(params.Command) != 2 {
		return nil, errors.New(constants.WrongArgsResponse)
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(params.Command[1]), params.Command[1])), nil
}

func handleHello(params internal.HandlerFuncParams) ([]byte, error) {
	if !slices.Contains([]int{1, 2, 4, 5, 7}, len(params.Command)) {
		return nil, errors.New(constants.WrongArgsResponse)
	}

	if len(params.Command) == 1 {
		serverInfo := params.GetServerInfo()
		connectionInfo := params.GetConnectionInfo(params.Connection)
		return buildHelloResponse(serverInfo, connectionInfo), nil
	}

	options, err := getHelloOptions(
		params.Command[2:],
		helloOptions{
			protocol:   2,
			clientname: "",
			auth: struct {
				authenticate bool
				username     string
				password     string
			}{
				authenticate: false,
				username:     "",
				password:     "",
			},
		})

	if err != nil {
		return nil, err
	}

	// Get protocol version
	protocol, err := strconv.Atoi(params.Command[1])
	if err != nil {
		return nil, err
	}
	if !slices.Contains([]int{2, 3}, protocol) {
		return nil, errors.New("protocol must be 2 or 3")
	}
	options.protocol = protocol

	// If AUTH option is provided, authenticate the connection.
	if options.auth.authenticate {
		accessControlList, ok := params.GetACL().(*acl.ACL)
		if !ok {
			return nil, errors.New("could not load ACL")
		}
		accessControlList.LockUsers()
		defer accessControlList.UnlockUsers()
		if err = accessControlList.AuthenticateConnection(
			params.Context,
			params.Connection,
			[]string{"AUTH", options.auth.username, options.auth.password},
		); err != nil {
			return nil, err
		}
	}

	// Set the connection details.
	params.SetConnectionInfo(params.Connection, options.protocol, options.clientname)

	// Get the new connection details and server info to return to the client.
	serverInfo := params.GetServerInfo()
	connectionInfo := params.GetConnectionInfo(params.Connection)
	return buildHelloResponse(serverInfo, connectionInfo), nil
}

func Commands() []internal.Command {
	return []internal.Command{
		{
			Command:    "auth",
			Module:     constants.ConnectionModule,
			Categories: []string{constants.ConnectionCategory, constants.SlowCategory},
			Description: `(AUTH [username] password) 
Authenticates the connection. If the username is not provided, the connection will be authenticated against the
default ACL user. Otherwise, it is authenticated against the ACL user with the provided username.`,
			Sync: false,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels:  make([]string, 0),
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handleAuth,
		},
		{
			Command:    "ping",
			Module:     constants.ConnectionModule,
			Categories: []string{constants.ConnectionCategory, constants.FastCategory},
			Description: `(PING [message])
Ping the echovault server. If a message is provided, the message will be echoed back to the client.
Otherwise, the server will return "PONG".`,
			Sync: false,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels:  make([]string, 0),
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handlePing,
		},
		{
			Command:     "echo",
			Module:      constants.ConnectionModule,
			Categories:  []string{constants.ConnectionCategory, constants.FastCategory},
			Description: `(ECHO message) Echo the message back to the client.`,
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels:  make([]string, 0),
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handleEcho,
		},
		{
			Command:     "hello",
			Module:      constants.ConnectionModule,
			Categories:  []string{constants.FastCategory, constants.ConnectionCategory},
			Description: `(HELLO [protover [AUTH username password] [SETNAME clientname]])`,
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels:  make([]string, 0),
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handleHello,
		},
	}
}
