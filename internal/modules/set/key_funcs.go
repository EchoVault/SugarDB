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

package set

import (
	"errors"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/types"
	"slices"
	"strings"
)

func saddKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func scardKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) != 2 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func sdiffKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 2 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func sdiffstoreKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[2:],
		WriteKeys: cmd[1:2],
	}, nil
}

func sinterKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 2 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func sintercardKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 2 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}

	limitIdx := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "limit")
	})

	if limitIdx == -1 {
		return types.AccessKeys{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[1:],
			WriteKeys: make([]string, 0),
		}, nil
	}

	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:limitIdx],
		WriteKeys: make([]string, 0),
	}, nil
}

func sinterstoreKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[2:],
		WriteKeys: cmd[1:2],
	}, nil
}

func sismemberKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) != 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func smembersKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) != 2 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func smismemberKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func smoveKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) != 4 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:3],
	}, nil
}

func spopKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func srandmemberKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func sremKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func sunionKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 2 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func sunionstoreKeyFunc(cmd []string) (types.AccessKeys, error) {
	if len(cmd) < 3 {
		return types.AccessKeys{}, errors.New(constants.WrongArgsResponse)
	}
	return types.AccessKeys{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[2:],
		WriteKeys: cmd[1:2],
	}, nil
}
