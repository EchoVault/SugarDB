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

package generic

import (
	"errors"

	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/constants"
)

func setKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 || len(cmd) > 7 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func msetKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd[1:])%2 != 0 {
		return internal.KeyExtractionFuncResult{}, errors.New("each key must be paired with a value")
	}
	var keys []string
	for i, key := range cmd[1:] {
		if i%2 == 0 {
			keys = append(keys, key)
		}
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: keys,
	}, nil
}

func getKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func mgetKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func delKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:],
	}, nil
}

func persistKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:],
	}, nil
}

func expireTimeKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func ttlKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func expireKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 || len(cmd) > 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func expireAtKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 || len(cmd) > 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func incrKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New("wrong number of arguments for INCR")
	}
	return internal.KeyExtractionFuncResult{
		WriteKeys: cmd[1:2],
	}, nil
}

func decrKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New("wrong number of arguments for INCR")
	}
	return internal.KeyExtractionFuncResult{
		WriteKeys: cmd[1:2],
	}, nil
}

func decrByKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		WriteKeys: []string{cmd[1]},
	}, nil
}
