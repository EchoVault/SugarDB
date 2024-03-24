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

package hash

import (
	"errors"
	"github.com/echovault/echovault/src/utils"
)

func hsetKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func hsetnxKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func hgetKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func hstrlenKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func hvalsKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func hrandfieldKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 || len(cmd) > 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	if len(cmd) == 2 {
		return cmd[1:], nil
	}
	return cmd[1:2], nil
}

func hlenKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func hkeysKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func hincrbyKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func hgetallKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func hexistsKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func hdelKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}
