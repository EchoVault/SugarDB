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

package str

import (
	"errors"

	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/constants"
)

func setRangeKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func strLenKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func subStrKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func appendKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}
