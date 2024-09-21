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

package sorted_set

import (
	"errors"
	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/constants"
	"slices"
	"strings"
)

func zaddKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func zcardKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:],
		WriteKeys: make([]string, 0),
	}, nil
}

func zcountKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zdiffKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}

	withscoresIndex := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "withscores")
	})

	if withscoresIndex == -1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[1:],
			WriteKeys: make([]string, 0),
		}, nil
	}

	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:withscoresIndex],
		WriteKeys: make([]string, 0),
	}, nil
}

func zdiffstoreKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[2:],
		WriteKeys: cmd[1:2],
	}, nil
}

func zincrbyKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func zinterKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		if strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES") {
			return true
		}
		return false
	})
	if endIdx == -1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[1:],
			WriteKeys: make([]string, 0),
		}, nil
	}
	if endIdx >= 1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[1:endIdx],
			WriteKeys: make([]string, 0),
		}, nil
	}
	return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
}

func zinterstoreKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		return strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES")
	})

	if endIdx == -1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[2:],
			WriteKeys: cmd[1:2],
		}, nil
	}

	if endIdx >= 3 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[2 : endIdx+1],
			WriteKeys: cmd[1:2],
		}, nil
	}

	return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
}

func zmpopKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd, func(s string) bool {
		return slices.Contains([]string{"MIN", "MAX", "COUNT"}, strings.ToUpper(s))
	})
	if endIdx == -1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  make([]string, 0),
			WriteKeys: cmd[1:],
		}, nil
	}
	if endIdx >= 2 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  make([]string, 0),
			WriteKeys: cmd[1:endIdx],
		}, nil
	}
	return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
}

func zmscoreKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zpopKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func zrandmemberKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 || len(cmd) > 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zrankKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 || len(cmd) > 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zremKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func zrevrankKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zscoreKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zremrangebylexKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func zremrangebyrankKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func zremrangebyscoreKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  make([]string, 0),
		WriteKeys: cmd[1:2],
	}, nil
}

func zlexcountKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) != 4 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zrangeKeyCount(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 4 || len(cmd) > 10 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[1:2],
		WriteKeys: make([]string, 0),
	}, nil
}

func zrangeStoreKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 5 || len(cmd) > 11 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  cmd[2:3],
		WriteKeys: cmd[1:2],
	}, nil
}

func zunionKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 2 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		if strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES") {
			return true
		}
		return false
	})
	if endIdx == -1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[1:],
			WriteKeys: make([]string, 0),
		}, nil
	}
	if endIdx >= 1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[1:endIdx],
			WriteKeys: cmd[1:endIdx],
		}, nil
	}
	return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
}

func zunionstoreKeyFunc(cmd []string) (internal.KeyExtractionFuncResult, error) {
	if len(cmd) < 3 {
		return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		if strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES") {
			return true
		}
		return false
	})
	if endIdx == -1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[2:],
			WriteKeys: cmd[1:2],
		}, nil
	}
	if endIdx >= 1 {
		return internal.KeyExtractionFuncResult{
			Channels:  make([]string, 0),
			ReadKeys:  cmd[2 : endIdx+1],
			WriteKeys: cmd[1:2],
		}, nil
	}
	return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
}
