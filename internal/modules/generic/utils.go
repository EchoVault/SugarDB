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
	"fmt"
	"github.com/echovault/sugardb/internal/clock"
	"strconv"
	"strings"
	"time"
)

type SetOptions struct {
	exists   string
	get      bool
	expireAt interface{} // Exact expireAt time un unix milliseconds
}

func getSetCommandOptions(clock clock.Clock, cmd []string, options SetOptions) (SetOptions, error) {
	if len(cmd) == 0 {
		return options, nil
	}
	switch strings.ToLower(cmd[0]) {
	case "get":
		options.get = true
		return getSetCommandOptions(clock, cmd[1:], options)

	case "nx":
		if options.exists != "" {
			return SetOptions{}, fmt.Errorf("cannot specify NX when %s is already specified", strings.ToUpper(options.exists))
		}
		options.exists = "NX"
		return getSetCommandOptions(clock, cmd[1:], options)

	case "xx":
		if options.exists != "" {
			return SetOptions{}, fmt.Errorf("cannot specify XX when %s is already specified", strings.ToUpper(options.exists))
		}
		options.exists = "XX"
		return getSetCommandOptions(clock, cmd[1:], options)

	case "ex":
		if len(cmd) < 2 {
			return SetOptions{}, errors.New("seconds value required after EX")
		}
		if options.expireAt != nil {
			return SetOptions{}, errors.New("cannot specify EX when expiry time is already set")
		}
		secondsStr := cmd[1]
		seconds, err := strconv.ParseInt(secondsStr, 10, 64)
		if err != nil {
			return SetOptions{}, errors.New("seconds value should be an integer")
		}
		options.expireAt = clock.Now().Add(time.Duration(seconds) * time.Second)
		return getSetCommandOptions(clock, cmd[2:], options)

	case "px":
		if len(cmd) < 2 {
			return SetOptions{}, errors.New("milliseconds value required after PX")
		}
		if options.expireAt != nil {
			return SetOptions{}, errors.New("cannot specify PX when expiry time is already set")
		}
		millisecondsStr := cmd[1]
		milliseconds, err := strconv.ParseInt(millisecondsStr, 10, 64)
		if err != nil {
			return SetOptions{}, errors.New("milliseconds value should be an integer")
		}
		options.expireAt = clock.Now().Add(time.Duration(milliseconds) * time.Millisecond)
		return getSetCommandOptions(clock, cmd[2:], options)

	case "exat":
		if len(cmd) < 2 {
			return SetOptions{}, errors.New("seconds value required after EXAT")
		}
		if options.expireAt != nil {
			return SetOptions{}, errors.New("cannot specify EXAT when expiry time is already set")
		}
		secondsStr := cmd[1]
		seconds, err := strconv.ParseInt(secondsStr, 10, 64)
		if err != nil {
			return SetOptions{}, errors.New("seconds value should be an integer")
		}
		options.expireAt = time.Unix(seconds, 0)
		return getSetCommandOptions(clock, cmd[2:], options)

	case "pxat":
		if len(cmd) < 2 {
			return SetOptions{}, errors.New("milliseconds value required after PXAT")
		}
		if options.expireAt != nil {
			return SetOptions{}, errors.New("cannot specify PXAT when expiry time is already set")
		}
		millisecondsStr := cmd[1]
		milliseconds, err := strconv.ParseInt(millisecondsStr, 10, 64)
		if err != nil {
			return SetOptions{}, errors.New("milliseconds value should be an integer")
		}
		options.expireAt = time.UnixMilli(milliseconds)
		return getSetCommandOptions(clock, cmd[2:], options)

	default:
		return SetOptions{}, fmt.Errorf("unknown option %s for set command", strings.ToUpper(cmd[0]))
	}
}
