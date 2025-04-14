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

type CopyOptions struct {
	database string
	replace bool
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

func getCopyCommandOptions(cmd []string, options CopyOptions) (CopyOptions, error) {
	if len(cmd) == 0 {
		return options, nil
	}

	switch strings.ToLower(cmd[0]){
	case "replace":
		options.replace = true
		return getCopyCommandOptions(cmd[1:], options)
		
	case "db":
		if len(cmd) < 2 {
			return CopyOptions{}, errors.New("syntax error")
		}

		_, err := strconv.Atoi(cmd[1])
		if err != nil {
			return CopyOptions{}, errors.New("value is not an integer or out of range")
		}
		
		options.database = cmd [1]
		return getCopyCommandOptions(cmd[2:], options)
		

	default:
		return CopyOptions{}, fmt.Errorf("unknown option %s for copy command", strings.ToUpper(cmd[0]))
	}
}

func matchPattern(pattern string, key string) bool {
	/*
		Implementation of Redis-style pattern matching
		https://redis.io/docs/latest/commands/keys/
	*/
	patternLen := len(pattern)
	keyLen := len(key) // length of the key to match
	patternPos := 0 // position in the pattern
	keyPos := 0 // position in the key

	for patternPos < patternLen {
		switch pattern[patternPos] {
		case '\\': // Match characters verbatum after slash
			if patternPos+1 < patternLen {
				patternPos++
				if keyPos >= keyLen || pattern[patternPos] != key[keyPos] {
					return false
				}
				keyPos++
			}
		case '?': // Match any single character (skip key position)
			// key position is at the end, return false
			if keyPos >= keyLen {
				return false
			}
			keyPos++
		case '*': // Match any sequence of characters
			// If pattern is at the end, return true
			if patternPos+1 >= patternLen {
				return true
			}
			// Use recursion to match the rest of the pattern at each position
			for i := keyPos; i <= keyLen; i++ {
				if matchPattern(pattern[patternPos+1:], key[i:]) {
					return true
				}
			}
			return false
		case '[': // Match any character in the character class brackets []
			// key position is at the end, return false
			if keyPos >= keyLen {
				return false
			}
			patternPos++ // skip the [ character
			// check if character class is negated (^)
			negate := false
			if patternPos < patternLen && pattern[patternPos] == '^' {
				negate = true
				patternPos++
			}

			// look through all characters in the character class
			matched := false
			for patternPos < patternLen && pattern[patternPos] != ']' {
				// if character is escaped, check the next character
				if pattern[patternPos] == '\\' && patternPos+1 < patternLen {
					patternPos++
					if pattern[patternPos] == key[keyPos] {
						matched = true
					}
				// if character is a range, check if the key position is within the range
				} else if patternPos+2 < patternLen && pattern[patternPos+1] == '-' {
					// Handle range
					if key[keyPos] >= pattern[patternPos] && key[keyPos] <= pattern[patternPos+2] {
						matched = true
					}
					patternPos += 2
				// if character is a match, set matched to true
				} else if pattern[patternPos] == key[keyPos] {
					matched = true
				}
				patternPos++
			}
			// if pattern position is at the end, return false
			if patternPos >= patternLen {
				return false
			}
			// negate check: if matched is true and negate is true, return false
			if matched == negate {
				return false
			}
			keyPos++
		default: // Match literal character (just like slash but on the current key position)
			if keyPos >= keyLen || pattern[patternPos] != key[keyPos] {
				return false
			}
			keyPos++
		}
		patternPos++
	}

	return keyPos == keyLen
}
