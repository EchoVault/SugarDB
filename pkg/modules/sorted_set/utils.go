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
	"slices"
	"strconv"
	"strings"
)

func extractKeysWeightsAggregateWithScores(cmd []string) ([]string, []int, string, bool, error) {
	var weights []int
	weightsIndex := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "weights")
	})
	if weightsIndex != -1 {
		for i := weightsIndex + 1; i < len(cmd); i++ {
			if slices.Contains([]string{"aggregate", "withscores"}, strings.ToLower(cmd[i])) {
				break
			}
			w, err := strconv.Atoi(cmd[i])
			if err != nil {
				return []string{}, []int{}, "", false, err
			}
			weights = append(weights, w)
		}
	}

	aggregate := "sum"
	aggregateIndex := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "aggregate")
	})
	if aggregateIndex != -1 {
		if !slices.Contains([]string{"sum", "min", "max"}, strings.ToLower(cmd[aggregateIndex+1])) {
			return []string{}, []int{}, "", false, errors.New("aggregate must be SUM, MIN, or MAX")
		}
		aggregate = strings.ToLower(cmd[aggregateIndex+1])
	}

	withscores := false
	withscoresIndex := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "withscores")
	})
	if withscoresIndex != -1 {
		withscores = true
	}

	// Get the first modifier index as this will be the upper boundary when extracting the keys
	firstModifierIndex := -1
	for _, modifierIndex := range []int{weightsIndex, aggregateIndex, withscoresIndex} {
		if modifierIndex == -1 {
			continue
		}
		if firstModifierIndex == -1 {
			firstModifierIndex = modifierIndex
			continue
		}
		if modifierIndex < firstModifierIndex {
			firstModifierIndex = modifierIndex
		}
	}

	var keys []string
	if firstModifierIndex == -1 {
		keys = cmd[1:]
	} else {
		keys = cmd[1:firstModifierIndex]
	}

	if weightsIndex != -1 && (len(keys) != len(weights)) {
		return []string{}, []int{}, "", false, errors.New("number of weights should match number of keys")
	} else if weightsIndex == -1 {
		for i := 0; i < len(keys); i++ {
			weights = append(weights, 1)
		}
	}

	return keys, weights, aggregate, withscores, nil
}
