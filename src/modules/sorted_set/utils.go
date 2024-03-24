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
	"cmp"
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

func validateUpdatePolicy(updatePolicy interface{}) (string, error) {
	if updatePolicy == nil {
		return "", nil
	}
	err := errors.New("update policy must be a string of value NX or XX")
	policy, ok := updatePolicy.(string)
	if !ok {
		return "", err
	}
	if !slices.Contains([]string{"nx", "xx"}, strings.ToLower(policy)) {
		return "", err
	}
	return policy, nil
}

func validateComparison(comparison interface{}) (string, error) {
	if comparison == nil {
		return "", nil
	}
	err := errors.New("comparison condition must be a string of value LT or GT")
	comp, ok := comparison.(string)
	if !ok {
		return "", err
	}
	if !slices.Contains([]string{"lt", "gt"}, strings.ToLower(comp)) {
		return "", err
	}
	return comp, nil
}

func validateChanged(changed interface{}) (string, error) {
	if changed == nil {
		return "", nil
	}
	err := errors.New("changed condition should be a string of value CH")
	ch, ok := changed.(string)
	if !ok {
		return "", err
	}
	if !strings.EqualFold(ch, "ch") {
		return "", err
	}
	return ch, nil
}

func validateIncr(incr interface{}) (string, error) {
	if incr == nil {
		return "", nil
	}
	err := errors.New("incr condition should be a string of value INCR")
	i, ok := incr.(string)
	if !ok {
		return "", err
	}
	if !strings.EqualFold(i, "incr") {
		return "", err
	}
	return i, nil
}

func compareScores(old Score, new Score, comp string) Score {
	switch strings.ToLower(comp) {
	default:
		return new
	case "lt":
		if new < old {
			return new
		}
		return old
	case "gt":
		if new > old {
			return new
		}
		return old
	}
}

// compareLex returns -1 when s2 is lexicographically greater than s1,
// 0 if they're equal and 1 if s2 is lexicographically less than s1.
func compareLex(s1 string, s2 string) int {
	if s1 == s2 {
		return 0
	}
	if strings.Contains(s1, s2) {
		return 1
	}
	if strings.Contains(s2, s1) {
		return -1
	}

	limit := len(s1)
	if len(s2) < limit {
		limit = len(s2)
	}

	var c int
	for i := 0; i < limit; i++ {
		c = cmp.Compare(s1[i], s2[i])
		if c != 0 {
			break
		}
	}

	return c
}
