package sorted_set

import (
	"cmp"
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"slices"
	"strconv"
	"strings"
)

func extractKeysWeightsAggregateWithScores(cmd []string) ([]string, []int, string, bool, error) {
	firstModifierIndex := -1

	var weights []int
	weightsIndex := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "weights")
	})
	if weightsIndex != -1 {
		firstModifierIndex = weightsIndex
		for i := weightsIndex + 1; i < len(cmd); i++ {
			if utils.Contains([]string{"aggregate", "withscores"}, cmd[i]) {
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
		if firstModifierIndex != -1 && (aggregateIndex != -1 && aggregateIndex < firstModifierIndex) {
			firstModifierIndex = aggregateIndex
		} else if firstModifierIndex == -1 {
			firstModifierIndex = aggregateIndex
		}
		if aggregateIndex >= len(cmd)-1 {
			return []string{}, []int{}, "", false, errors.New("aggregate must be SUM, MIN, or MAX")
		}
		if !utils.Contains([]string{"sum", "min", "max"}, strings.ToLower(cmd[aggregateIndex+1])) {
			return []string{}, []int{}, "", false, errors.New("aggregate must be SUM, MIN, or MAX")
		}
		aggregate = strings.ToLower(cmd[aggregateIndex+1])
	}

	withscores := false
	withscoresIndex := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "withscores")
	})
	if withscoresIndex != -1 {
		if firstModifierIndex != -1 && (withscoresIndex != -1 && withscoresIndex < firstModifierIndex) {
			firstModifierIndex = withscoresIndex
		} else if firstModifierIndex == -1 {
			firstModifierIndex = withscoresIndex
		}
		withscores = true
	}

	var keys []string
	if firstModifierIndex == -1 {
		keys = cmd[1:]
	} else if firstModifierIndex != -1 && firstModifierIndex < 2 {
		return []string{}, []int{}, "", false, errors.New("must provide at least 1 key")
	} else {
		keys = cmd[1:firstModifierIndex]
	}
	if len(keys) < 1 {
		return []string{}, []int{}, "", false, errors.New("must provide at least 1 key")
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
	if !utils.Contains([]string{"nx", "xx"}, strings.ToLower(policy)) {
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
	if !utils.Contains([]string{"lt", "gt"}, strings.ToLower(comp)) {
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
