package sorted_set

import (
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"strings"
)

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
