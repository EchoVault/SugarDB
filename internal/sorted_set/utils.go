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
	"strings"
)

func validateUpdatePolicy(updatePolicy interface{}) (string, error) {
	if updatePolicy == nil {
		return "", nil
	}
	err := errors.New("update policy must be a string of Value NX or XX")
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
	err := errors.New("comparison condition must be a string of Value LT or GT")
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
	err := errors.New("changed condition should be a string of Value CH")
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
	err := errors.New("incr condition should be a string of Value INCR")
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
