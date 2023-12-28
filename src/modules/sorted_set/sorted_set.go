package sorted_set

import (
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"strings"
)

type Value string

type Score float64

// MemberObject is the shape of the object as it's stored in the map that represents the set
type MemberObject struct {
	value  Value
	score  Score
	exists bool
}

// MemberParam is the shape of the object passed as a parameter to NewSortedSet and the Add method
type MemberParam struct {
	value Value
	score Score
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

type SortedSet struct {
	members map[Value]MemberObject
}

func NewSortedSet(members []MemberParam) *SortedSet {
	s := &SortedSet{
		members: make(map[Value]MemberObject),
	}
	for _, m := range members {
		s.members[m.value] = MemberObject{
			value:  m.value,
			score:  m.score,
			exists: true,
		}
	}
	return s
}

func (set *SortedSet) Contains(m Value) bool {
	return set.members[m].exists
}

func (set *SortedSet) Get(v Value) MemberObject {
	return set.members[v]
}

func (set *SortedSet) GetAll() []MemberParam {
	var res []MemberParam
	for k, v := range set.members {
		res = append(res, MemberParam{
			value: k,
			score: v.score,
		})
	}
	return res
}

func (set *SortedSet) Cardinality() int {
	return len(set.GetAll())
}

func (set *SortedSet) AddOrUpdate(
	members []MemberParam, updatePolicy interface{}, comparison interface{}, changed interface{}, incr interface{},
) (int, error) {
	policy, err := validateUpdatePolicy(updatePolicy)
	if err != nil {
		return 0, err
	}
	comp, err := validateComparison(comparison)
	if err != nil {
		return 0, err
	}
	ch, err := validateChanged(changed)
	if err != nil {
		return 0, err
	}
	inc, err := validateIncr(incr)
	if err != nil {
		return 0, err
	}
	if strings.EqualFold(policy, "nx") && comp != "" {
		return 0, errors.New("cannot use GT or LT when update policy is NX")
	}
	if strings.EqualFold(inc, "incr") && len(members) != 1 {
		return 0, errors.New("INCR can only be used with one member/score pair")
	}

	count := 0

	if strings.EqualFold(inc, "incr") {
		for _, m := range members {
			if !set.Contains(m.value) {
				return count, fmt.Errorf("cannot increment member %s as it does not exist in the sorted set", m.value)
			}
			set.members[m.value] = MemberObject{
				value:  m.value,
				score:  set.members[m.value].score + m.score,
				exists: true,
			}
			if strings.EqualFold(ch, "ch") {
				count += 1
			}
		}
		return count, nil
	}

	for _, m := range members {
		if strings.EqualFold(policy, "xx") {
			// Only update existing elements, do not add new elements
			if set.Contains(m.value) {
				set.members[m.value] = MemberObject{
					value:  m.value,
					score:  compareScores(set.members[m.value].score, m.score, comp),
					exists: true,
				}
				if strings.EqualFold(ch, "ch") {
					count += 1
				}
			}
			continue
		}
		if strings.EqualFold(policy, "nx") {
			// Only add new elements, do not update existing elements
			if !set.Contains(m.value) {
				set.members[m.value] = MemberObject{
					value:  m.value,
					score:  m.score,
					exists: true,
				}
				count += 1
			}
			continue
		}
		// Policy not specified, just set the elements and scores
		if set.members[m.value].score != m.score {
			count += 1
		}
		set.members[m.value] = MemberObject{
			value:  m.value,
			score:  compareScores(set.members[m.value].score, m.score, comp),
			exists: true,
		}
	}
	return count, nil
}
