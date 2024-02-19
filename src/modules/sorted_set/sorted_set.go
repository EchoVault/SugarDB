package sorted_set

import (
	"cmp"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"math"
	"math/rand"
	"slices"
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

func (set *SortedSet) GetRandom(count int) []MemberParam {
	var res []MemberParam

	members := set.GetAll()

	if utils.AbsInt(count) >= len(members) {
		return members
	}

	var n int

	if count < 0 {
		// If count is negative, allow repeat numbers
		for i := 0; i < utils.AbsInt(count); i++ {
			n = rand.Intn(len(members))
			res = append(res, members[n])
		}
	} else {
		// If count is positive only allow unique values
		for i := 0; i < utils.AbsInt(count); {
			n = rand.Intn(len(members))
			if !slices.ContainsFunc(res, func(m MemberParam) bool {
				return m.value == members[n].value
			}) {
				res = append(res, members[n])
				slices.DeleteFunc(members, func(m MemberParam) bool {
					return m.value == members[n].value
				})
				i++
			}
		}
	}

	return res
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
			if slices.Contains([]Score{Score(math.Inf(-1)), Score(math.Inf(1))}, set.members[m.value].score) {
				return count, errors.New("cannot increment -inf or +inf")
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
		if set.members[m.value].score != m.score || !set.members[m.value].exists {
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

func (set *SortedSet) Remove(v Value) bool {
	if set.Contains(v) {
		delete(set.members, v)
		return true
	}
	return false
}

func (set *SortedSet) Pop(count int, policy string) (*SortedSet, error) {
	popped := NewSortedSet([]MemberParam{})
	if !slices.Contains([]string{"min", "max"}, strings.ToLower(policy)) {
		return nil, errors.New("policy must be MIN or MAX")
	}
	if count < 0 {
		return nil, errors.New("count must be a positive integer")
	}
	if count == 0 {
		return popped, nil
	}

	members := set.GetAll()

	slices.SortFunc(members, func(a, b MemberParam) int {
		if strings.EqualFold(policy, "min") {
			return cmp.Compare(a.score, b.score)
		}
		return cmp.Compare(b.score, a.score)
	})

	for i := 0; i < count; i++ {
		if i < len(members) {
			set.Remove(members[i].value)
			_, err := popped.AddOrUpdate([]MemberParam{members[i]}, nil, nil, nil, nil)
			if err != nil {
				fmt.Println(err.Error())
				// TODO: Add all the removed elements back if we encounter an error
				return nil, err
			}
		}
	}

	return popped, nil
}

func (set *SortedSet) Subtract(others []*SortedSet) *SortedSet {
	res := NewSortedSet(set.GetAll())
	for _, ss := range others {
		for _, m := range ss.GetAll() {
			if res.Contains(m.value) {
				res.Remove(m.value)
			}
		}
	}
	return res
}

func (set *SortedSet) Union(others []*SortedSet, weights []int, aggregate string) (*SortedSet, error) {
	res := NewSortedSet([]MemberParam{})
	// Add elements from this set
	for _, m := range set.GetAll() {
		if _, err := res.AddOrUpdate(
			[]MemberParam{{value: m.value, score: m.score * Score(weights[0])}},
			nil, nil, nil, nil); err != nil {
			return nil, err
		}
	}
	// Add elements from the other sets
	var weightsIndex int
	var score Score
	for setIndex, sortedSet := range others {
		weightsIndex = setIndex + 1
		for _, m := range sortedSet.GetAll() {
			if !res.Contains(m.value) {
				// This member is not contained in the union
				if _, err := res.AddOrUpdate([]MemberParam{
					{value: m.value, score: m.score * Score(weights[weightsIndex])},
				}, nil, nil, nil, nil); err != nil {
					return nil, err
				}
			} else {
				// This member is contained in the union
				score = res.Get(m.value).score
				switch strings.ToLower(aggregate) {
				case "sum":
					score = score + (m.score * Score(weights[weightsIndex]))
				case "min":
					score = compareScores(score, m.score*Score(weights[weightsIndex]), "lt")
				case "max":
					score = compareScores(score, m.score*Score(weights[weightsIndex]), "gt")
				}
				if _, err := res.AddOrUpdate([]MemberParam{
					{value: m.value, score: score},
				}, nil, nil, nil, nil); err != nil {
					return nil, err
				}
			}
		}
	}
	return res, nil
}

func (set *SortedSet) Intersect(others []*SortedSet, weights []int, aggregate string) (*SortedSet, error) {
	res := NewSortedSet([]MemberParam{})
	// Find intersect between this set and the first set in others
	var score Score
	for _, m := range set.GetAll() {
		if others[0].Contains(m.value) {
			switch strings.ToLower(aggregate) {
			case "sum":
				score = m.score*Score(weights[0]) + (others[0].Get(m.value).score * Score(weights[1]))
			case "min":
				score = compareScores(m.score*Score(weights[0]), others[0].Get(m.value).score*Score(weights[1]), "lt")
			case "max":
				score = compareScores(m.score*Score(weights[0]), others[0].Get(m.value).score*Score(weights[1]), "gt")
			}
			if _, err := res.AddOrUpdate([]MemberParam{
				{value: m.value, score: score},
			}, nil, nil, nil, nil); err != nil {
				return nil, err
			}
		}
	}
	// Calculate intersect with the remaining sets in others
	for setIdx, sortedSet := range others[1:] {
		for _, m := range sortedSet.GetAll() {
			if res.Contains(m.value) {
				switch strings.ToLower(aggregate) {
				case "sum":
					score = res.Get(m.value).score + (m.score * Score(weights[setIdx+1]))
				case "min":
					score = compareScores(res.Get(m.value).score, m.score*Score(weights[setIdx+1]), "lt")
				case "max":
					score = compareScores(res.Get(m.value).score, m.score*Score(weights[setIdx+1]), "gt")
				}
				if _, err := res.AddOrUpdate([]MemberParam{
					{value: m.value, score: score},
				}, nil, nil, nil, nil); err != nil {
					return nil, err
				}
			}
		}
	}
	return res, nil
}
