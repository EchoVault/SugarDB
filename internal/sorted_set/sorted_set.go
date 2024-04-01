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
	"github.com/echovault/echovault/internal"
	"math"
	"math/rand"
	"slices"
	"strings"
)

type Value string

type Score float64

// MemberObject is the shape of the object as it's stored in the map that represents the Set
type MemberObject struct {
	Value  Value
	Score  Score
	Exists bool
}

// MemberParam is the shape of the object passed as a parameter to NewSortedSet and the Add method
type MemberParam struct {
	Value Value
	Score Score
}

type SortedSet struct {
	members map[Value]MemberObject
}

func NewSortedSet(members []MemberParam) *SortedSet {
	s := &SortedSet{
		members: make(map[Value]MemberObject),
	}
	for _, m := range members {
		s.members[m.Value] = MemberObject{
			Value:  m.Value,
			Score:  m.Score,
			Exists: true,
		}
	}
	return s
}

func (set *SortedSet) Contains(m Value) bool {
	return set.members[m].Exists
}

func (set *SortedSet) Get(v Value) MemberObject {
	return set.members[v]
}

func (set *SortedSet) GetRandom(count int) []MemberParam {
	var res []MemberParam

	members := set.GetAll()

	if internal.AbsInt(count) >= len(members) {
		return members
	}

	var n int

	if count < 0 {
		// If count is negative, allow repeat numbers
		for i := 0; i < internal.AbsInt(count); i++ {
			n = rand.Intn(len(members))
			res = append(res, members[n])
		}
	} else {
		// If count is positive only allow unique values
		for i := 0; i < internal.AbsInt(count); {
			n = rand.Intn(len(members))
			if !slices.ContainsFunc(res, func(m MemberParam) bool {
				return m.Value == members[n].Value
			}) {
				res = append(res, members[n])
				slices.DeleteFunc(members, func(m MemberParam) bool {
					return m.Value == members[n].Value
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
			Value: k,
			Score: v.Score,
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
		return 0, errors.New("INCR can only be used with one member/Score pair")
	}

	count := 0

	if strings.EqualFold(inc, "incr") {
		for _, m := range members {
			if !set.Contains(m.Value) {
				// If the member is not contained, add it with the increment as its Score
				set.members[m.Value] = MemberObject{
					Value:  m.Value,
					Score:  m.Score,
					Exists: true,
				}
				// Always add count because this is the addition of a new element
				count += 1
				return count, err
			}
			if slices.Contains([]Score{Score(math.Inf(-1)), Score(math.Inf(1))}, set.members[m.Value].Score) {
				return count, errors.New("cannot increment -inf or +inf")
			}
			set.members[m.Value] = MemberObject{
				Value:  m.Value,
				Score:  set.members[m.Value].Score + m.Score,
				Exists: true,
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
			if set.Contains(m.Value) {
				set.members[m.Value] = MemberObject{
					Value:  m.Value,
					Score:  compareScores(set.members[m.Value].Score, m.Score, comp),
					Exists: true,
				}
				if strings.EqualFold(ch, "ch") {
					count += 1
				}
			}
			continue
		}
		if strings.EqualFold(policy, "nx") {
			// Only add new elements, do not update existing elements
			if !set.Contains(m.Value) {
				set.members[m.Value] = MemberObject{
					Value:  m.Value,
					Score:  m.Score,
					Exists: true,
				}
				count += 1
			}
			continue
		}
		// Policy not specified, just Set the elements and scores
		if set.members[m.Value].Score != m.Score || !set.members[m.Value].Exists {
			count += 1
		}
		set.members[m.Value] = MemberObject{
			Value:  m.Value,
			Score:  compareScores(set.members[m.Value].Score, m.Score, comp),
			Exists: true,
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
			return cmp.Compare(a.Score, b.Score)
		}
		return cmp.Compare(b.Score, a.Score)
	})

	for i := 0; i < count; i++ {
		if i >= len(members) {
			break
		}
		set.Remove(members[i].Value)
		_, err := popped.AddOrUpdate([]MemberParam{members[i]}, nil, nil, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	return popped, nil
}

func (set *SortedSet) Subtract(others []*SortedSet) *SortedSet {
	res := NewSortedSet(set.GetAll())
	for _, ss := range others {
		for _, m := range ss.GetAll() {
			if res.Contains(m.Value) {
				res.Remove(m.Value)
			}
		}
	}
	return res
}

// SortedSetParam is a composite object used for Intersect and Union function
type SortedSetParam struct {
	Set    *SortedSet
	Weight int
}

func (set *SortedSet) Equals(other *SortedSet) bool {
	if set.Cardinality() != other.Cardinality() {
		return false
	}
	if set.Cardinality() == 0 {
		return true
	}
	for _, member := range set.members {
		if !other.Contains(member.Value) {
			return false
		}
		if member.Score != other.Get(member.Value).Score {
			return false
		}
	}
	return true
}

// Union uses divided & conquer to calculate the union of multiple sets
func Union(aggregate string, setParams ...SortedSetParam) *SortedSet {
	switch len(setParams) {
	case 0:
		return NewSortedSet([]MemberParam{})
	case 1:
		var params []MemberParam
		for _, member := range setParams[0].Set.GetAll() {
			params = append(params, MemberParam{
				Value: member.Value,
				Score: member.Score * Score(setParams[0].Weight),
			})
		}
		return NewSortedSet(params)
	case 2:
		var params []MemberParam
		// Traverse the params in the left sorted Set
		for _, member := range setParams[0].Set.GetAll() {
			// If the member does not exist in the other sorted Set, add it to params along with the appropriate Weight
			if !setParams[1].Set.Contains(member.Value) {
				params = append(params, MemberParam{
					Value: member.Value,
					Score: member.Score * Score(setParams[0].Weight),
				})
				continue
			}
			// If the member Exists, get both elements and apply the Weight
			param := MemberParam{
				Value: member.Value,
				Score: func(left, right Score) Score {
					// Choose which param to add to params depending on the aggregate
					switch aggregate {
					case "sum":
						return left + right
					case "min":
						return compareScores(left, right, "lt")
					default:
						// Aggregate is "max"
						return compareScores(left, right, "gt")
					}
				}(
					member.Score*Score(setParams[0].Weight),
					setParams[1].Set.Get(member.Value).Score*Score(setParams[1].Weight),
				),
			}
			params = append(params, param)
		}
		// Traverse the params on the right sorted Set and add all the elements that are not
		// already contained in params with their respective weights applied.
		for _, member := range setParams[1].Set.GetAll() {
			if !slices.ContainsFunc(params, func(param MemberParam) bool {
				return param.Value == member.Value
			}) {
				params = append(params, MemberParam{
					Value: member.Value,
					Score: member.Score * Score(setParams[1].Weight),
				})
			}
		}
		return NewSortedSet(params)
	default:
		// Divide the sets into 2 and return the unions
		left := Union(aggregate, setParams[0:len(setParams)/2]...)
		right := Union(aggregate, setParams[len(setParams)/2:]...)

		var params []MemberParam
		// Traverse left sub-Set and add the union elements to params
		for _, member := range left.GetAll() {
			if !right.Contains(member.Value) {
				// If the right Set does not contain the current element, just add it to params
				params = append(params, member)
				continue
			}
			params = append(params, MemberParam{
				Value: member.Value,
				Score: func(left, right Score) Score {
					switch aggregate {
					case "sum":
						return left + right
					case "min":
						return compareScores(left, right, "lt")
					default:
						// Aggregate is "max"
						return compareScores(left, right, "gt")
					}
				}(member.Score, right.Get(member.Value).Score),
			})
		}
		// Traverse the right sub-Set and add any remaining elements to params
		for _, member := range right.GetAll() {
			if !slices.ContainsFunc(params, func(param MemberParam) bool {
				return param.Value == member.Value
			}) {
				params = append(params, member)
			}
		}
		return NewSortedSet(params)
	}
}

// Intersect uses divide & conquer to calculate the intersection of multiple sets
func Intersect(aggregate string, setParams ...SortedSetParam) *SortedSet {
	switch len(setParams) {
	case 0:
		return NewSortedSet([]MemberParam{})
	case 1:
		var params []MemberParam
		for _, member := range setParams[0].Set.GetAll() {
			params = append(params, MemberParam{
				Value: member.Value,
				Score: member.Score * Score(setParams[0].Weight),
			})
		}
		return NewSortedSet(params)
	case 2:
		var params []MemberParam
		// Traverse the params in the left sorted Set
		for _, member := range setParams[0].Set.GetAll() {
			// Check if the member Exists in the right sorted Set
			if !setParams[1].Set.Contains(member.Value) {
				continue
			}
			// If the member Exists, get both elements and apply the Weight
			param := MemberParam{
				Value: member.Value,
				Score: func(left, right Score) Score {
					// Choose which param to add to params depending on the aggregate
					switch aggregate {
					case "sum":
						return left + right
					case "min":
						return compareScores(left, right, "lt")
					default:
						// Aggregate is "max"
						return compareScores(left, right, "gt")
					}
				}(
					member.Score*Score(setParams[0].Weight),
					setParams[1].Set.Get(member.Value).Score*Score(setParams[1].Weight),
				),
			}
			params = append(params, param)
		}
		return NewSortedSet(params)
	default:
		// Divide the sets into 2 and return the intersection
		left := Intersect(aggregate, setParams[0:len(setParams)/2]...)
		right := Intersect(aggregate, setParams[len(setParams)/2:]...)

		var params []MemberParam
		for _, member := range left.GetAll() {
			if !right.Contains(member.Value) {
				continue
			}
			params = append(params, MemberParam{
				Value: member.Value,
				Score: func(left, right Score) Score {
					switch aggregate {
					case "sum":
						return left + right
					case "min":
						return compareScores(left, right, "lt")
					default:
						// Aggregate is "max"
						return compareScores(left, right, "gt")
					}
				}(member.Score, right.Get(member.Value).Score),
			})
		}

		return NewSortedSet(params)
	}
}
