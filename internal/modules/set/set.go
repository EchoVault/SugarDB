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

package set

import (
	"math/rand"
	"slices"
	"unsafe"

	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/constants"
)

type Set struct {
	members map[string]interface{}
	length  int
}

func (s *Set) GetMem() int64 {
	var size int64
	size += int64(unsafe.Sizeof(s))
	// above only gives us the size of the pointer to the map, so we need to add it's headers and contents
	size += int64(unsafe.Sizeof(s.members))
	for k, v := range s.members {
		size += int64(unsafe.Sizeof(k))
		size += int64(len(k))
		size += int64(unsafe.Sizeof(v))
	}

	return size
}

// compile time interface check
var _ constants.CompositeType = (*Set)(nil)

func NewSet(elems []string) *Set {
	set := &Set{
		members: make(map[string]interface{}),
		length:  0,
	}
	set.Add(elems)
	return set
}

func (set *Set) Add(elems []string) int {
	count := 0
	for _, e := range elems {
		if !set.Contains(e) {
			set.members[e] = struct{}{}
			count += 1
		}
	}
	set.length += count
	return count
}

func (set *Set) get(e string) interface{} {
	return set.members[e]
}

func (set *Set) GetAll() []string {
	var res []string
	for e, _ := range set.members {
		res = append(res, e)
	}
	return res
}

func (set *Set) Cardinality() int {
	return set.length
}

func (set *Set) GetRandom(count int) []string {
	keys := set.GetAll()

	if count == 0 {
		return []string{}
	}

	if internal.AbsInt(count) >= set.Cardinality() {
		return keys
	}

	res := []string{}

	var n int

	if count < 0 {
		// If count is negative, allow repeat elements
		for i := 0; i < internal.AbsInt(count); i++ {
			n = rand.Intn(len(keys))
			res = append(res, keys[n])
		}
	} else {
		// Count is positive, do not allow repeat elements
		for i := 0; i < internal.AbsInt(count); {
			n = rand.Intn(len(keys))
			if !slices.Contains(res, keys[n]) {
				res = append(res, keys[n])
				keys = slices.DeleteFunc(keys, func(elem string) bool {
					return elem == keys[n]
				})
				i++
			}
		}
	}

	return res
}

func (set *Set) Remove(elems []string) int {
	count := 0
	for _, e := range elems {
		if set.get(e) != nil {
			delete(set.members, e)
			count += 1
		}
	}
	set.length -= count
	return count
}

func (set *Set) Pop(count int) []string {
	keys := set.GetRandom(count)
	set.Remove(keys)
	return keys
}

func (set *Set) Contains(e string) bool {
	return set.get(e) != nil
}

// Subtract received a list of sets and finds the difference between sets provided
func (set *Set) Subtract(others []*Set) *Set {
	diff := NewSet(set.GetAll())
	var remove []string
	for _, s := range others {
		for k, _ := range s.members {
			if diff.Contains(k) {
				remove = append(remove, k)
			}
		}
	}
	diff.Remove(remove)
	return diff
}

func (set *Set) Move(destination *Set, e string) int {
	if !set.Contains(e) {
		return 0
	}
	set.Remove([]string{e})
	destination.Add([]string{e})
	return 1
}

// The Intersection accepts limit parameter of type int and a list of sets whose intersects are to be calculated.
// When limit is greater than 0, then the calculation will stop once the intersect cardinality reaches limit without
// calculating the full intersect.
func Intersection(limit int, sets ...*Set) (*Set, bool) {
	// Use divide & conquer to get the set intersections
	switch len(sets) {
	case 1:
		return sets[0], false
	case 2:
		intersection := NewSet([]string{})
		var limitReached bool
		for _, member := range sets[0].GetAll() {
			if limit > 0 && intersection.Cardinality() >= limit {
				limitReached = true
				break
			}
			if sets[1].Contains(member) {
				intersection.Add([]string{member})
			}
		}
		return intersection, limitReached
	default:
		left, stop := Intersection(limit, sets[0:len(sets)/2]...)
		if stop { // Check if limit is reached by left, if it is, return left
			return left, stop
		}
		right, stop := Intersection(limit, sets[len(sets)/2:]...)
		if stop { // Check if limit is reached by right, if it is, return right
			return right, stop
		}
		return Intersection(limit, left, right)
	}
}

// Union takes a slice of sets and generates a union
func Union(sets ...*Set) *Set {
	switch len(sets) {
	case 1:
		return sets[0]
	case 2:
		union := sets[0]
		union.Add(sets[1].GetAll())
		return union
	default:
		left := Union(sets[0 : len(sets)/2]...)
		right := Union(sets[len(sets)/2:]...)
		return Union(left, right)
	}
}
