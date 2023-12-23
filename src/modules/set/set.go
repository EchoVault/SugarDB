package set

import (
	"github.com/kelvinmwinuka/memstore/src/utils"
	"math/rand"
)

type Set struct {
	members map[string]interface{}
	length  int
}

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

func (set *Set) Get(e string) interface{} {
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

	if utils.AbsInt(count) >= set.Cardinality() {
		return keys
	}

	res := []string{}

	var n int

	if count < 0 {
		// If count is negative, allow repeat elements
		for i := 0; i < utils.AbsInt(count); i++ {
			n = rand.Intn(len(keys))
			res = append(res, keys[n])
		}
	} else {
		// Count is positive, do not allow repeat elements
		for i := 0; i < utils.AbsInt(count); i++ {
			n = rand.Intn(len(keys))
			if !utils.Contains(res, keys[n]) {
				res = append(res, keys[n])
				keys = utils.Filter(keys, func(elem string) bool {
					return elem != keys[n]
				})
			}
		}
	}

	return res
}

func (set *Set) Remove(elems []string) int {
	count := 0
	for _, e := range elems {
		if set.Get(e) != nil {
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
	return set.Get(e) != nil
}

func (set *Set) Union(others []*Set) *Set {
	union := NewSet(set.GetAll())
	for _, s := range others {
		for k, _ := range s.members {
			union.Add([]string{k})
		}
	}
	return union
}

func (set *Set) Intersection(others []*Set, limit int) *Set {
	intersection := NewSet([]string{})
	for sIdx, s := range others {
		if sIdx == 0 {
			for _, e := range s.GetAll() {
				if limit > 0 && intersection.Cardinality() == limit {
					return intersection
				}
				if set.Contains(e) {
					intersection.Add([]string{e})
				}
			}
			continue
		}
		for _, e := range s.GetAll() {
			if limit > 0 && intersection.Cardinality() == limit {
				return intersection
			}
			if !intersection.Contains(e) {
				intersection.Remove([]string{e})
			} else {
				intersection.Add([]string{e})
			}
		}
	}
	return intersection
}

func (set *Set) Subtract(others []*Set) *Set {
	diff := NewSet(set.GetAll())
	remove := []string{}
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
