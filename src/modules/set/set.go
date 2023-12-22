package set

import (
	"math/rand"
)

type Set struct {
	members map[string]interface{}
}

func NewSet(elems []string) *Set {
	set := &Set{
		members: make(map[string]interface{}),
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
	return count
}

func (set *Set) Get(e string) interface{} {
	return set.members[e]
}

func (set *Set) GetRandom(count int) []string {
	keys := []string{}
	for k, _ := range set.members {
		keys = append(keys, k)
	}

	res := []string{}

	var n int

	if count > 1 {
		for i := 0; i < count; i++ {
			n = rand.Intn(len(keys))
			res = append(res, keys[n])
		}
	} else {
		n = rand.Intn(len(keys))
		res = append(res, keys[n])
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

func (set *Set) Union(others []*Set) Set {
	union := *set
	for _, s := range others {
		for k, _ := range s.members {
			union.Add([]string{k})
		}
	}
	return union
}

func (set *Set) Intersection(others []*Set) Set {
	intersection := *set
	remove := []string{}
	for _, s := range others {
		for k, _ := range s.members {
			if !intersection.Contains(k) {
				remove = append(remove, k)
			}
		}
	}
	intersection.Remove(remove)
	return intersection
}

func (set *Set) Subtract(others []*Set) Set {
	diff := *set
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
	if set.Get(e) == nil {
		return 0
	}
	set.Remove([]string{e})
	destination.Add([]string{e})
	return 1
}