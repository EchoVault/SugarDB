package set

type Set struct {
	members map[interface{}]interface{}
}

func NewSet(elems []interface{}) *Set {
	set := &Set{
		members: make(map[interface{}]interface{}),
	}
	for _, e := range elems {
		set.members[e] = struct{}{}
	}
	return set
}

func (set *Set) Add(elems []interface{}) int {
	count := 0
	for _, e := range elems {
		if set.members[e] == nil {
			set.members[e] = struct{}{}
			count += 1
		}
	}
	return count
}

func (set *Set) Remove(elems []interface{}) int {
	count := 0
	for _, e := range elems {
		if set.members[e] != nil {
			delete(set.members, e)
			count += 1
		}
	}
	return count
}

func (set *Set) Pop(count int) []interface{} {
	return []interface{}{}
}

func (set *Set) Contains(v interface{}) bool {
	return set.members[v] != nil
}

func (set *Set) Union(others []*Set) Set {
	union := *set
	for _, s := range others {
		for k, _ := range s.members {
			union.Add([]interface{}{k})
		}
	}
	return union
}

func (set *Set) Intersection(others []*Set) Set {
	intersection := *set
	remove := []interface{}{}
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
	remove := []interface{}{}
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

func (set *Set) Move(destination *Set, v interface{}) int {
	if set.members[v] == nil {
		return 0
	}
	set.Remove([]interface{}{v})
	destination.Add([]interface{}{v})
	return 1
}
