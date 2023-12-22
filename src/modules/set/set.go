package set

type Set struct {
	members map[interface{}]int
}

func NewSet(elems []interface{}) *Set {
	return &Set{}
}

func (set *Set) Add(elems []interface{}) int {
	return 0
}

func (set *Set) Remove(elems interface{}) int {
	return 0
}

func (set *Set) Pop(count int) []interface{} {
	return []interface{}{}
}

func (set *Set) Contains(v interface{}) bool {
	return false
}

func (set *Set) Union(other []*Set) Set {
	return Set{}
}

func (set *Set) Intersection(other []*Set) Set {
	return Set{}
}

func (set *Set) Subtract(other []*Set) Set {
	return Set{}
}

func (set *Set) Move(other *Set) bool {
	return false
}
