package eviction

import (
	"container/heap"
	"slices"
)

type EntryLFU struct {
	key   string // The key, matching the key in the store
	count int    // The number of times this key has been accessed
	index int    // The index of the entry in the heap
}

type Cache []*EntryLFU

func (cache *Cache) Len() int {
	return len(*cache)
}

func (cache *Cache) Less(i, j int) bool {
	return (*cache)[i].count > (*cache)[j].count
}

func (cache *Cache) Swap(i, j int) {
	(*cache)[i], (*cache)[j] = (*cache)[j], (*cache)[i]
	(*cache)[i].index = i
	(*cache)[j].index = j
}

func (cache *Cache) Push(key any) {
	n := len(*cache)
	*cache = append(*cache, &EntryLFU{
		key:   key.(string),
		count: 1,
		index: n,
	})
}

func (cache *Cache) Pop() any {
	old := *cache
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*cache = old[0 : n-1]
	return entry.key
}

func (cache *Cache) Update(key string) {
	// Get the item with key
	entryIdx := slices.IndexFunc(*cache, func(e *EntryLFU) bool {
		return e.key == key
	})
	entry := (*cache)[entryIdx]
	entry.count += 1
	heap.Fix(cache, entryIdx)
}
