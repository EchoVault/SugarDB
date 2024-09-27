package eviction

type MemCheck interface {
	GetMem() int64
}
