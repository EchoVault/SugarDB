package utils

const (
	AdminCategory       = "admin"
	BitmapCategory      = "bitmap"
	BlockingCategory    = "blocking"
	ConnectionCategory  = "connection"
	DangerousCategory   = "dangerous"
	GeoCategory         = "geo"
	HashCategory        = "hash"
	HyperLogLogCategory = "hyperloglog"
	FastCategory        = "fast"
	KeyspaceCategory    = "keyspace"
	ListCategory        = "list"
	PubSubCategory      = "pubsub"
	ReadCategory        = "read"
	ScriptingCategory   = "scripting"
	SetCategory         = "set"
	SortedSetCategory   = "sortedset"
	SlowCategory        = "slow"
	StreamCategory      = "stream"
	StringCategory      = "string"
	TransactionCategory = "transaction"
	WriteCategory       = "write"
)

const (
	OkResponse        = "+OK\r\n"
	WrongArgsResponse = "wrong number of arguments"
)

const (
	NoEviction     = "noeviction"
	AllKeysLRU     = "allkeys-lru"
	AllKeysLFU     = "allkeys-lfu"
	VolatileLRU    = "volatile-lru"
	VolatileLFU    = "volatile-lfu"
	AllKeysRandom  = "allkeys-random"
	VolatileRandom = "volatile-random"
	VolatileTTL    = "volatile-ttl"
)
