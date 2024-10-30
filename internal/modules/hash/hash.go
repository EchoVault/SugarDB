package hash

import (
	"time"
	"unsafe"

	"github.com/echovault/sugardb/internal/constants"
)

type HashValue struct {
	Value    interface{}
	ExpireAt time.Time
}

type Hash map[string]HashValue

func (h *Hash) GetMem() int64 {

	var size int64
	// Map headers
	size += int64(unsafe.Sizeof(*h))

	for key, val := range *h {

		size += int64(unsafe.Sizeof(val.ExpireAt))
		size += int64(unsafe.Sizeof(key))
		size += int64(len(key))

		switch vt := val.Value.(type) {

		// AdaptType() will always ensure data type is of string, float64 or int.
		case nil:
			size += 0
		case int:
			size += int64(unsafe.Sizeof(vt))
		case float64, int64:
			size += 8
		case string:
			size += int64(unsafe.Sizeof(vt))
			size += int64(len(vt))
		}
	}
	return size
}

var _ constants.CompositeType = (*Hash)(nil)
