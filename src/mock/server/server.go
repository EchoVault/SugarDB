package server

import (
	"sync"
)

type Server struct {
	store           map[string]interface{}
	keyLocks        map[string]*sync.RWMutex
	keyCreationLock *sync.Mutex
}
