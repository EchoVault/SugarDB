package server

import (
	"sync"
)

type Server struct {
	store           map[string]interface{}
	keyLocks        map[string]*sync.RWMutex
	keyCreationLock *sync.Mutex
}

func NewMockServer() *Server {
	return &Server{
		store:           make(map[string]interface{}),
		keyLocks:        make(map[string]*sync.RWMutex),
		keyCreationLock: &sync.Mutex{},
	}
}

func (server *Server) TakeSnapshot() error {
	return nil
}

func (server *Server) StartSnapshot() {
	// No-Op
}

func (server *Server) FinishSnapshot() {
	// No-Op
}

func (server *Server) SetLatestSnapshot(msec int64) {
	// No-Op
}

func (server *Server) GetLatestSnapshot() int64 {
	return 0
}

func (server *Server) StartRewriteAOF() {
	// No-Op
}

func (server *Server) FinishRewriteAOF() {
	// No-Op
}

func (server *Server) RewriteAOF() error {
	return nil
}
