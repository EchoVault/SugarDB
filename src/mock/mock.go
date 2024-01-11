package mock

import (
	"sync"
)

type Data struct {
	Mu   sync.Mutex
	Data map[string]interface{}
}

type Server struct {
	Data Data
}

func (server *Server) Lock() {
	server.Data.Mu.Lock()
}

func (server *Server) Unlock() {
	server.Data.Mu.Unlock()
}

func (server *Server) GetData(key string) interface{} {
	return server.Data.Data[key]
}

func (server *Server) SetData(key string, value interface{}) {
	server.Data.Data[key] = value
}
