package utils

import "sync"

type MockServer struct {
	Data sync.Map
}

func (server *MockServer) GetData(key string) interface{} {
	value, ok := server.Data.Load(key)

	if !ok {
		return nil
	}

	return value
}

func (server *MockServer) SetData(key string, value interface{}) {
	server.Data.Store(key, value)
}
