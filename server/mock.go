package main

import (
	"sync"
)

type MockData struct {
	Mu   sync.Mutex
	Data map[string]interface{}
}

type MockServer struct {
	Data MockData
}

func (server *MockServer) Lock() {
	server.Data.Mu.Lock()
}

func (server *MockServer) Unlock() {
	server.Data.Mu.Unlock()
}

func (server *MockServer) GetData(key string) interface{} {
	return server.Data.Data[key]
}

func (server *MockServer) SetData(key string, value interface{}) {
	server.Data.Data[key] = value
}
