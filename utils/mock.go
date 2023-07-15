package utils

import (
	"bytes"
	"io"
	"sync"
)

type CustomWriter struct {
	Buf bytes.Buffer
}

func (cw *CustomWriter) Write(p []byte) (int, error) {
	count := 0

	for _, b := range p {
		cw.Buf.WriteByte(b)
		count += 1
	}

	if count != len(p) {
		return count, io.ErrShortWrite
	}

	return count, nil
}

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
