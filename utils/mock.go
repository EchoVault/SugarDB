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
	mu   sync.Mutex
	data map[string]interface{}
}

type MockServer struct {
	data MockData
}

func (server *MockServer) Lock() {
	server.data.mu.Lock()
}

func (server *MockServer) Unlock() {
	server.data.mu.Unlock()
}

func (server *MockServer) GetData(key string) interface{} {
	return server.data.data[key]
}

func (server *MockServer) SetData(key string, value interface{}) {
	server.data.data[key] = value
}
