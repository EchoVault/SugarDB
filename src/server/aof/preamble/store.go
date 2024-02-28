package preamble

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

type PreambleReadWriter interface {
	io.ReadWriteSeeker
	io.Closer
	Truncate(size int64) error
	Sync() error
}

type PreambleStore struct {
	rw        PreambleReadWriter
	mut       sync.Mutex
	directory string
	getState  func() map[string]interface{}
	setValue  func(key string, value interface{})
}

func WithReadWriter(rw PreambleReadWriter) func(store *PreambleStore) {
	return func(store *PreambleStore) {
		store.rw = rw
	}
}

func WithGetStateFunc(f func() map[string]interface{}) func(store *PreambleStore) {
	return func(store *PreambleStore) {
		store.getState = f
	}
}

func WithSetValueFunc(f func(key string, value interface{})) func(store *PreambleStore) {
	return func(store *PreambleStore) {
		store.setValue = f
	}
}

func WithDirectory(directory string) func(store *PreambleStore) {
	return func(store *PreambleStore) {
		store.directory = directory
	}
}

func NewPreambleStore(options ...func(store *PreambleStore)) *PreambleStore {
	store := &PreambleStore{
		rw:        nil,
		mut:       sync.Mutex{},
		directory: "",
		getState: func() map[string]interface{} {
			// No-Op by default
			return nil
		},
		setValue: func(key string, value interface{}) {
			// No-Op by default
		},
	}

	for _, option := range options {
		option(store)
	}

	// If rw is nil, create the default
	if store.rw == nil {
		err := os.MkdirAll(path.Join(store.directory, "aof"), os.ModePerm)
		if err != nil {
			log.Println(fmt.Errorf("new preamle store -> mkdir error: %+v", err))
		}
		f, err := os.OpenFile(path.Join(store.directory, "aof", "preamble.bin"), os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			log.Println(fmt.Errorf("new preamble store -> open file error: %+v", err))
		}
		store.rw = f
	}

	return store
}

func (store *PreambleStore) CreatePreamble() error {
	store.mut.Lock()
	store.mut.Unlock()

	// Get current state.
	state := store.getState()
	o, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Truncate the preamble first
	if err = store.rw.Truncate(0); err != nil {
		return err
	}
	// Seek to the beginning of the file after truncating
	if _, err = store.rw.Seek(0, 0); err != nil {
		return err
	}

	if _, err = store.rw.Write(o); err != nil {
		return err
	}

	// Sync the changes
	if err = store.rw.Sync(); err != nil {
		return err
	}

	return nil
}

func (store *PreambleStore) Restore() error {
	if store.rw == nil {
		return nil
	}

	b, err := io.ReadAll(store.rw)
	if err != nil {
		return err
	}

	if len(b) <= 0 {
		return nil
	}

	state := make(map[string]interface{})

	if err = json.Unmarshal(b, &state); err != nil {
		return err
	}

	for key, value := range state {
		store.setValue(key, value)
	}

	return nil
}

func (store *PreambleStore) Close() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	return store.rw.Close()
}
