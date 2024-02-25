package aof

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
)

type PreambleStore struct {
	rw       io.ReadWriter
	mut      sync.Mutex
	getState func() map[string]interface{}
	setValue func(key string, value interface{})
}

func WithReadWriter(rw io.ReadWriter) func(store *PreambleStore) {
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

func NewPreambleStore(options ...func(store *PreambleStore)) *PreambleStore {
	store := &PreambleStore{
		rw:  nil,
		mut: sync.Mutex{},
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

	if _, err = store.rw.Write(o); err != nil {
		return err
	}

	// If the rw is a file, sync it immediately
	file, ok := store.rw.(*os.File)
	if ok {
		if err = file.Sync(); err != nil {
			log.Println(err)
		}
	}

	return nil
}

func (store *PreambleStore) Restore(ctx context.Context) error {
	if store.rw == nil {
		return nil
	}

	b, err := io.ReadAll(store.rw)
	if err != nil {
		return err
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
