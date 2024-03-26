// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package preamble

import (
	"encoding/json"
	"fmt"
	"github.com/echovault/echovault/pkg/utils"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

type PreambleReadWriter interface {
	io.ReadWriteSeeker
	io.Closer
	Truncate(size int64) error
	Sync() error
}

type PreambleStore struct {
	rw             PreambleReadWriter
	mut            sync.Mutex
	directory      string
	getStateFunc   func() map[string]utils.KeyData
	setKeyDataFunc func(key string, data utils.KeyData)
}

func WithReadWriter(rw PreambleReadWriter) func(store *PreambleStore) {
	return func(store *PreambleStore) {
		store.rw = rw
	}
}

func WithGetStateFunc(f func() map[string]utils.KeyData) func(store *PreambleStore) {
	return func(store *PreambleStore) {
		store.getStateFunc = f
	}
}

func WithSetKeyDataFunc(f func(key string, data utils.KeyData)) func(store *PreambleStore) {
	return func(store *PreambleStore) {
		store.setKeyDataFunc = f
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
		getStateFunc: func() map[string]utils.KeyData {
			// No-Op by default
			return nil
		},
		setKeyDataFunc: func(key string, data utils.KeyData) {},
	}

	for _, option := range options {
		option(store)
	}

	// If rw is nil, create the default
	if store.rw == nil && store.directory != "" {
		err := os.MkdirAll(path.Join(store.directory, "aof"), os.ModePerm)
		if err != nil {
			log.Println(fmt.Errorf("new preamble store -> mkdir error: %+v", err))
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
	state := store.filterExpiredKeys(store.getStateFunc())
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

	state := make(map[string]utils.KeyData)

	if err = json.Unmarshal(b, &state); err != nil {
		return err
	}

	for key, data := range store.filterExpiredKeys(state) {
		store.setKeyDataFunc(key, data)
	}

	return nil
}

func (store *PreambleStore) Close() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	return store.rw.Close()
}

// filterExpiredKeys filters out keys that are already expired, so they are not persisted.
func (store *PreambleStore) filterExpiredKeys(state map[string]utils.KeyData) map[string]utils.KeyData {
	var keysToDelete []string
	for k, v := range state {
		if v.ExpireAt.Before(time.Now()) {
			keysToDelete = append(keysToDelete, k)
		}
	}
	for _, key := range keysToDelete {
		delete(state, key)
	}
	return state
}
