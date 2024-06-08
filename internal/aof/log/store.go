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

package log

import (
	"fmt"
	"github.com/echovault/echovault/internal/clock"
	"github.com/tidwall/resp"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type AppendReadWriter interface {
	io.ReadWriteSeeker
	io.Closer
	Truncate(size int64) error
	Sync() error
}

type AppendStore struct {
	clock         clock.Clock
	strategy      string               // Append file sync strategy. Can only be "always", "everysec", or "no"
	mut           sync.Mutex           // Store mutex
	rw            AppendReadWriter     // The ReadWriter used to persist and load the log
	directory     string               // The directory for the AOF file if we must create one
	handleCommand func(command []byte) // Function to handle command read from AOF log after restore
}

func WithClock(clock clock.Clock) func(store *AppendStore) {
	return func(store *AppendStore) {
		store.clock = clock
	}
}

func WithStrategy(strategy string) func(store *AppendStore) {
	return func(store *AppendStore) {
		store.strategy = strings.ToLower(strategy)
	}
}

func WithReadWriter(rw AppendReadWriter) func(store *AppendStore) {
	return func(store *AppendStore) {
		store.rw = rw
	}
}

func WithDirectory(directory string) func(store *AppendStore) {
	return func(store *AppendStore) {
		store.directory = directory
	}
}

func WithHandleCommandFunc(f func(command []byte)) func(store *AppendStore) {
	return func(store *AppendStore) {
		store.handleCommand = f
	}
}

func NewAppendStore(options ...func(store *AppendStore)) (*AppendStore, error) {
	store := &AppendStore{
		clock:         clock.NewClock(),
		directory:     "",
		strategy:      "everysec",
		rw:            nil,
		mut:           sync.Mutex{},
		handleCommand: func(command []byte) {},
	}

	for _, option := range options {
		option(store)
	}

	// If rw is nil, use a default file at the provided directory
	if store.rw == nil && store.directory != "" {
		// Create the directory if it does not exist
		err := os.MkdirAll(path.Join(store.directory, "aof"), os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("new append store -> mkdir error: %+v", err)
		}
		f, err := os.OpenFile(path.Join(store.directory, "aof", "log.aof"), os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("new append store -> open file error: %+v", err)
		}
		store.rw = f
	}

	// Start another goroutine that takes handles syncing the content to the file system.
	// No need to start this goroutine if sync strategy is anything other than 'everysec'.
	if strings.EqualFold(store.strategy, "everysec") {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer func() {
				ticker.Stop()
			}()
			for {
				store.mut.Lock()
				if err := store.Sync(); err != nil {
					store.mut.Unlock()
					log.Println(fmt.Errorf("new append store error: %+v", err))
					break
				}
				store.mut.Unlock()
				<-ticker.C
			}
		}()
	}

	return store, nil
}

func (store *AppendStore) Write(command []byte) error {
	// Skip operation if ReadWriter is not defined
	if store.rw == nil {
		return nil
	}

	store.mut.Lock()
	defer store.mut.Unlock()

	// Add new line before writing to AOF file.
	out := append(command, []byte("\r\n")...)
	if _, err := store.rw.Write(out); err != nil {
		return err
	}

	if strings.EqualFold(store.strategy, "always") {
		if err := store.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (store *AppendStore) Sync() error {
	if store.rw != nil {
		return store.rw.Sync()
	}
	return nil
}

func (store *AppendStore) Restore() error {
	store.mut.Lock()
	defer store.mut.Unlock()

	// Move cursor to the beginning of the file
	if _, err := store.rw.Seek(0, 0); err != nil {
		return fmt.Errorf("restore aof: %v", err)
	}

	r := resp.NewReader(store.rw)
	for {
		value, n, err := r.ReadValue()
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			// Break out when there are no more bytes to read
			break
		}
		command, err := value.MarshalRESP()
		if err != nil {
			return err
		}
		store.handleCommand(command)
	}

	return nil
}

func (store *AppendStore) Truncate() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	if err := store.rw.Truncate(0); err != nil {
		return err
	}
	// Seek to the beginning of the file after truncating
	if _, err := store.rw.Seek(0, 0); err != nil {
		return err
	}
	return nil
}

func (store *AppendStore) Close() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	return store.rw.Close()
}
