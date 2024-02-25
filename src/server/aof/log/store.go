package log

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type AppendReadWriter interface {
	io.ReadWriter
	io.Closer
	Truncate(size int64) error
	Sync() error
}

type AppendStore struct {
	strategy      string               // Append file sync strategy. Can only be "always", "everysec", or "no
	mut           sync.Mutex           // Store mutex
	rw            AppendReadWriter     // The ReadWriter used to persist and load the log
	directory     string               // The directory for the AOF file if we must create one
	handleCommand func(command []byte) // Function to handle command read from AOF log after restore
}

func WithStrategy(strategy string) func(store *AppendStore) {
	return func(store *AppendStore) {
		store.strategy = strategy
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

func NewAppendStore(options ...func(store *AppendStore)) *AppendStore {
	store := &AppendStore{
		directory: "",
		strategy:  "everysec",
		rw:        nil,
		mut:       sync.Mutex{},
		handleCommand: func(command []byte) {
			// No-Op
		},
	}

	for _, option := range options {
		option(store)
	}

	// If rw is nil, use a default file at the provided directory
	if store.rw == nil {
		f, err := os.OpenFile(path.Join(store.directory, "aof", "log.aof"), os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
		store.rw = f
	}

	// Start another goroutine that takes handles syncing the content to the file system.
	// No need to start this goroutine if sync strategy is anything other than 'everysec'.
	if strings.EqualFold(store.strategy, "everysec") {
		go func() {
			for {
				if err := store.Sync(); err != nil {
					log.Println(err)
				}
				<-time.After(1 * time.Second)
			}
		}()
	}
	return store
}

func (store *AppendStore) Write(command []byte) error {
	store.mut.Lock()
	defer store.mut.Unlock()
	if _, err := store.rw.Write(command); err != nil {
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
	store.mut.Lock()
	store.mut.Unlock()
	return store.rw.Sync()
}

func (store *AppendStore) Restore() error {
	store.mut.Lock()
	defer store.mut.Unlock()

	buf := bufio.NewReader(store.rw)

	var commands [][]byte
	var line []byte

	for {
		b, _, err := buf.ReadLine()
		if err != nil && errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if len(b) <= 0 {
			line = append(line, []byte("\r\n\r\n")...)
			commands = append(commands, line)
			line = []byte{}
			continue
		}
		if len(line) > 0 {
			line = append(line, append([]byte("\r\n"), bytes.TrimLeft(b, "\x00")...)...)
			continue
		}
		line = append(line, bytes.TrimLeft(b, "\x00")...)
	}

	for _, c := range commands {
		store.handleCommand(c)
	}

	return nil
}

func (store *AppendStore) Truncate() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	if err := store.rw.Truncate(0); err != nil {
		return err
	}
	return nil
}

func (store *AppendStore) Close() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	return store.rw.Close()
}
