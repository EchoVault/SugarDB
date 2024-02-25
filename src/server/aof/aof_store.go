package aof

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
)

type AppendStore struct {
	rw            io.ReadWriter
	mut           sync.Mutex
	handleCommand func(ctx context.Context, command []byte, conn *net.Conn, replay bool) ([]byte, error)
}

func NewAppendStore() AppendStore {
	return AppendStore{}
}

func (store *AppendStore) Write(command []byte) error {
	store.mut.Lock()
	defer store.mut.Unlock()
	_, err := store.rw.Write(command)
	return err
}

func (store *AppendStore) Sync() error {
	store.mut.Lock()
	store.mut.Unlock()
	file, ok := store.rw.(*os.File)
	if ok {
		return file.Sync()
	}
	return nil
}

func (store *AppendStore) Restore(ctx context.Context) error {
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
		if _, err := store.handleCommand(ctx, c, nil, true); err != nil {
			return err
		}
	}

	return nil
}

func (store *AppendStore) Truncate() error {
	return nil
}

func (store *AppendStore) Close() error {
	store.mut.Lock()
	defer store.mut.Unlock()
	file, ok := store.rw.(*os.File)
	if !ok {
		return nil
	}
	return file.Close()
}
