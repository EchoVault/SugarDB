package aof

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/echovault/echovault/src/utils"
	"io"
	"log"
	"net"
	"os"
	"path"
	"sync"
)

// This package handles AOF logging in standalone mode only.
// Logging in clusters is handled in the raft layer.

type Opts struct {
	Config           utils.Config
	GetState         func() map[string]interface{}
	StartRewriteAOF  func()
	FinishRewriteAOF func()
	CreateKeyAndLock func(ctx context.Context, key string) (bool, error)
	KeyUnlock        func(key string)
	SetValue         func(ctx context.Context, key string, value interface{})
	HandleCommand    func(ctx context.Context, command []byte, conn *net.Conn, replay bool) ([]byte, error)
}

type Engine struct {
	options  Opts
	mut      sync.Mutex
	logChan  chan []byte
	logCount uint64
}

func NewAOFEngine(opts Opts) *Engine {
	return &Engine{
		options:  opts,
		mut:      sync.Mutex{},
		logChan:  make(chan []byte, 4096),
		logCount: 0,
	}
}

func (engine *Engine) Start(ctx context.Context) {
	go func() {
		for {
			c := <-engine.logChan
			if err := engine.LogCommand(c); err != nil {
				log.Println(err)
				continue
			}
		}
	}()
}

func (engine *Engine) QueueCommand(command []byte) {
	engine.logChan <- command
}

func (engine *Engine) LogCommand(command []byte) error {
	engine.mut.Lock()
	defer engine.mut.Unlock()

	err := os.MkdirAll(path.Join(engine.options.Config.DataDir, "aof"), os.ModePerm)
	if err != nil {
		return err
	}

	// Open aof file (create it if it does not exist).
	f, err := os.OpenFile(
		path.Join(engine.options.Config.DataDir, "aof", "log.aof"),
		os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		os.ModePerm)
	if err != nil {
		return err
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	// Append command to aof file.
	if _, err := f.Write(command); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		log.Println(err)
	}

	return nil
}

func (engine *Engine) RewriteLog() error {
	engine.mut.Lock()
	defer engine.mut.Unlock()

	engine.options.StartRewriteAOF()
	defer engine.options.FinishRewriteAOF()

	// Get current state.
	state := engine.options.GetState()
	o, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Replace snapshot contents file with current state.
	sf, err := os.Create(path.Join(engine.options.Config.DataDir, "aof", "snapshot.bin"))
	if err != nil {
		return err
	}
	defer func() {
		if err = sf.Close(); err != nil {
			log.Println(err)
		}
	}()
	if _, err = sf.Write(o); err != nil {
		return err
	}
	if err = sf.Sync(); err != nil {
		log.Println(err)
	}

	// Replace aof file with empty file.
	aof, err := os.Create(path.Join(engine.options.Config.DataDir, "aof", "log.aof"))
	if err != nil {
		return err
	}
	defer func() {
		if err = aof.Close(); err != nil {
			log.Println(err)
		}
	}()

	return nil
}

func (engine *Engine) RestoreSnapshot(ctx context.Context) error {
	sf, err := os.Open(path.Join(engine.options.Config.DataDir, "aof", "snapshot.bin"))
	if err != nil {
		return err
	}

	b, err := io.ReadAll(sf)
	if err != nil {
		return err
	}

	state := make(map[string]interface{})

	if err = json.Unmarshal(b, &state); err != nil {
		return err
	}

	for key, value := range state {
		if _, err = engine.options.CreateKeyAndLock(ctx, key); err != nil {
			log.Println(err)
		}
		engine.options.SetValue(ctx, key, value)
		engine.options.KeyUnlock(key)
	}

	return nil
}

func (engine *Engine) RestoreAOF(ctx context.Context) error {
	aof, err := os.Open(path.Join(engine.options.Config.DataDir, "aof", "log.aof"))
	if err != nil {
		log.Println(err)
	}

	buf := bufio.NewReader(aof)

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
		if _, err = engine.options.HandleCommand(ctx, c, nil, true); err != nil {
			return err
		}
	}

	return nil
}

func (engine *Engine) Restore(ctx context.Context) error {
	if err := engine.RestoreSnapshot(ctx); err != nil {
		log.Println(err)
	}
	if err := engine.RestoreAOF(ctx); err != nil {
		log.Println(err)
	}
	return nil
}
