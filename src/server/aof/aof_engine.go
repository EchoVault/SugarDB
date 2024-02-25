package aof

import (
	"context"
	"github.com/echovault/echovault/src/utils"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"
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
	options       Opts
	mut           sync.Mutex
	logChan       chan []byte
	logCount      uint64
	preambleStore *PreambleStore
	appendStore   AppendStore
}

func NewAOFEngine(opts Opts, appendRW io.ReadWriter, preambleRW io.ReadWriter) (*Engine, error) {
	engine := &Engine{
		options:  opts,
		mut:      sync.Mutex{},
		logChan:  make(chan []byte, 4096),
		logCount: 0,
	}

	// Obtain preamble file handler
	if preambleRW == nil {
		f, err := os.OpenFile(
			path.Join(engine.options.Config.DataDir, "aof", "preamble.bin"),
			os.O_WRONLY|os.O_CREATE|os.O_APPEND,
			os.ModePerm)
		if err != nil {
			return nil, err
		}
		preambleRW = f
	}

	// Setup Preamble engine
	engine.preambleStore = NewPreambleStore(
		WithReadWriter(preambleRW),
		WithGetStateFunc(opts.GetState),
		WithSetValueFunc(func(key string, value interface{}) {
			if _, err := engine.options.CreateKeyAndLock(context.Background(), key); err != nil {
				log.Println(err)
			}
			engine.options.SetValue(context.Background(), key, value)
			engine.options.KeyUnlock(key)
		}),
	)

	// 1. Create AOF directory if it does not exist.
	if err := os.MkdirAll(path.Join(engine.options.Config.DataDir, "aof"), os.ModePerm); err != nil {
		return nil, err
	}

	// 2. Setup storage engine.
	engine.appendStore = AppendStore{
		rw:  appendRW,
		mut: sync.Mutex{},
	}

	// If out is not provided by the caller, then create/open the new AOF file based on the configuration.
	if appendRW == nil {
		f, err := os.OpenFile(
			path.Join(engine.options.Config.DataDir, "aof", "log.aof"),
			os.O_WRONLY|os.O_CREATE|os.O_APPEND,
			os.ModePerm)
		if err != nil {
			return nil, err
		}
		engine.appendStore.rw = f
	}

	// 3. Start the goroutine to pick up queued commands in order to write them to the file.
	// LogCommand will get the open file handler from the struct top perform the AOF operation.
	go func() {
		for {
			c := <-engine.logChan
			if err := engine.appendStore.Write(c); err != nil {
				log.Println(err)
			}
			if strings.EqualFold(engine.options.Config.AOFSyncStrategy, "always") {
				if err := engine.appendStore.Sync(); err != nil {
					log.Println(err)
				}
			}
		}
	}()

	// 4. Start another goroutine that takes handles syncing the content to the file system.
	// No need to start this goroutine if sync strategy is anything other than 'everysec'.
	if strings.EqualFold(engine.options.Config.AOFSyncStrategy, "everysec") {
		go func() {
			for {
				if err := engine.appendStore.Sync(); err != nil {
					log.Println(err)
				}
				<-time.After(1 * time.Second)
			}
		}()
	}

	return engine, nil
}

func (engine *Engine) QueueCommand(command []byte) {
	engine.logChan <- command
}

func (engine *Engine) RewriteLog() error {
	engine.mut.Lock()
	defer engine.mut.Unlock()

	engine.options.StartRewriteAOF()
	defer engine.options.FinishRewriteAOF()

	// Create AOF preamble
	if err := engine.preambleStore.CreatePreamble(); err != nil {
		log.Println(err)
	}

	// Truncate the AOF file.
	if err := engine.appendStore.Truncate(); err != nil {
		log.Println(err)
	}

	return nil
}

func (engine *Engine) Restore(ctx context.Context) error {
	if err := engine.preambleStore.Restore(ctx); err != nil {
		log.Println(err)
	}
	if err := engine.appendStore.Restore(ctx); err != nil {
		log.Println(err)
	}
	return nil
}
