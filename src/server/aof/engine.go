package aof

import (
	"context"
	logstore "github.com/echovault/echovault/src/server/aof/log"
	"github.com/echovault/echovault/src/server/aof/preamble"
	"github.com/echovault/echovault/src/utils"
	"log"
	"net"
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
	options       Opts
	mut           sync.Mutex
	logChan       chan []byte
	logCount      uint64
	preambleStore *preamble.PreambleStore
	appendStore   *logstore.AppendStore
}

func NewAOFEngine(opts Opts, appendRW logstore.AppendReadWriter, preambleRW preamble.PreambleReadWriter) (*Engine, error) {
	engine := &Engine{
		options:  opts,
		mut:      sync.Mutex{},
		logChan:  make(chan []byte, 4096),
		logCount: 0,
	}

	// Setup Preamble engine
	engine.preambleStore = preamble.NewPreambleStore(
		preamble.WithDirectory(engine.options.Config.DataDir),
		preamble.WithReadWriter(preambleRW),
		preamble.WithGetStateFunc(opts.GetState),
		preamble.WithSetValueFunc(func(key string, value interface{}) {
			if _, err := engine.options.CreateKeyAndLock(context.Background(), key); err != nil {
				log.Println(err)
			}
			engine.options.SetValue(context.Background(), key, value)
			engine.options.KeyUnlock(key)
		}),
	)

	// Setup AOF log store engine
	engine.appendStore = logstore.NewAppendStore(
		logstore.WithDirectory(engine.options.Config.DataDir),
		logstore.WithStrategy(engine.options.Config.AOFSyncStrategy),
		logstore.WithReadWriter(appendRW),
		logstore.WithHandleCommandFunc(func(command []byte) {
			_, err := engine.options.HandleCommand(context.Background(), command, nil, true)
			if err != nil {
				log.Println(err)
			}
		}),
	)

	// 3. Start the goroutine to pick up queued commands in order to write them to the file.
	// LogCommand will get the open file handler from the struct top perform the AOF operation.
	go func() {
		for {
			c := <-engine.logChan
			if err := engine.appendStore.Write(c); err != nil {
				log.Println(err)
			}
		}
	}()

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

func (engine *Engine) Restore() error {
	if err := engine.preambleStore.Restore(); err != nil {
		log.Println(err)
	}
	if err := engine.appendStore.Restore(); err != nil {
		log.Println(err)
	}
	return nil
}
