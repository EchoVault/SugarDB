package aof

import (
	"fmt"
	logstore "github.com/echovault/echovault/src/aof/log"
	"github.com/echovault/echovault/src/aof/preamble"
	"log"
	"sync"
)

// This package handles AOF logging in standalone mode only.
// Logging in clusters is handled in the raft layer.

type Engine struct {
	syncStrategy string
	directory    string
	preambleRW   preamble.PreambleReadWriter
	appendRW     logstore.AppendReadWriter

	mut           sync.Mutex
	logChan       chan []byte
	logCount      uint64
	preambleStore *preamble.PreambleStore
	appendStore   *logstore.AppendStore

	startRewrite  func()
	finishRewrite func()
	getState      func() map[string]interface{}
	setValue      func(key string, value interface{})
	handleCommand func(command []byte)
}

func WithStrategy(strategy string) func(engine *Engine) {
	return func(engine *Engine) {
		engine.syncStrategy = strategy
	}
}

func WithDirectory(directory string) func(engine *Engine) {
	return func(engine *Engine) {
		engine.directory = directory
	}
}

func WithStartRewriteFunc(f func()) func(engine *Engine) {
	return func(engine *Engine) {
		engine.startRewrite = f
	}
}

func WithFinishRewriteFunc(f func()) func(engine *Engine) {
	return func(engine *Engine) {
		engine.finishRewrite = f
	}
}

func WithGetStateFunc(f func() map[string]interface{}) func(engine *Engine) {
	return func(engine *Engine) {
		engine.getState = f
	}
}

func WithSetValueFunc(f func(key string, value interface{})) func(engine *Engine) {
	return func(engine *Engine) {
		engine.setValue = f
	}
}

func WithHandleCommandFunc(f func(command []byte)) func(engine *Engine) {
	return func(engine *Engine) {
		engine.handleCommand = f
	}
}

func WithPreambleReadWriter(rw preamble.PreambleReadWriter) func(engine *Engine) {
	return func(engine *Engine) {
		engine.preambleRW = rw
	}
}

func WithAppendReadWriter(rw logstore.AppendReadWriter) func(engine *Engine) {
	return func(engine *Engine) {
		engine.appendRW = rw
	}
}

func NewAOFEngine(options ...func(engine *Engine)) *Engine {
	engine := &Engine{
		syncStrategy:  "everysec",
		directory:     "",
		mut:           sync.Mutex{},
		logChan:       make(chan []byte, 4096),
		logCount:      0,
		startRewrite:  func() {},
		finishRewrite: func() {},
		getState:      func() map[string]interface{} { return nil },
		setValue:      func(key string, value interface{}) {},
		handleCommand: func(command []byte) {},
	}

	for _, option := range options {
		option(engine)
	}

	// Setup Preamble engine
	engine.preambleStore = preamble.NewPreambleStore(
		preamble.WithDirectory(engine.directory),
		preamble.WithReadWriter(engine.preambleRW),
		preamble.WithGetStateFunc(engine.getState),
		preamble.WithSetValueFunc(engine.setValue),
	)

	// Setup AOF log store engine
	engine.appendStore = logstore.NewAppendStore(
		logstore.WithDirectory(engine.directory),
		logstore.WithStrategy(engine.syncStrategy),
		logstore.WithReadWriter(engine.appendRW),
		logstore.WithHandleCommandFunc(engine.handleCommand),
	)

	// 3. Start the goroutine to pick up queued commands in order to write them to the file.
	// LogCommand will get the open file handler from the struct top perform the AOF operation.
	go func() {
		for {
			c := <-engine.logChan
			if err := engine.appendStore.Write(c); err != nil {
				log.Println(fmt.Errorf("new aof engine error: %+v", err))
			}
		}
	}()

	return engine
}

func (engine *Engine) QueueCommand(command []byte) {
	engine.logChan <- command
}

func (engine *Engine) RewriteLog() error {
	engine.mut.Lock()
	defer engine.mut.Unlock()

	engine.startRewrite()
	defer engine.finishRewrite()

	// Create AOF preamble
	if err := engine.preambleStore.CreatePreamble(); err != nil {
		log.Println(fmt.Errorf("rewrite log -> create preamble error: %+v", err))
	}

	// Truncate the AOF file.
	if err := engine.appendStore.Truncate(); err != nil {
		log.Println(fmt.Errorf("rewrite log -> create aof error: %+v", err))
	}

	return nil
}

func (engine *Engine) Restore() error {
	if err := engine.preambleStore.Restore(); err != nil {
		log.Println(fmt.Errorf("restore aof -> restore preamble error: %+v", err))
	}
	if err := engine.appendStore.Restore(); err != nil {
		log.Println(fmt.Errorf("restore aof -> restore aof error: %+v", err))
	}
	return nil
}
