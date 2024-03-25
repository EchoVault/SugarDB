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

package aof

import (
	"fmt"
	logstore "github.com/echovault/echovault/src/aof/log"
	"github.com/echovault/echovault/src/aof/preamble"
	"github.com/echovault/echovault/src/utils"
	"log"
	"sync"
)

// This package handles AOF logging in standalone mode only.
// Logging in replication clusters is handled in the raft layer.

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

	startRewriteFunc  func()
	finishRewriteFunc func()
	getStateFunc      func() map[string]utils.KeyData
	setKeyDataFunc    func(key string, data utils.KeyData)
	handleCommand     func(command []byte)
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
		engine.startRewriteFunc = f
	}
}

func WithFinishRewriteFunc(f func()) func(engine *Engine) {
	return func(engine *Engine) {
		engine.finishRewriteFunc = f
	}
}

func WithGetStateFunc(f func() map[string]utils.KeyData) func(engine *Engine) {
	return func(engine *Engine) {
		engine.getStateFunc = f
	}
}

func WithSetKeyDataFunc(f func(key string, data utils.KeyData)) func(engine *Engine) {
	return func(engine *Engine) {
		engine.setKeyDataFunc = f
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
		syncStrategy:      "everysec",
		directory:         "",
		mut:               sync.Mutex{},
		logChan:           make(chan []byte, 4096),
		logCount:          0,
		startRewriteFunc:  func() {},
		finishRewriteFunc: func() {},
		getStateFunc:      func() map[string]utils.KeyData { return nil },
		setKeyDataFunc:    func(key string, data utils.KeyData) {},
		handleCommand:     func(command []byte) {},
	}

	// Setup Preamble engine
	engine.preambleStore = preamble.NewPreambleStore(
		preamble.WithDirectory(engine.directory),
		preamble.WithReadWriter(engine.preambleRW),
		preamble.WithGetStateFunc(engine.getStateFunc),
		preamble.WithSetKeyDataFunc(engine.setKeyDataFunc),
	)

	// Setup AOF log store engine
	engine.appendStore = logstore.NewAppendStore(
		logstore.WithDirectory(engine.directory),
		logstore.WithStrategy(engine.syncStrategy),
		logstore.WithReadWriter(engine.appendRW),
		logstore.WithHandleCommandFunc(engine.handleCommand),
	)

	for _, option := range options {
		option(engine)
	}

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

	engine.startRewriteFunc()
	defer engine.finishRewriteFunc()

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
