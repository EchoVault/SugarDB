package aof

import (
	"github.com/echovault/echovault/src/utils"
	"log"
	"os"
	"path"
	"sync"
)

// This package handles AOF logging in standalone mode only.
// Logging in clusters is handled in the raft layer.

type Opts struct {
	Config utils.Config
}

type Engine struct {
	options Opts
	mut     sync.Mutex
}

func NewAOFEngine(opts Opts) *Engine {
	return &Engine{
		options: opts,
		mut:     sync.Mutex{},
	}
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

	return nil
}

func (engine *Engine) RewriteLog() error {
	// Get current state.
	// Replace snapshot contents file with current state.
	// Close snapshot file.
	// Replace aof file with empty file.
	return nil
}

func (engine *Engine) Restore() error {
	// Open snapshot file.
	// If snapshot file exists, set current state to the state in snapshot file.
	// Open AOF file.
	// If AOF file exists, replay all the commands in the aof file.
	return nil
}
