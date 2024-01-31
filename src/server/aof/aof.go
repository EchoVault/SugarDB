package aof

import (
	"encoding/json"
	"github.com/echovault/echovault/src/utils"
	"log"
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

func (engine *Engine) LogCommand(command []byte, sync bool) error {
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

	if sync {
		if err = f.Sync(); err != nil {
			log.Println(err)
		}
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

func (engine *Engine) Restore() error {
	// Open snapshot file.
	// If snapshot file exists, set current state to the state in snapshot file.
	// Open AOF file.
	// If AOF file exists, replay all the commands in the aof file.
	return nil
}
