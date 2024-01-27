package aof

import (
	"github.com/echovault/echovault/src/utils"
)

// This package handles AOF logging in standalone mode only.
// Logging in clusters is handled in the raft layer.

type Opts struct {
	Config utils.Config
}

type Engine struct {
	options Opts
}

func NewAOFEngine(opts Opts) *Engine {
	return &Engine{
		options: opts,
	}
}
