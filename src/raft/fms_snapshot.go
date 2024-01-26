package raft

import (
	"encoding/json"
	"github.com/echovault/echovault/src/utils"
	"github.com/hashicorp/raft"
)

type SnapshotOpts struct {
	config         utils.Config
	data           map[string]interface{}
	startSnapshot  func()
	finishSnapshot func()
}

type Snapshot struct {
	options SnapshotOpts
}

func NewFSMSnapshot(opts SnapshotOpts) *Snapshot {
	return &Snapshot{
		options: opts,
	}
}

// Persist implements FSMSnapshot interface
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	s.options.startSnapshot()

	o, err := json.Marshal(s.options.data)

	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err = sink.Write(o); err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release implements FSMSnapshot interface
func (s *Snapshot) Release() {
	s.options.finishSnapshot()
}
