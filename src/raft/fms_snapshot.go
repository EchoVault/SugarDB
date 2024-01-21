package raft

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/kelvinmwinuka/memstore/src/utils"
)

type SnapshotOpts struct {
	Config utils.Config
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
	data := map[string]interface{}{}

	// TODO: Copy current store contents
	o, err := json.Marshal(data)

	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err = sink.Write(o); err != nil {
		sink.Cancel()
		return err
	}

	// TODO: Store data in separate snapshot file

	return nil
}

// Release implements FSMSnapshot interface
func (s *Snapshot) Release() {}
