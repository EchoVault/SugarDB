package raft

import (
	"encoding/json"
	"github.com/echovault/echovault/src/utils"
	"github.com/hashicorp/raft"
	"strconv"
	"strings"
)

type SnapshotOpts struct {
	config            utils.Config
	data              map[string]interface{}
	startSnapshot     func()
	finishSnapshot    func()
	setLatestSnapshot func(msec int64)
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

	msec, err := strconv.Atoi(strings.Split(sink.ID(), "-")[2])
	if err != nil {
		sink.Cancel()
		return err
	}

	snapshotObject := utils.SnapshotObject{
		State:                      s.options.data,
		LatestSnapshotMilliseconds: int64(msec),
	}

	o, err := json.Marshal(snapshotObject)

	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err = sink.Write(o); err != nil {
		sink.Cancel()
		return err
	}

	s.options.setLatestSnapshot(int64(msec))

	return nil
}

// Release implements FSMSnapshot interface
func (s *Snapshot) Release() {
	s.options.finishSnapshot()
}
