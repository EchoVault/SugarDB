package snapshot

import (
	"io"

	"github.com/hashicorp/raft"
)

type SQLiteSnapshotStore struct{}

type SQLiteSnapshotSink struct{}

// Implementation of snapshot store
func (store *SQLiteSnapshotStore) Create(
	version raft.SnapshotVersion,
	index, term uint64,
	configuration raft.Configuration,
	configurationIndex uint64,
	trans raft.Transport) (raft.SnapshotSink, error) {

	return &SQLiteSnapshotSink{}, nil
}

func (store *SQLiteSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	return []*raft.SnapshotMeta{}, nil
}

func (store *SQLiteSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, nil
}

// Implementation of snapshot sink
func (sink *SQLiteSnapshotSink) ID() string {
	return ""
}

func (sink *SQLiteSnapshotSink) Cancel() error {
	return nil
}

func (sink *SQLiteSnapshotSink) Write(b []byte) (int, error) {
	return 0, nil
}

func (sink *SQLiteSnapshotSink) Close() error {
	return nil
}
