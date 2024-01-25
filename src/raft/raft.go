package raft

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/memberlist"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/echovault/echovault/src/utils"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Opts struct {
	Config     utils.Config
	Server     utils.Server
	GetCommand func(command string) (utils.Command, error)
}

type Raft struct {
	options Opts
	raft    *raft.Raft
}

func NewRaft(opts Opts) *Raft {
	return &Raft{
		options: opts,
	}
}

func (r *Raft) RaftInit(ctx context.Context) {
	conf := r.options.Config

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(conf.ServerID)
	raftConfig.SnapshotThreshold = 5

	var logStore raft.LogStore
	var stableStore raft.StableStore
	var snapshotStore raft.SnapshotStore

	if conf.InMemory {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		snapshotStore = raft.NewInmemSnapshotStore()
	} else {
		boltdb, err := raftboltdb.NewBoltStore(filepath.Join(conf.DataDir, "logs.db"))
		if err != nil {
			log.Fatal(err)
		}

		logStore, err = raft.NewLogCache(512, boltdb)
		if err != nil {
			log.Fatal(err)
		}

		stableStore = raft.StableStore(boltdb)

		snapshotStore, err = raft.NewFileSnapshotStore(path.Join(conf.DataDir, "snapshots"), 2, os.Stdout)
		if err != nil {
			log.Fatal(err)
		}
	}

	addr := fmt.Sprintf("%s:%d", conf.BindAddr, conf.RaftBindPort)

	advertiseAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	raftTransport, err := raft.NewTCPTransport(
		addr,
		advertiseAddr,
		10,
		500*time.Millisecond,
		os.Stdout,
	)

	if err != nil {
		log.Fatal(err)
	}

	// Start raft server
	raftServer, err := raft.NewRaft(
		raftConfig,
		NewFSM(FSMOpts{
			Config:     r.options.Config,
			Server:     r.options.Server,
			GetCommand: r.options.GetCommand,
		}),
		logStore,
		stableStore,
		snapshotStore,
		raftTransport,
	)

	if err != nil {
		log.Fatalf("Could not start node with error; %s", err)
	}

	if conf.BootstrapCluster {
		// Error can be safely ignored if we're already leader
		_ = raftServer.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(conf.ServerID),
					Address:  raft.ServerAddress(addr),
				},
			},
		}).Error()
	}

	r.raft = raftServer
}

func (r *Raft) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	return r.raft.Apply(cmd, timeout)
}

func (r *Raft) IsRaftLeader() bool {
	return r.raft.State() == raft.Leader
}

func (r *Raft) isRaftFollower() bool {
	return r.raft.State() == raft.Follower
}

func (r *Raft) HasJoinedCluster() bool {
	isFollower := r.isRaftFollower()

	leaderAddr, leaderID := r.raft.LeaderWithID()
	hasLeader := leaderAddr != "" && leaderID != ""

	return isFollower && hasLeader
}

func (r *Raft) AddVoter(
	id raft.ServerID,
	address raft.ServerAddress,
	prevIndex uint64,
	timeout time.Duration,
) error {
	if r.IsRaftLeader() {
		raftConfig := r.raft.GetConfiguration()
		if err := raftConfig.Error(); err != nil {
			return errors.New("could not retrieve raft config")
		}

		for _, s := range raftConfig.Configuration().Servers {
			// Check if a server already exists with the current attributes
			if s.ID == id && s.Address == address {
				return fmt.Errorf("server with id %s and address %s already exists", id, address)
			}
		}

		err := r.raft.AddVoter(id, address, prevIndex, timeout).Error()
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Raft) RemoveServer(meta memberlist.NodeMeta) error {
	if !r.IsRaftLeader() {
		return errors.New("not leader, could not remove server")
	}

	if err := r.raft.RemoveServer(meta.ServerID, 0, 0).Error(); err != nil {
		return err
	}

	return nil
}

func (r *Raft) RaftShutdown(ctx context.Context) {
	// Leadership transfer if current node is the leader
	if r.IsRaftLeader() {
		err := r.raft.LeadershipTransfer().Error()

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Leadership transfer successful.")
	}
}
