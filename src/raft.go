package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/kelvinmwinuka/memstore/src/utils"
)

func (server *Server) RaftInit(ctx context.Context) {
	conf := server.config

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
		raft.FSM(server),
		logStore,
		stableStore,
		snapshotStore,
		raftTransport,
	)

	if err != nil {
		log.Fatalf("Could not start node with error; %s", err)
	}

	server.raft = raftServer

	if conf.BootstrapCluster {
		// Bootstrap raft cluster
		if err := server.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(conf.ServerID),
					Address:  raft.ServerAddress(addr),
				},
			},
		}).Error(); err != nil {
			log.Fatal(err)
		}
	}

}

// Apply Implements raft.FSM interface
func (server *Server) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:
		var request utils.ApplyRequest

		if err := json.Unmarshal(log.Data, &request); err != nil {
			return utils.ApplyResponse{
				Error:    err,
				Response: nil,
			}
		}

		ctx := context.WithValue(context.Background(), utils.ContextServerID("ServerID"), request.ServerID)
		ctx = context.WithValue(ctx, utils.ContextConnID("ConnectionID"), request.ConnectionID)

		// Handle command using plugins
		if res, err := server.handlePluginCommand(ctx, request.CMD); err != nil {
			return utils.ApplyResponse{
				Error:    err,
				Response: nil,
			}
		} else {
			return utils.ApplyResponse{
				Error:    nil,
				Response: res,
			}
		}
	}

	return nil
}

// Implements raft.FSM interface
func (server *Server) Snapshot() (raft.FSMSnapshot, error) {
	return server, nil
}

// Implements raft.FSM interface
func (server *Server) Restore(snapshot io.ReadCloser) error {
	b, err := io.ReadAll(snapshot)

	if err != nil {
		return err
	}

	data := make(map[string]interface{})

	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}

	for k, v := range data {
		server.keyLocks[k].Lock()
		server.SetValue(context.Background(), k, v)
		server.keyLocks[k].Unlock()
	}

	return nil
}

// Implements FSMSnapshot interface
func (server *Server) Persist(sink raft.SnapshotSink) error {
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

// Implements FSMSnapshot interface
func (server *Server) Release() {}

func (server *Server) isRaftLeader() bool {
	return server.raft.State() == raft.Leader
}

func (server *Server) isRaftFollower() bool {
	return server.raft.State() == raft.Follower
}

func (server *Server) hasJoinedCluster() bool {
	isFollower := server.isRaftFollower()

	leaderAddr, leaderID := server.raft.LeaderWithID()
	hasLeader := leaderAddr != "" && leaderID != ""

	return isFollower && hasLeader
}

func (server *Server) addVoter(
	id raft.ServerID,
	address raft.ServerAddress,
	prevIndex uint64,
	timeout time.Duration,
) error {
	if !server.isRaftLeader() {
		return errors.New("not leader, cannot add voter")
	}
	raftConfig := server.raft.GetConfiguration()
	if err := raftConfig.Error(); err != nil {
		return errors.New("could not retrieve raft config")
	}

	for _, s := range raftConfig.Configuration().Servers {
		// Check if a server already exists with the current attributes
		if s.ID == id && s.Address == address {
			return fmt.Errorf("server with id %s and address %s already exists", id, address)
		}
	}

	err := server.raft.AddVoter(id, address, prevIndex, timeout).Error()
	if err != nil {
		return err
	}

	return nil
}

func (server *Server) removeServer(meta NodeMeta) error {
	if !server.isRaftLeader() {
		return errors.New("not leader, could not remove server")
	}

	if err := server.raft.RemoveServer(meta.ServerID, 0, 0).Error(); err != nil {
		return err
	}

	return nil
}

func (server *Server) RaftShutdown(ctx context.Context) {
	// Leadership transfer if current node is the leader
	if server.isRaftLeader() {
		err := server.raft.LeadershipTransfer().Error()

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Leadership transfer successful.")
	}
}
