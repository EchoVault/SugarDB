package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

func (server *Server) RaftInit() {
	conf := server.config

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(conf.ServerID)

	raftLogStore := raft.NewInmemStore()
	raftStableStore := raft.NewInmemStore()
	raftSnapshotStore := raft.NewInmemSnapshotStore()

	raftAddr := fmt.Sprintf("%s:%d", conf.BindAddr, conf.RaftBindPort)

	raftAdvertiseAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		log.Fatal(err)
	}

	raftTransport, err := raft.NewTCPTransport(
		raftAddr,
		raftAdvertiseAddr,
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
		&raft.MockFSM{},
		raftLogStore,
		raftStableStore,
		raftSnapshotStore,
		raftTransport,
	)

	if err != nil {
		log.Fatalf("Could not start node with error; %s", err)
	}

	server.raft = raftServer

	if conf.JoinAddr == "" {
		// Bootstrap raft cluster
		if err := server.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(conf.ServerID),
					Address:  raft.ServerAddress(raftAddr),
				},
			},
		}).Error(); err != nil {
			log.Fatal(err)
		}
	}
}

// Implement raft.FSM interface
func (server *Server) Apply(log *raft.Log) interface{} {
	return nil
}

// Implement raft.FSM interface
func (server *Server) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

// Implement raft.FSM interface
func (server *Server) Restore(snapshot io.ReadCloser) error {
	return nil
}

// Implements raft.StableStore interface
func (server *Server) Set(key []byte, value []byte) error {
	return nil
}

// Implements raft.StableStore interface
func (server *Server) Get(key []byte) ([]byte, error) {
	return []byte{}, nil
}

// Implements raft.StableStore interface
func (server *Server) SetUint64(key []byte, val uint64) error {
	return nil
}

// Implements raft.StableStore interface
func (server *Server) GetUint64(key []byte) (uint64, error) {
	return 0, nil
}

func (server *Server) isRaftLeader() bool {
	return server.raft.State() == raft.Leader
}

func (server *Server) addVoter(
	id raft.ServerID,
	address raft.ServerAddress,
	prevIndex uint64,
	timeout time.Duration,
) error {
	if server.isRaftLeader() {
		raftConfig := server.raft.GetConfiguration()
		if err := raftConfig.Error(); err != nil {
			return errors.New("could not retrieve raft config")
		}

		// After successfully adding the voter node
		// or if voter node has already been added,
		// broadcast this success message
		msg := BroadcastMessage{
			Action:     "RaftJoinSuccess",
			ServerID:   id,
			ServerAddr: address,
		}

		for _, s := range raftConfig.Configuration().Servers {
			// Check if a server already exists with the current attribtues
			if s.ID == id && s.Address == address {
				server.broadcastQueue.QueueBroadcast(&msg)
				return fmt.Errorf("server with id %s and address %s already exists", id, address)
			}
		}

		err := server.raft.AddVoter(id, address, prevIndex, timeout).Error()
		if err != nil {
			return err
		}

		server.broadcastQueue.QueueBroadcast(&msg)
	}
	return nil
}

func (server *Server) RaftShutdown() {
	// Triggered before MemberListShutdown
	// Leadership transfer if current node is the leader
	// Shutdown of the raft server
	fmt.Println("Shutting down raft.")
}
