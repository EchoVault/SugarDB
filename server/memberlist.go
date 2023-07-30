package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/sethvargo/go-retry"
)

type BroadcastMessage struct {
	Action     string             `json:"Action"`
	ServerID   raft.ServerID      `json:"ServerID"`
	ServerAddr raft.ServerAddress `json:"ServerAddr"`
	Content    string             `json:"Content"`
}

// Implements Broadcast interface
func (broadcastMessage *BroadcastMessage) Invalidates(other memberlist.Broadcast) bool {
	mb, ok := other.(*BroadcastMessage)

	if !ok {
		return false
	}

	if mb.ServerID == broadcastMessage.ServerID && mb.Action == "RaftJoin" {
		return true
	}

	return false
}

// Implements Broadcast interface
func (broadcastMessage *BroadcastMessage) Message() []byte {
	msg, err := json.Marshal(broadcastMessage)

	if err != nil {
		fmt.Println(err)
		return []byte{}
	}

	return msg
}

// Implements Broadcast interface
func (broadcastMessage *BroadcastMessage) Finished() {
	// No-Op
}

func (server *Server) MemberListInit() {
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindAddr = server.config.BindAddr
	cfg.BindPort = int(server.config.MemberListBindPort)
	cfg.Events = server
	cfg.Delegate = server

	server.broadcastQueue.RetransmitMult = 1
	server.broadcastQueue.NumNodes = func() int {
		return server.numOfNodes
	}

	list, err := memberlist.Create(cfg)
	server.memberList = list

	if err != nil {
		log.Fatal(err)
	}

	if server.config.JoinAddr != "" {
		ctx := context.Background()

		backoffPolicy := RetryBackoff(retry.NewFibonacci(1*time.Second), 5, 200*time.Millisecond, 0, 0)

		err := retry.Do(ctx, backoffPolicy, func(ctx context.Context) error {
			_, err := list.Join([]string{server.config.JoinAddr})
			if err != nil {
				return retry.RetryableError(err)
			}
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}

		go server.broadcastRaftAddress()
	}
}

func (server *Server) broadcastRaftAddress() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			fmt.Println("[JOIN_BROADCAST]: Trying to joing server if id ", server.config.ServerID)
			msg := BroadcastMessage{
				Action:     "RaftJoin",
				ServerID:   raft.ServerID(server.config.ServerID),
				ServerAddr: raft.ServerAddress(fmt.Sprintf("%s:%d", server.config.BindAddr, server.config.RaftBindPort)),
			}
			server.broadcastQueue.QueueBroadcast(&msg)
		case msg := <-*server.raftJoinSuccessCh:
			if msg.ServerID == raft.ServerID(server.config.ServerID) {
				fmt.Printf("Server %s succesfully joined raft cluster.\n", msg.ServerID)
				return
			}
		}
	}
}

// Implements Delegate interface
func (server *Server) NodeMeta(limit int) []byte {
	return []byte("")
}

// Implements Delegate interface
func (server *Server) NotifyMsg(msgBytes []byte) {
	var msg BroadcastMessage

	if err := json.Unmarshal(msgBytes, &msg); err != nil {
		fmt.Print(err)
		return
	}

	switch msg.Action {
	case "RaftJoin":
		if err := server.addVoter(raft.ServerID(msg.ServerID), raft.ServerAddress(msg.ServerAddr), 0, 0); err != nil {
			fmt.Println(err)
		}
	case "RaftJoinSuccess":
		*server.raftJoinSuccessCh <- msg
	case "MutateData":
		// Mutate the value at a given key
	case "FetchData":
		// Fetch the value at a fiven key
	}
}

// Implements Delegate interface
func (server *Server) GetBroadcasts(overhead, limit int) [][]byte {
	return server.broadcastQueue.GetBroadcasts(overhead, limit)
}

// Implements Delegate interface
func (server *Server) LocalState(join bool) []byte {
	// No-Op
	return []byte("")
}

// Implements Delegate interface
func (server *Server) MergeRemoteState(buf []byte, join bool) {
	// No-Op
}

// Implements EventDelegate interface
func (server *Server) NotifyJoin(node *memberlist.Node) {
	server.numOfNodes += 1
}

// Implements EventDelegate interface
func (server *Server) NotifyLeave(node *memberlist.Node) {
	server.numOfNodes -= 1
}

// Implements EventDelegate interface
func (server *Server) NotifyUpdate(node *memberlist.Node) {
	// No-Op
}

func (server *Server) MemberListShutdown() {
	// Triggered after RaftShutdown
	// Gracefully leave memberlist cluster
	// Broadcast message to remove current node from raft cluster
	fmt.Println("Shutting down memberlist.")
}
