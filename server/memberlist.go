package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	retry "github.com/sethvargo/go-retry"
)

type BroadcastMessage struct {
	Action     string `json:"Action"`
	ServerID   string `json:"ServerID"`
	ServerAddr string `json:"ServerAddr"`
}

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

func (broadcastMessage *BroadcastMessage) Message() []byte {
	msg, err := json.Marshal(broadcastMessage)

	if err != nil {
		fmt.Println(err)
		return []byte{}
	}

	return msg
}

func (broadcastMessage *BroadcastMessage) Finished() {
	// No-Op
}

func (server *Server) MemberListInit() {
	// Triggered before RaftInit
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindAddr = server.config.BindAddr
	cfg.BindPort = int(server.config.MemberListBindPort)
	cfg.Delegate = server

	list, err := memberlist.Create(cfg)
	server.memberList = list

	if err != nil {
		log.Fatal(err)
	}

	if server.config.JoinAddr != "" {
		ctx := context.Background()

		backoffPolicy := RetryBackoff(retry.NewFibonacci(1*time.Second), 0, 0, 0, 0)

		err := retry.Do(ctx, backoffPolicy, func(ctx context.Context) error {
			fmt.Printf("%s trying to joing the cluster...\n", server.config.ServerID)
			return retry.RetryableError(fmt.Errorf("there's a retryable error"))
		})

		if err != nil {
			log.Fatal(err)
		}
	}
}

func (server *Server) NodeMeta(limit int) []byte {
	return []byte{}
}

func (server *Server) NotifyMsg(msg []byte) {
	fmt.Println(string(msg))
}

func (server *Server) GetBroadcasts(overhead, limit int) [][]byte {
	return server.broadcastQueue.GetBroadcasts(overhead, limit)
}

func (server *Server) LocalState(join bool) []byte {
	// No-Op
	return []byte{}
}

func (server *Server) MergeRemoteState(buf []byte, join bool) {
	// No-Op
}

func (server *Server) MemberListShutdown() {
	// Triggered after RaftShutdown
	// Gracefully leave memberlist cluster
	// Broadcast message to remove current node from raft cluster
	fmt.Println("Shutting down memberlist.")
}
