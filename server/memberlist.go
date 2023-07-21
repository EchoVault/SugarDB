package main

import (
	"log"

	"github.com/hashicorp/memberlist"
)

func (server *Server) MemberListInit() {
	// Triggered before RaftInit
	memberList, err := memberlist.Create(memberlist.DefaultLocalConfig())
	if err != nil {
		log.Fatal("Could not start memberlist cluster.")
	}

	server.memberList = memberList
}

func (server *Server) ShutdownMemberList() {
	// Triggered after RaftShutdown
	// Gracefully leave memberlist cluster
	// Broadcast message to remove current node from raft cluster
}
