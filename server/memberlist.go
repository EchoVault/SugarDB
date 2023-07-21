package main

import (
	"fmt"
	"log"

	"github.com/hashicorp/memberlist"
)

func (server *Server) MemberListInit() {
	// Triggered before RaftInit
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindAddr = server.config.BindAddr
	cfg.BindPort = int(server.config.MemberListBindPort)

	list, err := memberlist.Create(cfg)
	server.memberList = list

	if err != nil {
		log.Fatal(err)
	}

	if server.config.JoinAddr != "" {
		n, err := server.memberList.Join([]string{server.config.JoinAddr})

		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Joined cluster. Contacted %d nodes.\n", n)
	}

	// go func() {
	// 	for {
	// 		fmt.Println(server.memberList.NumMembers())
	// 		time.Sleep(2 * time.Second)
	// 	}
	// }()
}

func (server *Server) ShutdownMemberList() {
	// Triggered after RaftShutdown
	// Gracefully leave memberlist cluster
	// Broadcast message to remove current node from raft cluster
}
