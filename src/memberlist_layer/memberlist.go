package memberlist_layer

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"github.com/sethvargo/go-retry"
)

type NodeMeta struct {
	ServerID       raft.ServerID      `json:"ServerID"`
	MemberlistAddr string             `json:"MemberlistAddr"`
	RaftAddr       raft.ServerAddress `json:"RaftAddr"`
}

type MemberlistOpts struct {
	Config           utils.Config
	HasJoinedCluster func() bool
	AddVoter         func(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) error
	RemoveRaftServer func(meta NodeMeta) error
	IsRaftLeader     func() bool
}

type MemberList struct {
	options        MemberlistOpts
	broadcastQueue *memberlist.TransmitLimitedQueue
	numOfNodes     int
	memberList     *memberlist.Memberlist
}

func NewMemberList(opts MemberlistOpts) *MemberList {
	return &MemberList{
		options:        opts,
		broadcastQueue: new(memberlist.TransmitLimitedQueue),
		numOfNodes:     0,
	}
}

func (m *MemberList) MemberListInit(ctx context.Context) {
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindAddr = m.options.Config.BindAddr
	cfg.BindPort = int(m.options.Config.MemberListBindPort)
	cfg.Delegate = NewDelegate(DelegateOpts{
		config:         m.options.Config,
		broadcastQueue: m.broadcastQueue,
		addVoter:       m.options.AddVoter,
		isRaftLeader:   m.options.IsRaftLeader,
	})
	cfg.Events = NewEventDelegate(EventDelegateOpts{
		incrementNodes:   func() { m.numOfNodes += 1 },
		decrementNodes:   func() { m.numOfNodes -= 1 },
		removeRaftServer: m.options.RemoveRaftServer,
	})

	m.broadcastQueue.RetransmitMult = 1
	m.broadcastQueue.NumNodes = func() int {
		return m.numOfNodes
	}

	list, err := memberlist.Create(cfg)
	m.memberList = list

	if err != nil {
		log.Fatal(err)
	}

	if m.options.Config.JoinAddr != "" {
		backoffPolicy := utils.RetryBackoff(retry.NewFibonacci(1*time.Second), 5, 200*time.Millisecond, 0, 0)

		err := retry.Do(ctx, backoffPolicy, func(ctx context.Context) error {
			_, err := list.Join([]string{m.options.Config.JoinAddr})
			if err != nil {
				return retry.RetryableError(err)
			}
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}

		m.broadcastRaftAddress(ctx)
	}
}

func (m *MemberList) broadcastRaftAddress(ctx context.Context) {
	msg := BroadcastMessage{
		Action: "RaftJoin",
		NodeMeta: NodeMeta{
			ServerID: raft.ServerID(m.options.Config.ServerID),
			RaftAddr: raft.ServerAddress(fmt.Sprintf("%s:%d",
				m.options.Config.BindAddr, m.options.Config.RaftBindPort)),
		},
	}
	m.broadcastQueue.QueueBroadcast(&msg)
}

func (m *MemberList) ForwardDataMutation(ctx context.Context, cmd string, connId string) {
	// This function is only called by non-leaders
	// It uses the broadcast queue to forward a data mutation within the cluster
	m.broadcastQueue.QueueBroadcast(&BroadcastMessage{
		Action:      "MutateData",
		Content:     cmd,
		ContentHash: fmt.Sprintf("%x", md5.Sum([]byte(cmd))),
		ConnId:      connId,
		NodeMeta: NodeMeta{
			ServerID: raft.ServerID(m.options.Config.ServerID),
			RaftAddr: raft.ServerAddress(fmt.Sprintf("%s:%d",
				m.options.Config.BindAddr, m.options.Config.RaftBindPort)),
		},
	})
}

func (m *MemberList) MemberListShutdown(ctx context.Context) {
	// Gracefully leave memberlist cluster
	err := m.memberList.Leave(500 * time.Millisecond)

	if err != nil {
		log.Fatal("Could not gracefully leave memberlist cluster")
	}

	err = m.memberList.Shutdown()

	if err != nil {
		log.Fatal("Could not gracefully shutdown memberlist background maintanance")
	}

	fmt.Println("Successfully shutdown memberlist")
}
