package memberlist

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
)

type BroadcastMessage struct {
	NodeMeta
	Action      string `json:"Action"`
	Content     string `json:"Content"`
	ContentHash string `json:"ContentHash"`
	ConnId      string `json:"ConnId"`
}

// Invalidates Implements Broadcast interface
func (broadcastMessage *BroadcastMessage) Invalidates(other memberlist.Broadcast) bool {
	otherBroadcast, ok := other.(*BroadcastMessage)

	if !ok {
		return false
	}

	switch broadcastMessage.Action {
	case "RaftJoin":
		return broadcastMessage.Action == otherBroadcast.Action &&
			broadcastMessage.ServerID == otherBroadcast.ServerID
	case "MutateData":
		return broadcastMessage.Action == otherBroadcast.Action &&
			broadcastMessage.ContentHash == otherBroadcast.ContentHash
	default:
		return false
	}
}

// Message Implements Broadcast interface
func (broadcastMessage *BroadcastMessage) Message() []byte {
	msg, err := json.Marshal(broadcastMessage)

	if err != nil {
		fmt.Println(err)
		return []byte{}
	}

	return msg
}

// Finished Implements Broadcast interface
func (broadcastMessage *BroadcastMessage) Finished() {
	// No-Op
}
