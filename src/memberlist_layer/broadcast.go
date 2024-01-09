package memberlist_layer

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
)

type BroadcastMessage struct {
	NodeMeta
	Action  string `json:"Action"`
	Content string `json:"Content"`
}

// Invalidates Implements Broadcast interface
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
