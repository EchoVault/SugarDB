// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memberlist

import (
	"encoding/json"
	"github.com/hashicorp/memberlist"
	"log"
)

type BroadcastMessage struct {
	NodeMeta
	Action      string   `json:"Action"`
	Content     []byte   `json:"Content"`
	ContentHash [16]byte `json:"ContentHash"`
	ConnId      string   `json:"ConnId"`
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
		log.Println(err)
		return []byte{}
	}

	return msg
}

// Finished Implements Broadcast interface
func (broadcastMessage *BroadcastMessage) Finished() {
	// No-Op
}
