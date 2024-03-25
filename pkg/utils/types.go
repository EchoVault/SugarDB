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

package utils

import (
	"context"
	"net"
	"time"
)

// KeyData holds the structure of the in-memory data stored at a string key.
type KeyData struct {
	Value    interface{}
	ExpireAt time.Time
}

type EchoVault interface {
	KeyLock(ctx context.Context, key string) (bool, error)
	KeyUnlock(ctx context.Context, key string)
	KeyRLock(ctx context.Context, key string) (bool, error)
	KeyRUnlock(ctx context.Context, key string)
	KeyExists(ctx context.Context, key string) bool
	CreateKeyAndLock(ctx context.Context, key string) (bool, error)
	GetValue(ctx context.Context, key string) interface{}
	SetValue(ctx context.Context, key string, value interface{}) error
	GetExpiry(ctx context.Context, key string) time.Time
	SetExpiry(ctx context.Context, key string, expire time.Time, touch bool)
	RemoveExpiry(key string)
	DeleteKey(ctx context.Context, key string) error
	GetState() map[string]KeyData
	GetAllCommands() []Command
	GetACL() interface{}
	GetPubSub() interface{}
	TakeSnapshot() error
	StartSnapshot()
	FinishSnapshot()
	SetLatestSnapshot(msec int64)
	GetLatestSnapshot() int64
	RewriteAOF() error
}

type ContextServerID string
type ContextConnID string

type ApplyRequest struct {
	Type         string   `json:"Type"` // command | delete-key
	ServerID     string   `json:"ServerID"`
	ConnectionID string   `json:"ConnectionID"`
	CMD          []string `json:"CMD"`
	Key          string   `json:"Key"`
}

type ApplyResponse struct {
	Error    error
	Response []byte
}

type KeyExtractionFunc func(cmd []string) ([]string, error)
type HandlerFunc func(ctx context.Context, cmd []string, echovault EchoVault, conn *net.Conn) ([]byte, error)

type SubCommand struct {
	Command     string
	Categories  []string
	Description string
	Sync        bool // Specifies if sub-command should be synced across cluster
	KeyExtractionFunc
	HandlerFunc
}

type Command struct {
	Command     string
	Categories  []string
	Description string
	SubCommands []SubCommand
	Sync        bool // Specifies if command should be synced across cluster
	KeyExtractionFunc
	HandlerFunc
}

type Plugin interface {
	Name() string
	Commands() []Command
	Description() string
}

type ACL interface {
	RegisterConnection(conn *net.Conn)
	AuthorizeConnection(conn *net.Conn, cmd []string, command Command, subCommand SubCommand) error
}

type PubSub interface{}

type SnapshotObject struct {
	State                      map[string]KeyData
	LatestSnapshotMilliseconds int64
}
