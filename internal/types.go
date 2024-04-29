// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"github.com/echovault/echovault/internal/clock"
	"net"
	"time"
)

type KeyData struct {
	Value    interface{}
	ExpireAt time.Time
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

type SnapshotObject struct {
	State                      map[string]KeyData
	LatestSnapshotMilliseconds int64
}

type KeyExtractionFuncResult struct {
	Channels  []string
	ReadKeys  []string
	WriteKeys []string
}

type KeyExtractionFunc func(cmd []string) (KeyExtractionFuncResult, error)

type HandlerFuncParams struct {
	Context               context.Context
	Command               []string
	Connection            *net.Conn
	KeyLock               func(ctx context.Context, key string) (bool, error)
	KeyUnlock             func(ctx context.Context, key string)
	KeyRLock              func(ctx context.Context, key string) (bool, error)
	KeyRUnlock            func(ctx context.Context, key string)
	KeyExists             func(ctx context.Context, key string) bool
	CreateKeyAndLock      func(ctx context.Context, key string) (bool, error)
	GetValue              func(ctx context.Context, key string) interface{}
	SetValue              func(ctx context.Context, key string, value interface{}) error
	GetExpiry             func(ctx context.Context, key string) time.Time
	SetExpiry             func(ctx context.Context, key string, expire time.Time, touch bool)
	RemoveExpiry          func(ctx context.Context, key string)
	DeleteKey             func(ctx context.Context, key string) error
	GetClock              func() clock.Clock
	GetAllCommands        func() []Command
	GetACL                func() interface{}
	GetPubSub             func() interface{}
	TakeSnapshot          func() error
	RewriteAOF            func() error
	GetLatestSnapshotTime func() int64
}

type HandlerFunc func(params HandlerFuncParams) ([]byte, error)

type Command struct {
	Command     string
	Module      string
	Categories  []string
	Description string
	SubCommands []SubCommand
	Sync        bool // Specifies if command should be synced across replication cluster
	KeyExtractionFunc
	HandlerFunc
}

type SubCommand struct {
	Command     string
	Module      string
	Categories  []string
	Description string
	Sync        bool // Specifies if sub-command should be synced across replication cluster
	KeyExtractionFunc
	HandlerFunc
}
