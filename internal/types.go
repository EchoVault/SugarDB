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
	KeysExist             func(keys []string) map[string]bool
	GetExpiry             func(key string) time.Time
	DeleteKey             func(key string) error
	GetValues             func(ctx context.Context, keys []string) map[string]interface{}
	SetValues             func(ctx context.Context, entries map[string]interface{}) error
	SetExpiry             func(ctx context.Context, key string, expire time.Time, touch bool)
	GetClock              func() clock.Clock
	GetAllCommands        func() []Command
	GetACL                func() interface{}
	GetPubSub             func() interface{}
	TakeSnapshot          func() error
	RewriteAOF            func() error
	GetLatestSnapshotTime func() int64
	LoadModule            func(path string, args ...string) error
	UnloadModule          func(module string)
	ListModules           func() []string
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
