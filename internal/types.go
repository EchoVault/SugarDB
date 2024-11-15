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
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"
	"unsafe"

	"github.com/echovault/sugardb/internal/clock"
	"github.com/echovault/sugardb/internal/constants"
)

type KeyData struct {
	Value    interface{}
	ExpireAt time.Time
}

func (k *KeyData) GetMem() (int64, error) {
	var size int64
	size = int64(unsafe.Sizeof(k.ExpireAt))

	// check type of Value field
	switch v := k.Value.(type) {
	case nil:
		size += 0
	// AdaptType() will always ensure data type is of string, float64 or int.
	case int:
		size += int64(unsafe.Sizeof(v))
	// int64 data type used with module.SET
	case float64, int64:
		size += 8
	case string:
		// Add the size of the header and the number of bytes of the string
		size += int64(unsafe.Sizeof(v))
		size += int64(len(v))

	// handle list
	case []string:
		for _, s := range v {
			size += int64(unsafe.Sizeof(s))
			size += int64(len(s))
		}

	// handle non primitive datatypes like hash, set, and sorted set
	case constants.CompositeType:
		size += k.Value.(constants.CompositeType).GetMem()

	default:
		return 0, errors.New(fmt.Sprintf("ERROR: type %v is not supported in method KeyData.GetMem()", reflect.TypeOf(v)))
	}

	return size, nil
}

type ContextServerID string
type ContextConnID string

type ApplyRequest struct {
	Type         string   `json:"Type"` // command | delete-key
	ServerID     string   `json:"ServerID"`
	ConnectionID string   `json:"ConnectionID"`
	Protocol     int      `json:"Protocol"`
	Database     int      `json:"Database"`
	CMD          []string `json:"CMD"`
	Key          string   `json:"Key"` // Optional: Used with delete-key type to specify which key to delete.
}

type ApplyResponse struct {
	Error    error
	Response []byte
}

type SnapshotObject struct {
	State                      map[int]map[string]KeyData
	LatestSnapshotMilliseconds int64
}

// ServerInfo holds information about the server/node.
type ServerInfo struct {
	Server     string
	Version    string
	Id         string
	Mode       string
	Role       string
	Modules    []string
	MemoryUsed int64
	MaxMemory  uint64
}

// ConnectionInfo holds information about the connection
type ConnectionInfo struct {
	Id       uint64 // Connection id.
	Name     string // Alias name for this connection.
	Protocol int    // The RESP protocol used by the client. Can be either 2 or 3.
	Database int    // Database index currently being used by the connection.
}

// KeyExtractionFuncResult is the return type of the KeyExtractionFunc for the command/subcommand.
type KeyExtractionFuncResult struct {
	Channels  []string // The pubsub channels the command accesses. For non pubsub commands, this should be an empty slice.
	ReadKeys  []string // The keys the command reads from. If no keys are read, this should be an empty slice.
	WriteKeys []string // The keys the command writes to. If no keys are written to, this should be an empty slice.
}

// KeyExtractionFunc is included with every command/subcommand. This function returns a KeyExtractionFuncResult object.
// The return value of this function is used in the ACL layer to determine whether the connection is allowed to
// execute this command.
// The cmd parameter is a string slice of the command. All the keys are extracted from this command.
type KeyExtractionFunc func(cmd []string) (KeyExtractionFuncResult, error)

// HandlerFuncParams is the object passed to a command handler when a command is triggered.
// These params are provided to commands by the SugarDB engine to help the command hook into functions from the
// echovault package.
type HandlerFuncParams struct {
	// Context is the context passed from the SugarDB instance.
	Context context.Context
	// Command is the string slice contains the command (e.g []string{"SET", "key", "value"})
	Command []string
	// Connection is the connection that triggered this command.
	// Do not write the response directly to the connection, return it from the function.
	Connection *net.Conn
	// KeysExist returns a map that specifies which keys exist in the keyspace.
	KeysExist func(ctx context.Context, keys []string) map[string]bool
	// GetExpiry returns the expiry time of a key.
	GetExpiry func(ctx context.Context, key string) time.Time
	// GetHashExpiry returns the expiry time of a field in a key whose value is a hash.
	GetHashExpiry func(ctx context.Context, key string, field string) time.Time
	// DeleteKey deletes the specified key. Returns an error if the deletion was unsuccessful.
	DeleteKey func(ctx context.Context, key string) error
	// GetValues retrieves the values from the specified keys.
	// Non-existent keys will be nil.
	GetValues func(ctx context.Context, keys []string) map[string]interface{}
	// SetValues sets each of the keys with their corresponding values in the provided map.
	SetValues func(ctx context.Context, entries map[string]interface{}) error
	// SetExpiry sets the expiry time of the key.
	SetExpiry func(ctx context.Context, key string, expire time.Time, touch bool)
	// SetHashExpiry sets the expiry time of a field in a key whose value is a hash.
	SetHashExpiry func(ctx context.Context, key string, field string, expire time.Time) error
	// GetClock gets the clock used by the server.
	// Use this when making use of time methods like .Now and .After.
	// This inversion of control is a helper for testing as the clock is automatically mocked in tests.
	GetClock func() clock.Clock
	// GetAllCommands returns all the commands loaded in the SugarDB instance.
	GetAllCommands func() []Command
	// GetACL returns the SugarDB instance's ACL engine.
	// There's no need to use this outside of the acl package,
	// ACL authorizations for all commands will be handled automatically by the SugarDB instance as long as the
	// commands KeyExtractionFunc returns the correct keys.
	GetACL func() interface{}
	// GetPubSub returns the SugarDB instance's PubSub engine.
	// There's no need to use this outside of the pubsub package.
	GetPubSub func() interface{}
	// TakeSnapshot triggers a snapshot by the SugarDB instance.
	TakeSnapshot func() error
	// RewriteAOF triggers a compaction of the commands logs by the SugarDB instance.
	RewriteAOF func() error
	// GetLatestSnapshotTime returns the latest snapshot timestamp.
	GetLatestSnapshotTime func() int64
	// LoadModule loads the provided module with the given args passed to the module's
	// key extraction and handler functions.
	LoadModule func(path string, args ...string) error
	// UnloadModule removes the specified module.
	// This unloads both custom modules and internal modules.
	UnloadModule func(module string)
	// ListModules returns the list of modules loaded in the SugarDB instance.
	ListModules func() []string
	// SetConnectionInfo sets the connection's protocol and clientname.
	SetConnectionInfo func(conn *net.Conn, clientname string, protocol int, database int)
	// GetConnectionInfo returns information about the current connection.
	GetConnectionInfo func(conn *net.Conn) ConnectionInfo
	// GetServerInfo returns information about the server when requested by commands such as HELLO.
	GetServerInfo func() ServerInfo
	// SwapDBs swaps two databases,
	// so that immediately all the clients connected to a given database will see the data of the other database,
	// and the other way around.
	SwapDBs func(database1, database2 int)
	// FlushDB flushes the specified database keys. It accepts the integer index of the database to be flushed.
	// If -1 is passed as the index, then all databases will be flushed.
	Flush func(database int)
	// Randomkey returns a random key
	Randomkey func(ctx context.Context) string
	// (TOUCH key [key ...]) Alters the last access time or access count of the key(s) depending on whether LFU or LRU strategy was used.
	// A key is ignored if it does not exist.
	Touchkey func(ctx context.Context, keys []string) (int64, error)
	// GetObjectFrequency retrieves the access frequency count of a key. Can only be used with LFU type eviction policies.
	GetObjectFrequency func(ctx context.Context, keys string) (int, error)
	// GetObjectIdleTime retrieves the time in seconds since the last access of a key. Can only be used with LRU type eviction policies.
	GetObjectIdleTime func(ctx context.Context, keys string) (float64, error)
}

// HandlerFunc is a functions described by a command where the bulk of the command handling is done.
// This function returns a byte slice which contains a RESP2 response. The response from this function
// is forwarded directly to the client connection that triggered the command.
// In embedded mode, the response is parsed and a native Go type is returned to the caller.
type HandlerFunc func(params HandlerFuncParams) ([]byte, error)

type Command struct {
	Command     string       // The command keyword (e.g. "set", "get", "hset").
	Module      string       // The module this command belongs to. All the available modules are in the `constants` package.
	Categories  []string     // The ACL categories this command belongs to. All the available categories are in the `constants` package.
	Description string       // The description of the command. Includes the command syntax.
	SubCommands []SubCommand // The list of subcommands for this command. Empty if the command has no subcommands.
	Sync        bool         // Specifies if command should be synced across replication cluster
	KeyExtractionFunc
	HandlerFunc
}

type SubCommand struct {
	Command     string   // The keyword for this subcommand. (Check the acl module for an example of subcommands within a command).
	Module      string   // The module this subcommand belongs to. Should be the same as the parent command.
	Categories  []string // The ACL categories the subcommand belongs to.
	Description string   // The description of the subcommand. Includes syntax.
	Sync        bool     // Specifies if sub-command should be synced across replication cluster
	KeyExtractionFunc
	HandlerFunc
}
