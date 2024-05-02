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

package echovault

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/aof"
	"github.com/echovault/echovault/internal/clock"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/constants"
	"github.com/echovault/echovault/internal/eviction"
	"github.com/echovault/echovault/internal/memberlist"
	"github.com/echovault/echovault/internal/modules/acl"
	"github.com/echovault/echovault/internal/modules/admin"
	"github.com/echovault/echovault/internal/modules/connection"
	"github.com/echovault/echovault/internal/modules/generic"
	"github.com/echovault/echovault/internal/modules/hash"
	"github.com/echovault/echovault/internal/modules/list"
	"github.com/echovault/echovault/internal/modules/pubsub"
	"github.com/echovault/echovault/internal/modules/set"
	"github.com/echovault/echovault/internal/modules/sorted_set"
	str "github.com/echovault/echovault/internal/modules/string"
	"github.com/echovault/echovault/internal/raft"
	"github.com/echovault/echovault/internal/snapshot"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type EchoVault struct {
	// clock is an implementation of a time interface that allows mocking of time functions during testing.
	clock    clock.Clock
	getClock func() clock.Clock

	// config holds the echovault configuration variables.
	config config.Config

	// The current index for the latest connection id.
	// This number is incremented everytime there's a new connection and
	// the new number is the new connection's ID.
	connId atomic.Uint64

	store           map[string]internal.KeyData // Data store to hold the keys and their associated data, expiry time, etc.
	keyLocks        map[string]*sync.RWMutex    // Map to hold all the individual key locks.
	keyCreationLock *sync.Mutex                 // The mutex for creating a new key. Only one goroutine should be able to create a key at a time.

	// Holds all the keys that are currently associated with an expiry.
	keysWithExpiry struct {
		rwMutex sync.RWMutex // Mutex as only one process should be able to update this list at a time.
		keys    []string     // string slice of the volatile keys
	}
	// LFU cache used when eviction policy is allkeys-lfu or volatile-lfu
	lfuCache struct {
		mutex sync.Mutex        // Mutex as only one goroutine can edit the LFU cache at a time.
		cache eviction.CacheLFU // LFU cache represented by a min head.
	}
	// LRU cache used when eviction policy is allkeys-lru or volatile-lru
	lruCache struct {
		mutex sync.Mutex        // Mutex as only one goroutine can edit the LRU at a time.
		cache eviction.CacheLRU // LRU cache represented by a max head.
	}

	// Holds the list of all commands supported by the echovault.
	commands    []internal.Command
	getCommands func() []internal.Command

	raft       *raft.Raft             // The raft replication layer for the echovault.
	memberList *memberlist.MemberList // The memberlist layer for the echovault.

	context context.Context

	acl    *acl.ACL
	getACL func() interface{}

	pubSub    *pubsub.PubSub
	getPubSub func() interface{}

	snapshotInProgress         atomic.Bool      // Atomic boolean that's true when actively taking a snapshot.
	rewriteAOFInProgress       atomic.Bool      // Atomic boolean that's true when actively rewriting AOF file is in progress.
	stateCopyInProgress        atomic.Bool      // Atomic boolean that's true when actively copying state for snapshotting or preamble generation.
	stateMutationInProgress    atomic.Bool      // Atomic boolean that is set to true when state mutation is in progress.
	latestSnapshotMilliseconds atomic.Int64     // Unix epoch in milliseconds
	snapshotEngine             *snapshot.Engine // Snapshot engine for standalone mode
	aofEngine                  *aof.Engine      // AOF engine for standalone mode
}

// WithContext is an options that for the NewEchoVault function that allows you to
// configure a custom context object to be used in EchoVault.
// If you don't provide this option, EchoVault will create its own internal context object.
func WithContext(ctx context.Context) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.context = ctx
	}
}

// WithConfig is an option for the NewEchoVault function that allows you to pass a
// custom configuration to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithConfig(config config.Config) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config = config
	}
}

// NewEchoVault creates a new EchoVault instance.
// This functions accepts the WithContext, WithConfig and WithCommands options.
func NewEchoVault(options ...func(echovault *EchoVault)) (*EchoVault, error) {
	echovault := &EchoVault{
		clock:           clock.NewClock(),
		context:         context.Background(),
		config:          config.DefaultConfig(),
		store:           make(map[string]internal.KeyData),
		keyLocks:        make(map[string]*sync.RWMutex),
		keyCreationLock: &sync.Mutex{},
		commands: func() []internal.Command {
			var commands []internal.Command
			commands = append(commands, acl.Commands()...)
			commands = append(commands, admin.Commands()...)
			commands = append(commands, generic.Commands()...)
			commands = append(commands, hash.Commands()...)
			commands = append(commands, list.Commands()...)
			commands = append(commands, connection.Commands()...)
			commands = append(commands, pubsub.Commands()...)
			commands = append(commands, set.Commands()...)
			commands = append(commands, sorted_set.Commands()...)
			commands = append(commands, str.Commands()...)
			return commands
		}(),
	}

	for _, option := range options {
		option(echovault)
	}

	echovault.context = context.WithValue(
		echovault.context, "ServerID",
		internal.ContextServerID(echovault.config.ServerID),
	)

	// Load .so modules from config
	for _, path := range echovault.config.Modules {
		if err := echovault.LoadModule(path); err != nil {
			log.Println(err)
		}
	}

	// Function for server commands retrieval
	echovault.getCommands = func() []internal.Command {
		return echovault.commands
	}

	// Function for clock retrieval
	echovault.getClock = func() clock.Clock {
		return echovault.clock
	}

	// Set up ACL module
	echovault.acl = acl.NewACL(echovault.config)
	echovault.getACL = func() interface{} {
		return echovault.acl
	}

	// Set up Pub/Sub module
	echovault.pubSub = pubsub.NewPubSub()
	echovault.getPubSub = func() interface{} {
		return echovault.pubSub
	}

	if echovault.isInCluster() {
		echovault.raft = raft.NewRaft(raft.Opts{
			Config:                echovault.config,
			GetCommand:            echovault.getCommand,
			CreateKeyAndLock:      echovault.CreateKeyAndLock,
			SetValue:              echovault.SetValue,
			SetExpiry:             echovault.SetExpiry,
			KeyUnlock:             echovault.KeyUnlock,
			DeleteKey:             echovault.DeleteKey,
			StartSnapshot:         echovault.startSnapshot,
			FinishSnapshot:        echovault.finishSnapshot,
			SetLatestSnapshotTime: echovault.setLatestSnapshot,
			GetHandlerFuncParams:  echovault.getHandlerFuncParams,
			GetState: func() map[string]internal.KeyData {
				state := make(map[string]internal.KeyData)
				for k, v := range echovault.getState() {
					if data, ok := v.(internal.KeyData); ok {
						state[k] = data
					}
				}
				return state
			},
		})
		echovault.memberList = memberlist.NewMemberList(memberlist.Opts{
			Config:           echovault.config,
			HasJoinedCluster: echovault.raft.HasJoinedCluster,
			AddVoter:         echovault.raft.AddVoter,
			RemoveRaftServer: echovault.raft.RemoveServer,
			IsRaftLeader:     echovault.raft.IsRaftLeader,
			ApplyMutate:      echovault.raftApplyCommand,
			ApplyDeleteKey:   echovault.raftApplyDeleteKey,
		})
	} else {
		// Set up standalone snapshot engine
		echovault.snapshotEngine = snapshot.NewSnapshotEngine(
			snapshot.WithClock(echovault.clock),
			snapshot.WithDirectory(echovault.config.DataDir),
			snapshot.WithThreshold(echovault.config.SnapShotThreshold),
			snapshot.WithInterval(echovault.config.SnapshotInterval),
			snapshot.WithStartSnapshotFunc(echovault.startSnapshot),
			snapshot.WithFinishSnapshotFunc(echovault.finishSnapshot),
			snapshot.WithSetLatestSnapshotTimeFunc(echovault.setLatestSnapshot),
			snapshot.WithGetLatestSnapshotTimeFunc(echovault.getLatestSnapshotTime),
			snapshot.WithGetStateFunc(func() map[string]internal.KeyData {
				state := make(map[string]internal.KeyData)
				for k, v := range echovault.getState() {
					if data, ok := v.(internal.KeyData); ok {
						state[k] = data
					}
				}
				return state
			}),
			snapshot.WithSetKeyDataFunc(func(key string, data internal.KeyData) {
				ctx := context.Background()
				if _, err := echovault.CreateKeyAndLock(ctx, key); err != nil {
					log.Println(err)
				}
				if err := echovault.SetValue(ctx, key, data.Value); err != nil {
					log.Println(err)
				}
				echovault.SetExpiry(ctx, key, data.ExpireAt, false)
				echovault.KeyUnlock(ctx, key)
			}),
		)
		// Set up standalone AOF engine
		echovault.aofEngine = aof.NewAOFEngine(
			aof.WithClock(echovault.clock),
			aof.WithDirectory(echovault.config.DataDir),
			aof.WithStrategy(echovault.config.AOFSyncStrategy),
			aof.WithStartRewriteFunc(echovault.startRewriteAOF),
			aof.WithFinishRewriteFunc(echovault.finishRewriteAOF),
			aof.WithGetStateFunc(func() map[string]internal.KeyData {
				state := make(map[string]internal.KeyData)
				for k, v := range echovault.getState() {
					if data, ok := v.(internal.KeyData); ok {
						state[k] = data
					}
				}
				return state
			}),
			aof.WithSetKeyDataFunc(func(key string, value internal.KeyData) {
				ctx := context.Background()
				if _, err := echovault.CreateKeyAndLock(ctx, key); err != nil {
					log.Println(err)
				}
				if err := echovault.SetValue(ctx, key, value.Value); err != nil {
					log.Println(err)
				}
				echovault.SetExpiry(ctx, key, value.ExpireAt, false)
				echovault.KeyUnlock(ctx, key)
			}),
			aof.WithHandleCommandFunc(func(command []byte) {
				_, err := echovault.handleCommand(context.Background(), command, nil, true, false)
				if err != nil {
					log.Println(err)
				}
			}),
		)
	}

	// If eviction policy is not noeviction, start a goroutine to evict keys every 100 milliseconds.
	if echovault.config.EvictionPolicy != constants.NoEviction {
		go func() {
			for {
				<-echovault.clock.After(echovault.config.EvictionInterval)
				if err := echovault.evictKeysWithExpiredTTL(context.Background()); err != nil {
					log.Println(err)
				}
			}
		}()
	}

	if echovault.config.TLS && len(echovault.config.CertKeyPairs) <= 0 {
		return nil, errors.New("must provide certificate and key file paths for TLS mode")
	}

	if echovault.isInCluster() {
		// Initialise raft and memberlist
		echovault.raft.RaftInit(echovault.context)
		echovault.memberList.MemberListInit(echovault.context)
		if echovault.raft.IsRaftLeader() {
			echovault.initialiseCaches()
		}
	}

	if !echovault.isInCluster() {
		echovault.initialiseCaches()
		// Restore from AOF by default if it's enabled
		if echovault.config.RestoreAOF {
			err := echovault.aofEngine.Restore()
			if err != nil {
				log.Println(err)
			}
		}

		// Restore from snapshot if snapshot restore is enabled and AOF restore is disabled
		if echovault.config.RestoreSnapshot && !echovault.config.RestoreAOF {
			err := echovault.snapshotEngine.Restore()
			if err != nil {
				log.Println(err)
			}
		}
	}

	return echovault, nil
}

func (server *EchoVault) startTCP() {
	conf := server.config

	listenConfig := net.ListenConfig{
		KeepAlive: 200 * time.Millisecond,
	}

	listener, err := listenConfig.Listen(server.context, "tcp", fmt.Sprintf("%s:%d", conf.BindAddr, conf.Port))

	if err != nil {
		log.Fatal(err)
	}

	if !conf.TLS {
		// TCP
		fmt.Printf("Starting TCP echovault at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
	}

	if conf.TLS || conf.MTLS {
		// TLS
		if conf.TLS {
			fmt.Printf("Starting mTLS echovault at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		} else {
			fmt.Printf("Starting TLS echovault at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		}

		var certificates []tls.Certificate
		for _, certKeyPair := range conf.CertKeyPairs {
			c, err := tls.LoadX509KeyPair(certKeyPair[0], certKeyPair[1])
			if err != nil {
				log.Fatal(err)
			}
			certificates = append(certificates, c)
		}

		clientAuth := tls.NoClientCert
		clientCerts := x509.NewCertPool()

		if conf.MTLS {
			clientAuth = tls.RequireAndVerifyClientCert
			for _, c := range conf.ClientCAs {
				ca, err := os.Open(c)
				if err != nil {
					log.Fatal(err)
				}
				certBytes, err := io.ReadAll(ca)
				if err != nil {
					log.Fatal(err)
				}
				if ok := clientCerts.AppendCertsFromPEM(certBytes); !ok {
					log.Fatal(err)
				}
			}
		}

		listener = tls.NewListener(listener, &tls.Config{
			Certificates: certificates,
			ClientAuth:   clientAuth,
			ClientCAs:    clientCerts,
		})
	}

	// Listen to connection
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Could not establish connection")
			continue
		}
		// Read loop for connection
		go server.handleConnection(conn)
	}
}

func (server *EchoVault) handleConnection(conn net.Conn) {
	// If ACL module is loaded, register the connection with the ACL
	if server.acl != nil {
		server.acl.RegisterConnection(&conn)
	}

	w, r := io.Writer(conn), io.Reader(conn)

	cid := server.connId.Add(1)
	ctx := context.WithValue(server.context, internal.ContextConnID("ConnectionID"),
		fmt.Sprintf("%s-%d", server.context.Value(internal.ContextServerID("ServerID")), cid))

	for {
		message, err := internal.ReadMessage(r)

		if err != nil && errors.Is(err, io.EOF) {
			// Connection closed
			log.Println(err)
			break
		}

		if err != nil {
			log.Println(err)
			break
		}

		res, err := server.handleCommand(ctx, message, &conn, false, false)

		if err != nil && errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			if _, err = w.Write([]byte(fmt.Sprintf("-Error %s\r\n", err.Error()))); err != nil {
				log.Println(err)
			}
			continue
		}

		chunkSize := 1024

		// If the length of the response is 0, return nothing to the client
		if len(res) == 0 {
			continue
		}

		if len(res) <= chunkSize {
			_, _ = w.Write(res)
			continue
		}

		// If the response is large, send it in chunks.
		startIndex := 0
		for {
			// If the current start index is less than chunkSize from length, return the remaining bytes.
			if len(res)-1-startIndex < chunkSize {
				_, err = w.Write(res[startIndex:])
				if err != nil {
					log.Println(err)
				}
				break
			}
			n, _ := w.Write(res[startIndex : startIndex+chunkSize])
			if n < chunkSize {
				break
			}
			startIndex += chunkSize
		}
	}

	if err := conn.Close(); err != nil {
		log.Println(err)
	}
}

// Start starts the EchoVault instance's TCP listener.
// This allows the instance to accept connections handle client commands over TCP.
//
// You can still use command functions like echovault.Set if you're embedding EchoVault in your application.
// However, if you'd like to also accept TCP request on the same instance, you must call this function.
func (server *EchoVault) Start() {
	server.startTCP()
}

// takeSnapshot triggers a snapshot when called.
func (server *EchoVault) takeSnapshot() error {
	if server.snapshotInProgress.Load() {
		return errors.New("snapshot already in progress")
	}

	go func() {
		if server.isInCluster() {
			// Handle snapshot in cluster mode
			if err := server.raft.TakeSnapshot(); err != nil {
				log.Println(err)
			}
			return
		}
		// Handle snapshot in standalone mode
		if err := server.snapshotEngine.TakeSnapshot(); err != nil {
			log.Println(err)
		}
	}()

	return nil
}

func (server *EchoVault) startSnapshot() {
	server.snapshotInProgress.Store(true)
}

func (server *EchoVault) finishSnapshot() {
	server.snapshotInProgress.Store(false)
}

func (server *EchoVault) setLatestSnapshot(msec int64) {
	server.latestSnapshotMilliseconds.Store(msec)
}

// getLatestSnapshotTime returns the latest snapshot time in unix epoch milliseconds.
func (server *EchoVault) getLatestSnapshotTime() int64 {
	return server.latestSnapshotMilliseconds.Load()
}

func (server *EchoVault) startRewriteAOF() {
	server.rewriteAOFInProgress.Store(true)
}

func (server *EchoVault) finishRewriteAOF() {
	server.rewriteAOFInProgress.Store(false)
}

// rewriteAOF triggers an AOF compaction when running in standalone mode.
func (server *EchoVault) rewriteAOF() error {
	if server.rewriteAOFInProgress.Load() {
		return errors.New("aof rewrite in progress")
	}
	go func() {
		if err := server.aofEngine.RewriteLog(); err != nil {
			log.Println(err)
		}
	}()
	return nil
}

// ShutDown gracefully shuts down the EchoVault instance.
// This function shuts down the memberlist and raft layers.
func (server *EchoVault) ShutDown() {
	if server.isInCluster() {
		server.raft.RaftShutdown()
		server.memberList.MemberListShutdown()
	}
}

func (server *EchoVault) initialiseCaches() {
	// Set up LFU cache
	server.lfuCache = struct {
		mutex sync.Mutex
		cache eviction.CacheLFU
	}{
		mutex: sync.Mutex{},
		cache: eviction.NewCacheLFU(),
	}
	// set up LRU cache
	server.lruCache = struct {
		mutex sync.Mutex
		cache eviction.CacheLRU
	}{
		mutex: sync.Mutex{},
		cache: eviction.NewCacheLRU(),
	}
}
