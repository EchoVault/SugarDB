package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/memberlist"
	"github.com/echovault/echovault/src/raft"
	"github.com/echovault/echovault/src/server/aof"
	"github.com/echovault/echovault/src/server/snapshot"
	"github.com/echovault/echovault/src/utils"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	Config utils.Config

	ConnID atomic.Uint64

	store           map[string]interface{}
	keyLocks        map[string]*sync.RWMutex
	keyCreationLock *sync.Mutex

	Commands []utils.Command

	raft       *raft.Raft
	memberList *memberlist.MemberList

	CancelCh *chan os.Signal

	ACL    utils.ACL
	PubSub utils.PubSub

	SnapshotInProgress         atomic.Bool
	RewriteAOFInProgress       atomic.Bool
	StateCopyInProgress        atomic.Bool
	StateMutationInProgress    atomic.Bool
	LatestSnapshotMilliseconds atomic.Int64 // Unix epoch in milliseconds
	SnapshotEngine             *snapshot.Engine
	AOFEngine                  *aof.Engine
}

type Opts struct {
	Config   utils.Config
	ACL      utils.ACL
	PubSub   utils.PubSub
	CancelCh *chan os.Signal
	Commands []utils.Command
}

func NewServer(opts Opts) *Server {
	server := &Server{
		Config:          opts.Config,
		ACL:             opts.ACL,
		PubSub:          opts.PubSub,
		CancelCh:        opts.CancelCh,
		Commands:        opts.Commands,
		store:           make(map[string]interface{}),
		keyLocks:        make(map[string]*sync.RWMutex),
		keyCreationLock: &sync.Mutex{},
	}
	if server.IsInCluster() {
		server.raft = raft.NewRaft(raft.Opts{
			Config:     opts.Config,
			Server:     server,
			GetCommand: server.getCommand,
		})
		server.memberList = memberlist.NewMemberList(memberlist.MemberlistOpts{
			Config:           opts.Config,
			HasJoinedCluster: server.raft.HasJoinedCluster,
			AddVoter:         server.raft.AddVoter,
			RemoveRaftServer: server.raft.RemoveServer,
			IsRaftLeader:     server.raft.IsRaftLeader,
			ApplyMutate:      server.raftApply,
		})
	} else {
		// Set up standalone snapshot engine
		server.SnapshotEngine = snapshot.NewSnapshotEngine(snapshot.Opts{
			Config:                        opts.Config,
			StartSnapshot:                 server.StartSnapshot,
			FinishSnapshot:                server.FinishSnapshot,
			GetState:                      server.GetState,
			SetLatestSnapshotMilliseconds: server.SetLatestSnapshot,
			GetLatestSnapshotMilliseconds: server.GetLatestSnapshot,
			CreateKeyAndLock:              server.CreateKeyAndLock,
			KeyUnlock:                     server.KeyUnlock,
			SetValue:                      server.SetValue,
		})
		// Set up standalone AOF engine
		server.AOFEngine = aof.NewAOFEngine(
			aof.WithDirectory(opts.Config.DataDir),
			aof.WithStrategy(opts.Config.AOFSyncStrategy),
			aof.WithStartRewriteFunc(server.StartRewriteAOF),
			aof.WithFinishRewriteFunc(server.FinishRewriteAOF),
			aof.WithGetStateFunc(server.GetState),
			aof.WithSetValueFunc(func(key string, value interface{}) {
				if _, err := server.CreateKeyAndLock(context.Background(), key); err != nil {
					log.Println(err)
					return
				}
				server.SetValue(context.Background(), key, value)
				server.KeyUnlock(key)
			}),
			aof.WithHandleCommandFunc(func(command []byte) {
				_, err := server.handleCommand(context.Background(), command, nil, true)
				if err != nil {
					log.Println(err)
				}
			}),
		)
	}
	return server
}

func (server *Server) StartTCP(ctx context.Context) {
	conf := server.Config

	listenConfig := net.ListenConfig{
		KeepAlive: 200 * time.Millisecond,
	}

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf("%s:%d", conf.BindAddr, conf.Port))

	if err != nil {
		log.Fatal(err)
	}

	if !conf.TLS {
		// TCP
		fmt.Printf("Starting TCP server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
	}

	if conf.TLS || conf.MTLS {
		// TLS
		if conf.TLS {
			fmt.Printf("Starting mTLS server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		} else {
			fmt.Printf("Starting TLS server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
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
		go server.handleConnection(ctx, conn)
	}
}

func (server *Server) handleConnection(ctx context.Context, conn net.Conn) {
	server.ACL.RegisterConnection(&conn)

	w, r := io.Writer(conn), io.Reader(conn)

	cid := server.ConnID.Add(1)
	ctx = context.WithValue(ctx, utils.ContextConnID("ConnectionID"),
		fmt.Sprintf("%s-%d", ctx.Value(utils.ContextServerID("ServerID")), cid))

	for {
		message, err := utils.ReadMessage(r)

		if err != nil && errors.Is(err, io.EOF) {
			// Connection closed
			log.Println(err)
			break
		}

		if err != nil {
			log.Println(err)
			break
		}

		res, err := server.handleCommand(ctx, message, &conn, false)

		if err != nil {
			if _, err = w.Write([]byte(fmt.Sprintf("-Error %s\r\n", err.Error()))); err != nil {
				log.Println(err)
			}
			continue
		}

		chunkSize := 1024

		if len(res) <= chunkSize {
			_, err = w.Write(res)
			if err != nil {
				log.Println(err)
			}
			continue
		}

		// If the response is large, send it in chunks.
		startIndex := 0
		for {
			// If the current start index is less than chunkSize from length, return the remaining bytes.
			if len(res)-1-startIndex < chunkSize {
				_, _ = w.Write(res[startIndex:])
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

func (server *Server) Start(ctx context.Context) {
	conf := server.Config

	if conf.TLS && len(conf.CertKeyPairs) <= 0 {
		log.Fatal("must provide certificate and key file paths for TLS mode")
		return
	}

	if server.IsInCluster() {
		// Initialise raft and memberlist
		server.raft.RaftInit(ctx)
		server.memberList.MemberListInit(ctx)
	} else {
		// Restore from AOF by default if it's enabled
		if conf.RestoreAOF {
			err := server.AOFEngine.Restore()
			if err != nil {
				log.Println(err)
			}
		}

		// Restore from snapshot if snapshot restore is enabled and AOF restore is disabled
		if conf.RestoreSnapshot && !conf.RestoreAOF {
			err := server.SnapshotEngine.Restore(ctx)
			if err != nil {
				log.Println(err)
			}
		}
		server.SnapshotEngine.Start(ctx)

	}

	server.StartTCP(ctx)
}

func (server *Server) TakeSnapshot() error {
	if server.SnapshotInProgress.Load() {
		return errors.New("snapshot already in progress")
	}

	go func() {
		if server.IsInCluster() {
			// Handle snapshot in cluster mode
			if err := server.raft.TakeSnapshot(); err != nil {
				log.Println(err)
			}
			return
		}
		// Handle snapshot in standalone mode
		if err := server.SnapshotEngine.TakeSnapshot(); err != nil {
			log.Println(err)
		}
	}()

	return nil
}

func (server *Server) StartSnapshot() {
	server.SnapshotInProgress.Store(true)
}

func (server *Server) FinishSnapshot() {
	server.SnapshotInProgress.Store(false)
}

func (server *Server) SetLatestSnapshot(msec int64) {
	server.LatestSnapshotMilliseconds.Store(msec)
}

func (server *Server) GetLatestSnapshot() int64 {
	return server.LatestSnapshotMilliseconds.Load()
}

func (server *Server) StartRewriteAOF() {
	server.RewriteAOFInProgress.Store(true)
}

func (server *Server) FinishRewriteAOF() {
	server.RewriteAOFInProgress.Store(false)
}

func (server *Server) RewriteAOF() error {
	if server.RewriteAOFInProgress.Load() {
		return errors.New("aof rewrite in progress")
	}
	go func() {
		if err := server.AOFEngine.RewriteLog(); err != nil {
			log.Println(err)
		}
	}()
	return nil
}

func (server *Server) ShutDown(ctx context.Context) {
	if server.IsInCluster() {
		server.raft.RaftShutdown(ctx)
		server.memberList.MemberListShutdown(ctx)
	}
}
