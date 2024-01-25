package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/echovault/echovault/src/memberlist"
	"github.com/echovault/echovault/src/modules/acl"
	"github.com/echovault/echovault/src/modules/pubsub"
	"github.com/echovault/echovault/src/raft"
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

	commands []utils.Command

	raft       *raft.Raft
	memberList *memberlist.MemberList

	CancelCh *chan os.Signal

	ACL    *acl.ACL
	PubSub *pubsub.PubSub
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

	if conf.TLS {
		// TLS
		fmt.Printf("Starting TLS server at Address %s, Port %d...\n", conf.BindAddr, conf.Port)
		cer, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
		if err != nil {
			log.Fatal(err)
		}

		listener = tls.NewListener(listener, &tls.Config{
			Certificates: []tls.Certificate{cer},
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

		if err != nil {
			if err == io.EOF {
				// Connection closed
				// TODO: Remove this connection from channel subscriptions
				break
			}
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// Connection timeout
				fmt.Println(err)
				break
			}
			if err, ok := err.(tls.RecordHeaderError); ok {
				// TLS verification error
				fmt.Println(err)
				break
			}
			fmt.Println(err)
			break
		}

		if cmd, err := utils.Decode(message); err != nil {
			// Return error to client
			if _, err := w.Write([]byte(fmt.Sprintf("-Error %s\r\n\r\n", err.Error()))); err != nil {
				// TODO: Log error at configured logger
				fmt.Println(err)
			}
			continue
		} else {
			command, err := server.getCommand(cmd[0])

			if err != nil {
				if _, err := w.Write([]byte(fmt.Sprintf("-%s\r\n\r\n", err.Error()))); err != nil {
					// TODO: Log error at configured logger
					fmt.Println(err)
				}
				continue
			}

			synchronize := command.Sync
			handler := command.HandlerFunc

			subCommand, ok := utils.GetSubCommand(command, cmd).(utils.SubCommand)

			if ok {
				synchronize = subCommand.Sync
				handler = subCommand.HandlerFunc
			}

			if err := server.ACL.AuthorizeConnection(&conn, cmd, command, subCommand); err != nil {
				if _, err := w.Write([]byte(fmt.Sprintf("-%s\r\n\r\n", err.Error()))); err != nil {
					// TODO: Log error at configured logger
					fmt.Println(err)
				}
				continue
			}

			if !server.IsInCluster() || !synchronize {
				if res, err := handler(ctx, cmd, server, &conn); err != nil {
					if _, err := w.Write([]byte(fmt.Sprintf("-%s\r\n\r\n", err.Error()))); err != nil {
						// TODO: Log error at configured logger
						fmt.Println(err)
					}
				} else {
					if _, err := w.Write(res); err != nil {
						// TODO: Log error at configured logger
						fmt.Println(err)
					}
					// TODO: Write successful, add entry to AOF
				}
				continue
			}

			// Handle other commands that need to be synced across the cluster
			if server.raft.IsRaftLeader() {
				if res, err := server.raftApply(ctx, cmd); err != nil {
					if _, err := w.Write([]byte(fmt.Sprintf("-Error %s\r\n\r\n", err.Error()))); err != nil {
						// TODO: Log error at configured logger
						fmt.Println(err)
					}
				} else {
					if _, err := w.Write(res); err != nil {
						// TODO: Log error at configured logger
						fmt.Println(err)
					}
				}
				continue
			}

			// Forward message to leader and return immediate OK response
			if server.Config.ForwardCommand {
				server.memberList.ForwardDataMutation(ctx, message)
				if _, err := w.Write([]byte(utils.OK_RESPONSE)); err != nil {
					// TODO: Log error at configured logger
					fmt.Println(err)
				}
				continue
			}

			if _, err := w.Write([]byte("-Error not cluster leader, cannot carry out command\r\n\r\n")); err != nil {
				// TODO: Log error at configured logger
				fmt.Println(err)
			}
		}
	}

	if err := conn.Close(); err != nil {
		// TODO: Log error at configured logger
		fmt.Println(err)
	}
}

func (server *Server) Start(ctx context.Context) {
	conf := server.Config

	server.store = make(map[string]interface{})
	server.keyLocks = make(map[string]*sync.RWMutex)
	server.keyCreationLock = &sync.Mutex{}

	server.LoadModules(ctx)

	if conf.TLS && (len(conf.Key) <= 0 || len(conf.Cert) <= 0) {
		fmt.Println("Must provide key and certificate file paths for TLS mode.")
		return
	}

	if server.IsInCluster() {
		// Initialise raft and memberlist
		server.raft = raft.NewRaft(raft.Opts{
			Config:     conf,
			Server:     server,
			GetCommand: server.getCommand,
		})
		server.memberList = memberlist.NewMemberList(memberlist.MemberlistOpts{
			Config:           conf,
			HasJoinedCluster: server.raft.HasJoinedCluster,
			AddVoter:         server.raft.AddVoter,
			RemoveRaftServer: server.raft.RemoveServer,
			IsRaftLeader:     server.raft.IsRaftLeader,
			ApplyMutate:      server.raftApply,
		})
		server.raft.RaftInit(ctx)
		server.memberList.MemberListInit(ctx)
	}

	server.StartTCP(ctx)
}

func (server *Server) TakeSnapshot() error {
	// TODO: Check if there's a snapshot currently in progress
	go func() {
		err := server.raft.TakeSnapshot()
		log.Println(err)
	}()
	return nil
}

func (server *Server) ShutDown(ctx context.Context) {
	if server.IsInCluster() {
		server.raft.RaftShutdown(ctx)
		server.memberList.MemberListShutdown(ctx)
	}
}
