package main

import (
	"context"
	"github.com/kelvinmwinuka/memstore/src/modules/acl"
	"github.com/kelvinmwinuka/memstore/src/modules/pubsub"
	"github.com/kelvinmwinuka/memstore/src/server"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

func main() {
	config, err := utils.GetConfig()

	if err != nil {
		log.Fatal(err)
	}

	ctx := context.WithValue(context.Background(), utils.ContextServerID("ServerID"), config.ServerID)

	// Default BindAddr if it's not specified
	if config.BindAddr == "" {
		if addr, err := utils.GetIPAddress(); err != nil {
			log.Fatal(err)
		} else {
			config.BindAddr = addr
		}
	}

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	s := &server.Server{
		Config:   config,
		ConnID:   atomic.Uint64{},
		ACL:      acl.NewACL(config),
		PubSub:   pubsub.NewPubSub(),
		CancelCh: &cancelCh,
	}

	go s.Start(ctx)

	<-cancelCh

	s.ShutDown(ctx)
}
