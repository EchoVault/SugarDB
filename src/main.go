package main

import (
	"context"
	"github.com/echovault/echovault/src/modules/acl"
	"github.com/echovault/echovault/src/modules/admin"
	"github.com/echovault/echovault/src/modules/connection"
	"github.com/echovault/echovault/src/modules/generic"
	"github.com/echovault/echovault/src/modules/hash"
	"github.com/echovault/echovault/src/modules/list"
	"github.com/echovault/echovault/src/modules/pubsub"
	"github.com/echovault/echovault/src/modules/set"
	"github.com/echovault/echovault/src/modules/sorted_set"
	str "github.com/echovault/echovault/src/modules/string"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func GetCommands() []utils.Command {
	var commands []utils.Command
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
}

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

	s := server.NewServer(server.Opts{
		Config:   config,
		ACL:      acl.NewACL(config),
		PubSub:   pubsub.NewPubSub(),
		CancelCh: &cancelCh,
		Commands: GetCommands(),
	})

	go s.Start(ctx)

	<-cancelCh

	s.ShutDown(ctx)
}
