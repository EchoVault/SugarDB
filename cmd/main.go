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

package main

import (
	"context"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/pkg/echovault"
	"github.com/echovault/echovault/pkg/modules/acl"
	"github.com/echovault/echovault/pkg/modules/admin"
	"github.com/echovault/echovault/pkg/modules/connection"
	"github.com/echovault/echovault/pkg/modules/generic"
	"github.com/echovault/echovault/pkg/modules/hash"
	"github.com/echovault/echovault/pkg/modules/list"
	"github.com/echovault/echovault/pkg/modules/pubsub"
	"github.com/echovault/echovault/pkg/modules/set"
	"github.com/echovault/echovault/pkg/modules/sorted_set"
	str "github.com/echovault/echovault/pkg/modules/string"
	"github.com/echovault/echovault/pkg/utils"
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
	config, err := internal.GetConfig()

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

	server := echovault.NewEchoVault(
		echovault.WithContext(ctx),
		echovault.WithConfig(config),
		echovault.WithACL(acl.NewACL(config)),
		echovault.WithPubSub(pubsub.NewPubSub()),
		echovault.WithCommands(GetCommands()),
	)

	go server.Start(ctx)

	<-cancelCh

	server.ShutDown(ctx)
}
