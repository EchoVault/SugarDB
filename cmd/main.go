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
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/commands"
	"github.com/echovault/echovault/pkg/echovault"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	conf, err := config.GetConfig()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.WithValue(context.Background(), internal.ContextServerID("ServerID"), conf.ServerID)

	// Default BindAddr if it's not specified
	if conf.BindAddr == "" {
		if addr, err := internal.GetIPAddress(); err != nil {
			log.Fatal(err)
		} else {
			conf.BindAddr = addr
		}
	}

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	server := echovault.NewEchoVault(
		echovault.WithContext(ctx),
		echovault.WithConfig(conf),
		echovault.WithCommands(commands.All()),
	)

	go server.Start()

	<-cancelCh

	server.ShutDown()
}
