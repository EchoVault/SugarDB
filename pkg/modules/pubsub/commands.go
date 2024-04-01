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

package pubsub

import (
	"context"
	"errors"
	"fmt"
	internal_pubsub "github.com/echovault/echovault/internal/pubsub"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/types"
	"net"
	"strings"
)

func handleSubscribe(ctx context.Context, cmd []string, server types.EchoVault, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*internal_pubsub.PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	channels := cmd[1:]

	if len(channels) == 0 {
		return nil, errors.New(constants.WrongArgsResponse)
	}

	withPattern := strings.EqualFold(cmd[0], "psubscribe")
	pubsub.Subscribe(ctx, conn, channels, withPattern)

	return nil, nil
}

func handleUnsubscribe(ctx context.Context, cmd []string, server types.EchoVault, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*internal_pubsub.PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	channels := cmd[1:]

	withPattern := strings.EqualFold(cmd[0], "punsubscribe")

	return pubsub.Unsubscribe(ctx, conn, channels, withPattern), nil
}

func handlePublish(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*internal_pubsub.PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	if len(cmd) != 3 {
		return nil, errors.New(constants.WrongArgsResponse)
	}
	pubsub.Publish(ctx, cmd[2], cmd[1])
	return []byte(constants.OkResponse), nil
}

func handlePubSubChannels(_ context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	if len(cmd) > 3 {
		return nil, errors.New(constants.WrongArgsResponse)
	}

	pubsub, ok := server.GetPubSub().(*internal_pubsub.PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	pattern := ""
	if len(cmd) == 3 {
		pattern = cmd[2]
	}

	return pubsub.Channels(pattern), nil
}

func handlePubSubNumPat(_ context.Context, _ []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*internal_pubsub.PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	num := pubsub.NumPat()
	return []byte(fmt.Sprintf(":%d\r\n", num)), nil
}

func handlePubSubNumSubs(_ context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*internal_pubsub.PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	return pubsub.NumSub(cmd[2:]), nil
}

func Commands() []types.Command {
	return []types.Command{
		{
			Command:     "subscribe",
			Module:      constants.PubSubModule,
			Categories:  []string{constants.PubSubCategory, constants.ConnectionCategory, constants.SlowCategory},
			Description: "(SUBSCRIBE channel [channel ...]) Subscribe to one or more channels.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channels as keys
				if len(cmd) < 2 {
					return nil, errors.New(constants.WrongArgsResponse)
				}
				return cmd[1:], nil
			},
			HandlerFunc: handleSubscribe,
		},
		{
			Command:     "psubscribe",
			Module:      constants.PubSubModule,
			Categories:  []string{constants.PubSubCategory, constants.ConnectionCategory, constants.SlowCategory},
			Description: "(PSUBSCRIBE pattern [pattern ...]) Subscribe to one or more glob patterns.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the patterns as keys
				if len(cmd) < 2 {
					return nil, errors.New(constants.WrongArgsResponse)
				}
				return cmd[1:], nil
			},
			HandlerFunc: handleSubscribe,
		},
		{
			Command:     "publish",
			Module:      constants.PubSubModule,
			Categories:  []string{constants.PubSubCategory, constants.FastCategory},
			Description: "(PUBLISH channel message) Publish a message to the specified channel.",
			Sync:        true,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channel as a key
				if len(cmd) != 3 {
					return nil, errors.New(constants.WrongArgsResponse)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handlePublish,
		},
		{
			Command:    "unsubscribe",
			Module:     constants.PubSubModule,
			Categories: []string{constants.PubSubCategory, constants.ConnectionCategory, constants.SlowCategory},
			Description: `(UNSUBSCRIBE [channel [channel ...]]) Unsubscribe from a list of channels.
If the channel list is not provided, then the connection will be unsubscribed from all the channels that
it's currently subscribe to.`,
			Sync: false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channels as keys
				return cmd[1:], nil
			},
			HandlerFunc: handleUnsubscribe,
		},
		{
			Command:    "punsubscribe",
			Module:     constants.PubSubModule,
			Categories: []string{constants.PubSubCategory, constants.ConnectionCategory, constants.SlowCategory},
			Description: `(PUNSUBSCRIBE [pattern [pattern ...]]) Unsubscribe from a list of channels using patterns.
If the pattern list is not provided, then the connection will be unsubscribed from all the patterns that
it's currently subscribe to.`,
			Sync: false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channels as keys
				return cmd[1:], nil
			},
			HandlerFunc: handleUnsubscribe,
		},
		{
			Command:           "pubsub",
			Module:            constants.PubSubModule,
			Categories:        []string{},
			Description:       "",
			Sync:              false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
			HandlerFunc: func(_ context.Context, _ []string, _ types.EchoVault, _ *net.Conn) ([]byte, error) {
				return nil, errors.New("provide CHANNELS, NUMPAT, or NUMSUB subcommand")
			},
			SubCommands: []types.SubCommand{
				{
					Command:    "channels",
					Module:     constants.PubSubModule,
					Categories: []string{constants.PubSubCategory, constants.SlowCategory},
					Description: `(PUBSUB CHANNELS [pattern]) Returns an array containing the list of channels that
match the given pattern. If no pattern is provided, all active channels are returned. Active channels are 
channels with 1 or more subscribers.`,
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
					HandlerFunc:       handlePubSubChannels,
				},
				{
					Command:           "numpat",
					Module:            constants.PubSubModule,
					Categories:        []string{constants.PubSubCategory, constants.SlowCategory},
					Description:       `(PUBSUB NUMPAT) Return the number of patterns that are currently subscribed to by clients.`,
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
					HandlerFunc:       handlePubSubNumPat,
				},
				{
					Command:    "numsub",
					Module:     constants.PubSubModule,
					Categories: []string{constants.PubSubCategory, constants.SlowCategory},
					Description: `(PUBSUB NUMSUB [channel [channel ...]]) Return an array of arrays containing the provided
channel name and how many clients are currently subscribed to the channel.`,
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return cmd[2:], nil },
					HandlerFunc:       handlePubSubNumSubs,
				},
			},
		},
	}
}
