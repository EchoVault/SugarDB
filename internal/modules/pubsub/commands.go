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
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/constants"
	"strings"
)

func handleSubscribe(params internal.HandlerFuncParams) ([]byte, error) {
	pubsub, ok := params.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	channels := params.Command[1:]

	if len(channels) == 0 {
		return nil, errors.New(constants.WrongArgsResponse)
	}

	withPattern := strings.EqualFold(params.Command[0], "psubscribe")
	pubsub.Subscribe(params.Context, params.Connection, channels, withPattern)

	return nil, nil
}

func handleUnsubscribe(params internal.HandlerFuncParams) ([]byte, error) {
	pubsub, ok := params.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	channels := params.Command[1:]

	withPattern := strings.EqualFold(params.Command[0], "punsubscribe")

	return pubsub.Unsubscribe(params.Context, params.Connection, channels, withPattern), nil
}

func handlePublish(params internal.HandlerFuncParams) ([]byte, error) {
	pubsub, ok := params.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	if len(params.Command) != 3 {
		return nil, errors.New(constants.WrongArgsResponse)
	}
	pubsub.Publish(params.Context, params.Command[2], params.Command[1])
	return []byte(constants.OkResponse), nil
}

func handlePubSubChannels(params internal.HandlerFuncParams) ([]byte, error) {
	if len(params.Command) > 3 {
		return nil, errors.New(constants.WrongArgsResponse)
	}

	pubsub, ok := params.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	pattern := ""
	if len(params.Command) == 3 {
		pattern = params.Command[2]
	}

	return pubsub.Channels(pattern), nil
}

func handlePubSubNumPat(params internal.HandlerFuncParams) ([]byte, error) {
	pubsub, ok := params.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	num := pubsub.NumPat()
	return []byte(fmt.Sprintf(":%d\r\n", num)), nil
}

func handlePubSubNumSubs(params internal.HandlerFuncParams) ([]byte, error) {
	pubsub, ok := params.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	return pubsub.NumSub(params.Command[2:]), nil
}

func Commands() []internal.Command {
	return []internal.Command{
		{
			Command:     "subscribe",
			Module:      constants.PubSubModule,
			Categories:  []string{constants.PubSubCategory, constants.ConnectionCategory, constants.SlowCategory},
			Description: "(SUBSCRIBE channel [channel ...]) Subscribe to one or more channels.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				// Treat the channels as keys
				if len(cmd) < 2 {
					return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
				}
				return internal.KeyExtractionFuncResult{
					Channels:  cmd[1:],
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handleSubscribe,
		},
		{
			Command:     "psubscribe",
			Module:      constants.PubSubModule,
			Categories:  []string{constants.PubSubCategory, constants.ConnectionCategory, constants.SlowCategory},
			Description: "(PSUBSCRIBE pattern [pattern ...]) Subscribe to one or more glob patterns.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				// Treat the patterns as keys
				if len(cmd) < 2 {
					return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
				}
				return internal.KeyExtractionFuncResult{
					Channels:  cmd[1:],
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handleSubscribe,
		},
		{
			Command:     "publish",
			Module:      constants.PubSubModule,
			Categories:  []string{constants.PubSubCategory, constants.FastCategory},
			Description: "(PUBLISH channel message) Publish a message to the specified channel.",
			Sync:        true,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				// Treat the channel as a key
				if len(cmd) != 3 {
					return internal.KeyExtractionFuncResult{}, errors.New(constants.WrongArgsResponse)
				}
				return internal.KeyExtractionFuncResult{
					Channels:  cmd[1:2],
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
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
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				// Treat the channels as keys
				return internal.KeyExtractionFuncResult{
					Channels:  cmd[1:],
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
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
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels:  cmd[1:],
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handleUnsubscribe,
		},
		{
			Command:     "pubsub",
			Module:      constants.PubSubModule,
			Categories:  []string{},
			Description: "",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels:  make([]string, 0),
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: func(_ internal.HandlerFuncParams) ([]byte, error) {
				return nil, errors.New("provide CHANNELS, NUMPAT, or NUMSUB subcommand")
			},
			SubCommands: []internal.SubCommand{
				{
					Command:    "channels",
					Module:     constants.PubSubModule,
					Categories: []string{constants.PubSubCategory, constants.SlowCategory},
					Description: `(PUBSUB CHANNELS [pattern]) Returns an array containing the list of channels that
match the given pattern. If no pattern is provided, all active channels are returned. Active channels are 
channels with 1 or more subscribers.`,
					Sync: false,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels:  make([]string, 0),
							ReadKeys:  make([]string, 0),
							WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: handlePubSubChannels,
				},
				{
					Command:     "numpat",
					Module:      constants.PubSubModule,
					Categories:  []string{constants.PubSubCategory, constants.SlowCategory},
					Description: `(PUBSUB NUMPAT) Return the number of patterns that are currently subscribed to by clients.`,
					Sync:        false,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels:  make([]string, 0),
							ReadKeys:  make([]string, 0),
							WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: handlePubSubNumPat,
				},
				{
					Command:    "numsub",
					Module:     constants.PubSubModule,
					Categories: []string{constants.PubSubCategory, constants.SlowCategory},
					Description: `(PUBSUB NUMSUB [channel [channel ...]]) Return an array of arrays containing the provided
channel name and how many clients are currently subscribed to the channel.`,
					Sync: false,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels:  cmd[2:],
							ReadKeys:  make([]string, 0),
							WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: handlePubSubNumSubs,
				},
			},
		},
	}
}
