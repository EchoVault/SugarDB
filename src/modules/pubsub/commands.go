package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
	"strings"
)

func handleSubscribe(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	channels := cmd[1:]

	if len(channels) == 0 {
		return nil, errors.New(utils.WrongArgsResponse)
	}

	withPattern := strings.EqualFold(cmd[0], "psubscribe")
	pubsub.Subscribe(ctx, conn, channels, withPattern)

	return nil, nil
}

func handleUnsubscribe(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	channels := cmd[1:]

	withPattern := strings.EqualFold(cmd[0], "punsubscribe")

	return pubsub.Unsubscribe(ctx, conn, channels, withPattern), nil
}

func handlePublish(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	if len(cmd) != 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	pubsub.Publish(ctx, cmd[2], cmd[1])
	fmt.Println("PUBLISHED:", cmd[2])
	return []byte(utils.OkResponse), nil
}

func handlePubSubChannels(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) > 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}

	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	pattern := ""
	if len(cmd) == 3 {
		pattern = cmd[2]
	}

	return pubsub.Channels(ctx, pattern), nil
}

func handlePubSubNumPat(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	num := pubsub.NumPat(ctx)
	return []byte(fmt.Sprintf(":%d\r\n", num)), nil
}

func handlePubSubNumSubs(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	return pubsub.NumSub(ctx, cmd[2:]), nil
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:     "subscribe",
			Categories:  []string{utils.PubSubCategory, utils.ConnectionCategory, utils.SlowCategory},
			Description: "(SUBSCRIBE channel [channel ...]) Subscribe to one or more channels.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channels as keys
				if len(cmd) < 2 {
					return nil, errors.New(utils.WrongArgsResponse)
				}
				return cmd[1:], nil
			},
			HandlerFunc: handleSubscribe,
		},
		{
			Command:     "psubscribe",
			Categories:  []string{utils.PubSubCategory, utils.ConnectionCategory, utils.SlowCategory},
			Description: "(PSUBSCRIBE pattern [pattern ...]) Subscribe to one or more glob patterns.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the patterns as keys
				if len(cmd) < 2 {
					return nil, errors.New(utils.WrongArgsResponse)
				}
				return cmd[1:], nil
			},
			HandlerFunc: handleSubscribe,
		},
		{
			Command:     "publish",
			Categories:  []string{utils.PubSubCategory, utils.FastCategory},
			Description: "(PUBLISH channel message) Publish a message to the specified channel.",
			Sync:        true,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channel as a key
				if len(cmd) != 3 {
					return nil, errors.New(utils.WrongArgsResponse)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handlePublish,
		},
		{
			Command:    "unsubscribe",
			Categories: []string{utils.PubSubCategory, utils.ConnectionCategory, utils.SlowCategory},
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
			Categories: []string{utils.PubSubCategory, utils.ConnectionCategory, utils.SlowCategory},
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
			Categories:        []string{},
			Description:       "",
			Sync:              false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
			HandlerFunc: func(_ context.Context, _ []string, _ utils.Server, _ *net.Conn) ([]byte, error) {
				return nil, errors.New("provide CHANNELS, NUMPAT, or NUMSUB subcommand")
			},
			SubCommands: []utils.SubCommand{
				{
					Command:    "channels",
					Categories: []string{utils.PubSubCategory, utils.SlowCategory},
					Description: `(PUBSUB CHANNELS [pattern]) Returns an array containing the list of channels that
match the given pattern. If no pattern is provided, all active channels are returned. Active channels are 
channels with 1 or more subscribers.`,
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
					HandlerFunc:       handlePubSubChannels,
				},
				{
					Command:           "numpat",
					Categories:        []string{utils.PubSubCategory, utils.SlowCategory},
					Description:       `(PUBSUB NUMPAT) Return the number of patterns that are currently subscribed to by clients.`,
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
					HandlerFunc:       handlePubSubNumPat,
				},
				{
					Command:    "numsub",
					Categories: []string{utils.PubSubCategory, utils.SlowCategory},
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
