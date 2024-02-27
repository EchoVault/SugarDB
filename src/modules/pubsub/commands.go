package pubsub

import (
	"context"
	"errors"
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
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	switch strings.ToLower(cmd[0]) {
	case "subscribe":
		pubsub.Subscribe(ctx, conn, channels, false)
	case "psubscribe":
		pubsub.Subscribe(ctx, conn, channels, true)
	}

	return []byte{}, nil
}

func handleUnsubscribe(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}

	channels := cmd[1:]

	if len(channels) == 0 {
		pubsub.Unsubscribe(ctx, conn, "*")
		return []byte(utils.OK_RESPONSE), nil
	}

	for _, channel := range channels {
		pubsub.Unsubscribe(ctx, conn, channel)
	}

	return []byte(utils.OK_RESPONSE), nil
}

func handlePublish(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub module")
	}
	if len(cmd) != 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	pubsub.Publish(ctx, cmd[2], cmd[1])
	return []byte(utils.OK_RESPONSE), nil
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
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
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
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
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
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
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
	}
}
