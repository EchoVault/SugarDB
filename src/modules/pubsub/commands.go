package pubsub

import (
	"context"
	"errors"
	"github.com/echovault/echovault/src/utils"
	"net"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	description string
	pubSub      *PubSub
}

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() []utils.Command {
	return p.commands
}

func (p Plugin) Description() string {
	return p.description
}

func handleSubscribe(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub")
	}
	switch len(cmd) {
	case 2:
		// Subscribe to specified channel
		pubsub.Subscribe(ctx, conn, cmd[1], nil)
	case 3:
		// Subscribe to specified channel and specified consumer group
		pubsub.Subscribe(ctx, conn, cmd[1], cmd[2])
	default:
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []byte("+SUBSCRIBE_OK\r\n\r\n"), nil
}

func handleUnsubscribe(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub")
	}
	switch len(cmd) {
	case 1:
		pubsub.Unsubscribe(ctx, conn, nil)
	case 2:
		pubsub.Unsubscribe(ctx, conn, cmd[1])
	default:
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []byte(utils.OK_RESPONSE), nil
}

func handlePublish(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	pubsub, ok := server.GetPubSub().(*PubSub)
	if !ok {
		return nil, errors.New("could not load pubsub")
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
			Command:     "subscribe",
			Categories:  []string{utils.PubSubCategory, utils.ConnectionCategory, utils.SlowCategory},
			Description: "(SUBSCRIBE channel [consumer_group]) Subscribe to a channel with an option to join a consumer group on the channel.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channel as a key
				if len(cmd) < 2 {
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handleSubscribe,
		},
		{
			Command:     "unsubscribe",
			Categories:  []string{utils.PubSubCategory, utils.ConnectionCategory, utils.SlowCategory},
			Description: "(UNSUBSCRIBE channel) Unsubscribe from a channel.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				// Treat the channel as a key
				if len(cmd) != 2 {
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handleUnsubscribe,
		},
	}
}
