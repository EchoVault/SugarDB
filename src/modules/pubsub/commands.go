package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"strings"
)

const (
	OK = "+OK\r\n\n"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	description string
	pubSub      *PubSub
}

var PubSubModule Plugin

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() []utils.Command {
	fmt.Println(p)
	return p.commands
}

func (p Plugin) Description() string {
	return p.description
}

func (p Plugin) HandleCommand(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "publish":
		return handlePublish(ctx, p, cmd, server)
	case "subscribe":
		return handleSubscribe(ctx, p, cmd, server, conn)
	case "unsubscribe":
		return handleUnsubscribe(ctx, p, cmd, server, conn)
	}
}

func handleSubscribe(ctx context.Context, p Plugin, cmd []string, s utils.Server, conn *net.Conn) ([]byte, error) {
	switch len(cmd) {
	case 1:
		// Subscribe to all channels
		p.pubSub.Subscribe(ctx, conn, nil, nil)
	case 2:
		// Subscribe to specified channel
		p.pubSub.Subscribe(ctx, conn, cmd[1], nil)
	case 3:
		// Subscribe to specified channel and specified consumer group
		fmt.Println(p.pubSub)
		p.pubSub.Subscribe(ctx, conn, cmd[1], cmd[2])
	default:
		return nil, errors.New("wrong number of arguments")
	}
	return []byte("+SUBSCRIBE_OK\r\n\n"), nil
}

func handleUnsubscribe(ctx context.Context, p Plugin, cmd []string, s utils.Server, conn *net.Conn) ([]byte, error) {
	switch len(cmd) {
	case 1:
		p.pubSub.Unsubscribe(ctx, conn, nil)
	case 2:
		p.pubSub.Unsubscribe(ctx, conn, cmd[1])
	default:
		return nil, errors.New("wrong number of arguments")
	}
	return []byte("+OK\r\n\n"), nil
}

func handlePublish(ctx context.Context, p Plugin, cmd []string, s utils.Server) ([]byte, error) {
	if len(cmd) == 3 {
		p.pubSub.Publish(ctx, cmd[2], cmd[1])
	} else if len(cmd) == 2 {
		p.pubSub.Publish(ctx, cmd[1], nil)
	} else {
		return nil, errors.New("wrong number of arguments")
	}
	return []byte("+PUBLISH_OK\r\n\n"), nil
}

func NewModule(pubsub *PubSub) Plugin {
	PubSubModule := Plugin{
		pubSub: pubsub,
		name:   "PubSubCommands",
		commands: []utils.Command{
			{
				Command:     "publish",
				Categories:  []string{},
				Description: "(PUBLISH channel message) Publish a message to the specified channel.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					// Treat the channel as a key
					if len(cmd) != 3 {
						return nil, errors.New("wrong number of arguments")
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "subscribe",
				Categories:  []string{},
				Description: "(SUBSCRIBE channel [consumer_group]) Subscribe to a channel with an option to join a consumer group on the channel.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					// Treat the channel as a key
					if len(cmd) < 2 {
						return nil, errors.New("wrong number of arguments")
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "unsubscribe",
				Categories:  []string{},
				Description: "(UNSUBSCRIBE channel) Unsubscribe from a channel.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					// Treat the channel as a key
					if len(cmd) != 2 {
						return nil, errors.New("wrong number of arguments")
					}
					return []string{cmd[1]}, nil
				},
			},
		},
		description: "Handle PUBSUB feature",
	}
	return PubSubModule
}
