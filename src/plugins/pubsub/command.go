package main

import (
	"context"
	"errors"
	"net"
	"strings"
)

const (
	OK = "+OK\r\n\n"
)

type Server interface {
	KeyLock(ctx context.Context, key string) (bool, error)
	KeyUnlock(key string)
	KeyRLock(ctx context.Context, key string) (bool, error)
	KeyRUnlock(key string)
	KeyExists(key string) bool
	CreateKeyAndLock(ctx context.Context, key string) (bool, error)
	GetValue(key string) interface{}
	SetValue(ctx context.Context, key string, value interface{})
}

type plugin struct {
	name        string
	commands    []string
	description string
	pubSub      *PubSub
}

var Plugin plugin

func (p *plugin) Name() string {
	return p.name
}

func (p *plugin) Commands() []string {
	return p.commands
}

func (p *plugin) Description() string {
	return p.description
}

func (p *plugin) HandleCommandWithConnection(ctx context.Context, cmd []string, server interface{}, conn *net.Conn) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "subscribe":
		return handleSubscribe(ctx, p, cmd, server.(Server), conn)
	case "unsubscribe":
		return handleUnsubscribe(ctx, p, cmd, server.(Server), conn)
	}
}

func (p *plugin) HandleCommand(ctx context.Context, cmd []string, server interface{}) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "publish":
		return handlePublish(ctx, p, cmd, server.(Server))
	}
}

func handleSubscribe(ctx context.Context, p *plugin, cmd []string, s Server, conn *net.Conn) ([]byte, error) {
	switch len(cmd) {
	case 1:
		p.pubSub.Subscribe(ctx, conn, nil, nil)
	case 2:
		p.pubSub.Subscribe(ctx, conn, cmd[1], nil)
	case 3:
		p.pubSub.Subscribe(ctx, conn, cmd[1], cmd[2])
	default:
		return nil, errors.New("wrong number of arguments")
	}
	return []byte("+SUBSCRIBE_OK\r\n\n"), nil
}

func handleUnsubscribe(ctx context.Context, p *plugin, cmd []string, s Server, conn *net.Conn) ([]byte, error) {
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

func handlePublish(ctx context.Context, p *plugin, cmd []string, s Server) ([]byte, error) {
	if len(cmd) == 3 {
		p.pubSub.Publish(ctx, cmd[2], cmd[1])
	} else if len(cmd) == 2 {
		p.pubSub.Publish(ctx, cmd[1], nil)
	} else {
		return nil, errors.New("wrong number of arguments")
	}
	return []byte("+PUBLISH_OK\r\n\n"), nil
}

func init() {
	Plugin.name = "PubSubCommands"
	Plugin.commands = []string{"publish", "subscribe", "unsubscribe"}
	Plugin.description = "Handle PUBSUB functionality."
	Plugin.pubSub = NewPubSub()
}
