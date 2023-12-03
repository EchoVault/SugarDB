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
	return nil, errors.New("not implemented")
}

func (p *plugin) HandleCommand(ctx context.Context, cmd []string, server interface{}) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("not implemented")
	case "ping":
		return handlePing(ctx, cmd, server.(Server))
	case "ack":
		return []byte("$-1\r\n\n"), nil
	}
}

func handlePing(ctx context.Context, cmd []string, s Server) ([]byte, error) {
	switch len(cmd) {
	default:
		return nil, errors.New("wrong number of arguments for PING command")
	case 1:
		return []byte("+PONG\r\n\n"), nil
	case 2:
		return []byte("+" + cmd[1] + "\r\n\n"), nil
	}
}

func init() {
	Plugin.name = "PingCommand"
	Plugin.commands = []string{"ping", "ack"}
	Plugin.description = "Handle PING command"
}
