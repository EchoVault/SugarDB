package main

import "bufio"

const (
	OK = "+OK\r\n\n"
)

type Server interface {
	Lock()
	Unlock()
	GetData(key string) interface{}
	SetData(key string, value interface{})
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

func (p *plugin) HandleCommand(cmd []string, server interface{}, conn *bufio.Writer) {
	switch len(cmd) {
	default:
		conn.Write([]byte("-Error wrong number of arguments for PING command\r\n\n"))
		conn.Flush()
	case 1:
		conn.Write([]byte("+PONG\r\n\n"))
		conn.Flush()
	case 2:
		conn.Write([]byte("+" + cmd[1] + "\r\n\n"))
		conn.Flush()
	}
}

func init() {
	Plugin.name = "PingCommand"
	Plugin.commands = []string{"ping"}
	Plugin.description = "Handle PING command"
}
