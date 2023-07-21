package main

import "bufio"

type PingCommand struct {
	name        string
	commands    []string
	description string
}

func (p *PingCommand) Name() string {
	return p.name
}

func (p *PingCommand) Commands() []string {
	return p.commands
}

func (p *PingCommand) Description() string {
	return p.description
}

func (p *PingCommand) HandleCommand(cmd []string, server *Server, conn *bufio.Writer) {
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

func NewPingCommand() *PingCommand {
	return &PingCommand{
		name:        "PingCommand",
		commands:    []string{"ping"},
		description: "Handle PING command",
	}
}
