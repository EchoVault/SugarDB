package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"github.com/kelvinmwinuka/memstore/utils"
)

type Server interface {
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
	switch strings.ToLower(cmd[0]) {
	case "get":
		handleGet(cmd, server.(Server), conn)
	case "set":
		handleSet(cmd, server.(Server), conn)
	case "mget":
		handleMGet(cmd, server.(Server), conn)
	}
}

func handleGet(cmd []string, s Server, conn *bufio.Writer) {

	if len(cmd) != 2 {
		conn.Write([]byte("-Error wrong number of args for GET command\r\n\n"))
		conn.Flush()
		return
	}

	value := s.GetData(cmd[1])

	switch value.(type) {
	default:
		fmt.Println("Error. The requested object's type cannot be returned with the GET command")
	case nil:
		conn.Write([]byte("+nil\r\n\n"))
	case string:
		conn.Write([]byte(fmt.Sprintf("+%s\r\n\n", value)))
	case float64:
		conn.Write([]byte(fmt.Sprintf("+%f\r\n\n", value)))
	case int:
		conn.Write([]byte(fmt.Sprintf("+%d\r\n\n", value)))
	}

	conn.Flush()
}

func handleMGet(cmd []string, s Server, conn *bufio.Writer) {
	vals := []string{}

	for _, key := range cmd[1:] {
		switch s.GetData(key).(type) {
		case nil:
			vals = append(vals, "nil")
		case string:
			vals = append(vals, fmt.Sprintf("%s", s.GetData(key)))
		case float64:
			vals = append(vals, fmt.Sprintf("%f", s.GetData(key)))
		case int:
			vals = append(vals, fmt.Sprintf("%d", s.GetData(key)))
		}
	}

	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(vals))))

	for _, val := range vals {
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
	}

	conn.Write([]byte("\n"))
	conn.Flush()
}

func handleSet(cmd []string, s Server, conn *bufio.Writer) {
	switch x := len(cmd); {
	default:
		conn.Write([]byte("-Error wrong number of args for SET command\r\n\n"))
		conn.Flush()
	case x > 3:
		s.SetData(cmd[1], strings.Join(cmd[2:], " "))
		conn.Write([]byte("+OK\r\n"))
	case x == 3:
		val, err := strconv.ParseFloat(cmd[2], 32)

		if err != nil {
			s.SetData(cmd[1], cmd[2])
		} else if !utils.IsInteger(val) {
			s.SetData(cmd[1], val)
		} else {
			s.SetData(cmd[1], int(val))
		}

		conn.Write([]byte("+OK\r\n\n"))
		conn.Flush()
	}
}

func init() {
	Plugin.name = "GetCommand"
	Plugin.commands = []string{"set", "get", "mget"}
	Plugin.description = "Handle basic SET, GET and MGET commands"
}
