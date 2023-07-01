package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"github.com/kelvinmwinuka/memstore/serialization"
	"github.com/kelvinmwinuka/memstore/utils"
)

func processPing(cmd []string, connRW *bufio.ReadWriter) {
	if len(cmd) == 1 {
		serialization.Encode(connRW, "SimpleString PONG")
		connRW.Write([]byte("\n"))
		connRW.Flush()
	}
	if len(cmd) == 2 {
		serialization.Encode(connRW, fmt.Sprintf("SimpleString \"%s\"", cmd[1]))
		connRW.Write([]byte("\n"))
		connRW.Flush()
	}
}

func processSet(cmd []string, connRW *bufio.ReadWriter, server *Server) {
	fmt.Println("Process set command")
	server.data.mu.Lock()
	defer server.data.mu.Unlock()

	switch x := len(cmd); {
	default:
		fmt.Println("Wrong number of args for SET commands")
	case x > 3:
		server.data.data[cmd[1]] = strings.Join(cmd[2:], " ")
		serialization.Encode(connRW, "SimpleString OK")
	case x == 3:
		val, err := strconv.ParseFloat(cmd[2], 32)

		if err != nil {
			server.data.data[cmd[1]] = cmd[2]
		} else if !utils.IsInteger(val) {
			server.data.data[cmd[1]] = val
		} else {
			server.data.data[cmd[1]] = int(val)
		}

		serialization.Encode(connRW, "SimpleString OK")
	}

	connRW.Write([]byte("\n"))
	connRW.Flush()
}

func processGet(cmd []string, connRW *bufio.ReadWriter, server *Server) {
	server.data.mu.Lock()
	defer server.data.mu.Unlock()

	// Use reflection to determine the type of the value and how to encode it
	switch server.data.data[cmd[1]].(type) {
	default:
		fmt.Println("Error. The requested object's type cannot be returned with the GET command")
	case nil:
		serialization.Encode(connRW, "SimpleString nil")
	case string:
		serialization.Encode(connRW, fmt.Sprintf("SimpleString \"%s\"", server.data.data[cmd[1]]))
	case float64:
		serialization.Encode(connRW, fmt.Sprintf("SimpleString %f", server.data.data[cmd[1]]))
	case int:
		serialization.Encode(connRW, fmt.Sprintf("Integer %d", server.data.data[cmd[1]]))
	}

	connRW.Write([]byte("\n"))
	connRW.Flush()
}

func processMGet(cmd []string, connRW *bufio.ReadWriter, server *Server) {
	server.data.mu.Lock()
	defer server.data.mu.Unlock()

	vals := []string{}

	for _, key := range cmd[1:] {
		switch server.data.data[key].(type) {
		case nil:
			vals = append(vals, "nil")
		case string:
			vals = append(vals, fmt.Sprintf("%s", server.data.data[key]))
		case float64:
			vals = append(vals, fmt.Sprintf("%f", server.data.data[key]))
		case int:
			vals = append(vals, fmt.Sprintf("%d", server.data.data[key]))
		}
	}

	serialization.Encode(connRW, fmt.Sprintf("Array %s", strings.Join(vals, " ")))

	connRW.Write([]byte("\n"))
	connRW.Flush()
}

func processCommand(cmd []string, connRW *bufio.ReadWriter, server *Server) {
	// Return encoded message to client
	switch strings.ToLower(cmd[0]) {
	default:
		fmt.Println("The command is unknown")
	case "ping":
		processPing(cmd, connRW)
	case "set":
		processSet(cmd, connRW, server)
	case "get":
		processGet(cmd, connRW, server)
	case "mget":
		processMGet(cmd, connRW, server)
	}
}
