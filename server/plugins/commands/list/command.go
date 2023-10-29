package main

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/kelvinmwinuka/memstore/server/utils"
)

const (
	OK = "+OK\r\n\n"
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

func (p *plugin) HandleCommand(cmd []string, server interface{}) ([]byte, error) {
	c := strings.ToLower(cmd[0])

	switch {
	default:
		return nil, errors.New("command unknown")
	case c == "llen":
		return handleLLen(cmd, server.(Server))

	case c == "lindex":
		return handleLIndex(cmd, server.(Server))

	case c == "lrange":
		return handleLRange(cmd, server.(Server))

	case c == "lset":
		return handleLSet(cmd, server.(Server))

	case c == "ltrim":
		return handleLTrim(cmd, server.(Server))

	case c == "lrem":
		return handleLRem(cmd, server.(Server))

	case c == "lmove":
		return handleLMove(cmd, server.(Server))

	case utils.Contains[string]([]string{"lpush", "lpushx"}, c):
		return handleLPush(cmd, server.(Server))

	case utils.Contains[string]([]string{"rpush", "rpushx"}, c):
		return handleRPush(cmd, server.(Server))

	case utils.Contains[string]([]string{"lpop", "rpop"}, c):
		return handlePop(cmd, server.(Server))
	}
}

func handleLLen(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New("wrong number of args for LLEN command")
	}

	list, ok := server.GetData(cmd[1]).([]interface{})

	if !ok {

		return nil, errors.New("LLEN command on non-list item")
	}

	return []byte(fmt.Sprintf(":%d\r\n\n", len(list))), nil
}

func handleLIndex(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 3 {
		return nil, errors.New("wrong number of args for LINDEX command")
	}

	index, ok := utils.AdaptType(cmd[2]).(int)

	if !ok {
		return nil, errors.New("index must be an integer")
	}

	list, ok := server.GetData(cmd[1]).([]interface{})

	if !ok {

		return nil, errors.New("LINDEX command on non-list item")
	}

	if !(index >= 0 && index < len(list)) {

		return nil, errors.New("index must be within list range")
	}

	return []byte(fmt.Sprintf("+%s\r\n\n", list[index])), nil
}

func handleLRange(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 4 {
		return nil, errors.New("wrong number of arguments for LRANGE command")
	}

	start, startOk := utils.AdaptType(cmd[2]).(int)
	end, endOk := utils.AdaptType(cmd[3]).(int)

	if !startOk || !endOk {
		return nil, errors.New("both start and end indices must be integers")
	}

	list, ok := server.GetData(cmd[1]).([]interface{})

	if !ok {
		return nil, errors.New("type cannot be returned with LRANGE command")
	}

	// Make sure start is within range
	if !(start >= 0 && start < len(list)) {
		return nil, errors.New("start index not within list range")
	}

	// Make sure end is within range, or is -1 otherwise
	if !((end >= 0 && end < len(list)) || end == -1) {
		return nil, errors.New("end index must be within list range or -1")
	}

	var bytes []byte

	// If end is -1, read list from start to the end of the list
	if end == -1 {
		bytes = []byte("*" + fmt.Sprint(len(list)-start) + "\r\n")
		for i := start; i < len(list); i++ {
			str := fmt.Sprintf("%v", list[i])
			bytes = append(bytes, []byte("$"+fmt.Sprint(len(str))+"\r\n"+str+"\r\n")...)
		}
		bytes = append(bytes, []byte("\n")...)
		return bytes, nil
	}

	// Make sure start and end are not equal to each other
	if start == end {
		return nil, errors.New("start and end indices cannot be equal")
	}

	// If end is not -1:
	//	1) If end is larger than start, return slice from start -> end
	//	2) If end is smaller than start, return slice from end -> start
	bytes = []byte("*" + fmt.Sprint(int(math.Abs(float64(start-end)))+1) + "\r\n")

	i := start
	j := end + 1
	if start > end {
		j = end - 1
	}

	for i != j {
		str := fmt.Sprintf("%v", list[i])
		bytes = append(bytes, []byte("$"+fmt.Sprint(len(str))+"\r\n"+str+"\r\n")...)
		if start < end {
			i++
		} else {
			i--
		}

	}

	bytes = append(bytes, []byte("\n")...)

	return bytes, nil
}

func handleLSet(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 4 {
		return nil, errors.New("wrong number of arguments for LSET command")
	}

	list, ok := server.GetData(cmd[1]).([]interface{})

	if !ok {

		return nil, errors.New("LSET command on non-list item")
	}

	index, ok := utils.AdaptType(cmd[2]).(int)

	if !ok {

		return nil, errors.New("index must be an integer")
	}

	if !(index >= 0 && index < len(list)) {

		return nil, errors.New("index must be within range")
	}

	list[index] = utils.AdaptType(cmd[3])
	server.SetData(cmd[1], list)

	return []byte(OK), nil
}

func handleLTrim(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 4 {
		return nil, errors.New("wrong number of args for command LTRIM")
	}

	start, startOk := utils.AdaptType(cmd[2]).(int)
	end, endOk := utils.AdaptType(cmd[3]).(int)

	if !startOk || !endOk {
		return nil, errors.New("start and end indices must be integers")
	}

	if end < start && end != -1 {
		return nil, errors.New("end index must be greater than start index or -1")
	}

	list, ok := server.GetData(cmd[1]).([]interface{})

	if !ok {

		return nil, errors.New("LTRIM command on non-list item")
	}

	if !(start >= 0 && start < len(list)) {

		return nil, errors.New("start index must be within list boundary")
	}

	if end == -1 || end > len(list) {
		server.SetData(cmd[1], list[start:])

		return []byte(OK), nil
	}

	server.SetData(cmd[1], list[start:end])

	return []byte(OK), nil
}

func handleLRem(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 4 {
		return nil, errors.New("wrong number of arguments for LREM command")
	}

	value := cmd[3]
	count, ok := utils.AdaptType(cmd[2]).(int)

	if !ok {
		return nil, errors.New("count must be an integer")
	}

	absoluteCount := math.Abs(float64(count))

	list, ok := server.GetData(cmd[1]).([]interface{})

	if !ok {

		return nil, errors.New("LREM command on non-list item")
	}

	switch {
	default:
		// Count is zero, keep list the same
	case count > 0:
		// Start from the head
		for i := 0; i < len(list); i++ {
			if absoluteCount == 0 {
				break
			}
			if fmt.Sprintf("%v", list[i]) == value {
				list[i] = nil
				absoluteCount -= 1
			}
		}
	case count < 0:
		// Start from the tail
		for i := len(list) - 1; i >= 0; i-- {
			if absoluteCount == 0 {
				break
			}
			if fmt.Sprintf("%v", list[i]) == value {
				list[i] = nil
				absoluteCount -= 1
			}
		}
	}

	list = utils.Filter[interface{}](list, func(elem interface{}) bool {
		return elem != nil
	})

	server.SetData(cmd[1], list)

	return []byte(OK), nil
}

func handleLMove(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 5 {
		return nil, errors.New("wrong number of arguments for LMOVE command")
	}

	whereFrom := strings.ToLower(cmd[3])
	whereTo := strings.ToLower(cmd[4])

	if !utils.Contains[string]([]string{"left", "right"}, whereFrom) || !utils.Contains[string]([]string{"left", "right"}, whereTo) {
		return nil, errors.New("wherefrom and whereto arguments must be either LEFT or RIGHT")
	}

	source, sourceOk := server.GetData(cmd[1]).([]interface{})
	destination, destinationOk := server.GetData(cmd[2]).([]interface{})

	if !sourceOk || !destinationOk {

		return nil, errors.New("source and destination must both be lists")
	}

	switch whereFrom {
	case "left":
		server.SetData(cmd[1], append([]interface{}{}, source[1:]...))
		if whereTo == "left" {
			server.SetData(cmd[2], append(source[0:1], destination...))
		} else if whereTo == "right" {
			server.SetData(cmd[2], append(destination, source[0]))
		}
	case "right":
		server.SetData(cmd[1], append([]interface{}{}, source[:len(source)-1]...))
		if whereTo == "left" {
			server.SetData(cmd[2], append(source[len(source)-1:], destination...))
		} else if whereTo == "right" {
			server.SetData(cmd[2], append(destination, source[len(source)-1]))
		}
	}

	return []byte(OK), nil
}

func handleLPush(cmd []string, server Server) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, fmt.Errorf("wrong number of arguments for %s command", strings.ToUpper(cmd[0]))
	}

	newElems := []interface{}{}

	for _, elem := range cmd[2:] {
		newElems = append(newElems, utils.AdaptType(elem))
	}

	currentList := server.GetData(cmd[1])

	if currentList == nil {
		switch strings.ToLower(cmd[0]) {
		default:
			server.SetData(cmd[1], newElems)

			return []byte(OK), nil
		case "lpushx":

			return nil, errors.New("no list at key")
		}
	}

	l, ok := currentList.([]interface{})

	if !ok {

		return nil, errors.New("LPUSH command on non-list item")
	}

	server.SetData(cmd[1], append(newElems, l...))

	return []byte(OK), nil
}

func handleRPush(cmd []string, server Server) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, fmt.Errorf("wrong number of arguments for %s command", strings.ToUpper(cmd[0]))
	}

	newElems := []interface{}{}

	for _, elem := range cmd[2:] {
		newElems = append(newElems, utils.AdaptType(elem))
	}

	currentList := server.GetData(cmd[1])

	if currentList == nil {
		switch strings.ToLower(cmd[0]) {
		default:
			server.SetData(cmd[1], newElems)

			return []byte(OK), nil
		case "rpushx":

			return nil, errors.New("no list at key")
		}
	}

	l, ok := currentList.([]interface{})

	if !ok {

		return nil, errors.New("RPUSH command on non-list item")
	}

	server.SetData(cmd[1], append(l, newElems...))

	return []byte(OK), nil
}

func handlePop(cmd []string, server Server) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, fmt.Errorf("wrong number of args for %s command", strings.ToUpper(cmd[0]))
	}

	list, ok := server.GetData(cmd[1]).([]interface{})

	if !ok {
		return nil, fmt.Errorf("%s command on non-list item", strings.ToUpper(cmd[0]))
	}

	switch strings.ToLower(cmd[0]) {
	default:
		server.SetData(cmd[1], list[1:])

		return []byte(fmt.Sprintf("+%v\r\n\n", list[0])), nil
	case "rpop":
		server.SetData(cmd[1], list[:len(list)-1])

		return []byte(fmt.Sprintf("+%v\r\n\n", list[len(list)-1])), nil
	}

}

func init() {
	Plugin.name = "ListCommand"
	Plugin.commands = []string{
		"lpush",  // (LPUSH key value1 [value2]) Prepends one or more values to the beginning of a list, creates the list if it does not exist.
		"lpushx", // (LPUSHX key value) Prepends a value to the beginning of a list only if the list exists.
		"lpop",   // (LPOP key) Removes and returns the first element of a list.
		"llen",   // (LLEN key) Return the length of a list.
		"lrange", // (LRANGE key start end) Return a range of elements between the given indices.
		"lindex", // (LINDEX key index) Gets list element by index.
		"lset",   // (LSET key index value) Sets the value of an element in a list by its index.
		"ltrim",  // (LTRIM key start end) Trims a list to the specified range.
		"lrem",   // (LREM key count value) Remove elements from list.
		"lmove",  // (LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> Move element from one list to the other specifying left/right for both lists.
		"rpop",   // (RPOP key) Removes and gets the last element in a list.
		"rpush",  // (RPUSH key value [value2]) Appends one or multiple elements to the end of a list.
		"rpushx", // (RPUSHX key value) Appends an element to the end of a list, only if the list exists.
	}
	Plugin.description = "Handle List commands"
}
