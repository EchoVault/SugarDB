package main

import (
	"bufio"
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
}

func init() {
	Plugin.name = "ListCommand"
	Plugin.commands = []string{
		"lpush",     // (LPUSH key value1 [value2]) Prepends one or more values to the beginning of a list, creates the list if it does not exist.
		"lpushx",    // (LPUSHX key value) Prepends a value to the beginning of a list only if the list exists.
		"lpop",      // (LPOP key) Removes and returns the first element of a list.
		"llen",      // (LLEN key) Return the length of a list.
		"lrange",    // (LRANGE key start end) Return a range of elements between the given indices.
		"lmove",     // (LMOVE key1 key2 LEFT/RIGHT LEFT/RIGHT) Move element from one list to the other specifying left/right for both lists.
		"lrem",      // (LREM key count value) Remove elements from list.
		"lset",      // (LSET key index value) Sets teh value of an element in a list by its index.
		"ltrim",     // (LTRIM key start end) Trims a list to the specified range.
		"lincr",     // (LINCR key index) Increment the list element at the given index by 1.
		"lincrby",   // (LINCRBY key index value) Increment the list element at the given index by the given value.
		"lindex",    // (LINDEX key index) Gets list element by index.
		"rpop",      // (RPOP key) Removes and gets the last element in a list.
		"rpoplpush", // (RPOPLPUSH key1 key2) Removes last element of one list, prepends it to another list and returns it.
		"rpush",     // (RPUSH key value [value2]) Appends one or multiple elements to the end of a list.
		"rpushx",    // (RPUSHX key value) Appends an element to the end of a list, only if the list exists.
	}
	Plugin.description = "Handle List commands"
}
