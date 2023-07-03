package main

type Server interface {
	GetData(key string) interface{}
	SetData(key string, value interface{})
}

type plugin struct {
	name        string
	command     string
	description string
}

var Plugin plugin

func (p *plugin) Name() string {
	return p.name
}

func (p *plugin) Command() string {
	return p.command
}

func (p *plugin) Description() string {
	return p.description
}

func (p *plugin) HandleCommand(tokens []string, server interface{}) error {
	return nil
}

func init() {
	Plugin.name = "SetCommand"
	Plugin.command = "set"
	Plugin.description = "Set the value of the specified key"
}
