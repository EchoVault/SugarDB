package acl

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	categories  []string
	description string
	acl         *ACL
}

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() ([]byte, error) {
	return json.Marshal(p.commands)
}

func (p Plugin) Description() string {
	return p.description
}

func (p Plugin) HandleCommandWithConnection(ctx context.Context, cmd []string, server interface{}, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (p Plugin) HandleCommand(ctx context.Context, cmd []string, server interface{}) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (p Plugin) GetCommands() []utils.Command {
	return p.commands
}

var ACLPlugin Plugin

func NewACLPlugin(acl *ACL) Plugin {
	ACLPlugin = Plugin{
		acl:  acl,
		name: "ACLCommands",
		commands: []utils.Command{
			{
				Command:              "cat",
				Categories:           []string{},
				Description:          "List all the categories and commands inside a category",
				HandleWithConnection: false,
				Sync:                 false,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "auth",
				Categories:           []string{},
				Description:          "Authenticates the connection",
				HandleWithConnection: true,
				Sync:                 false,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "users",
				Categories:           []string{},
				Description:          "List all ACL users",
				HandleWithConnection: false,
				Sync:                 false,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "setuser",
				Categories:           []string{},
				Description:          "Configure a new or existing user",
				HandleWithConnection: false,
				Sync:                 true,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "getuser",
				Categories:           []string{},
				Description:          "List the ACL rules of a user",
				HandleWithConnection: true,
				Sync:                 false,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "deluser",
				Categories:           []string{},
				Description:          "Deletes users and terminates their connections",
				HandleWithConnection: false,
				Sync:                 true,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "whoami",
				Categories:           []string{},
				Description:          "Returns the authenticated user of the current connection",
				HandleWithConnection: false,
				Sync:                 true,
				Plugin:               ACLPlugin,
			},

			{
				Command:              "genpass",
				Categories:           []string{},
				Description:          "Generates a password that can be used to identify a user",
				HandleWithConnection: false,
				Sync:                 true,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "list",
				Categories:           []string{},
				Description:          "Dumps effective acl rules in acl config file format",
				HandleWithConnection: false,
				Sync:                 true,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "load",
				Categories:           []string{},
				Description:          "Reloads the rules from the configured ACL config file",
				HandleWithConnection: false,
				Sync:                 true,
				Plugin:               ACLPlugin,
			},
			{
				Command:              "save",
				Categories:           []string{},
				Description:          "Saves the effective ACL rules the configured ACL config file",
				HandleWithConnection: false,
				Sync:                 true,
				Plugin:               ACLPlugin,
			},
		},
		description: "Internal plugin to handle ACL commands",
	}
	return ACLPlugin
}
