package acl

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"strings"
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

func (p Plugin) HandleCommandWithConnection(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if strings.EqualFold(cmd[0], "auth") {
		return p.handleAuth(ctx, cmd, server, conn)
	}
	if strings.EqualFold(cmd[0], "acl") {
		switch strings.ToLower(cmd[1]) {
		default:
			return nil, errors.New("not implemented")
		case "getuser":
			return p.handleGetUser(ctx, cmd, server, conn)
		}
	}
	return nil, errors.New("not implemented")
}

func (p Plugin) HandleCommand(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if strings.EqualFold(cmd[0], "acl") {
		switch strings.ToLower(cmd[1]) {
		default:
			return nil, errors.New("not implemented")
		case "cat":
			return p.handleCat(ctx, cmd, server)
		case "users":
			return p.handleUsers(ctx, cmd, server)
		case "setuser":
			return p.handleSetUser(ctx, cmd, server)
		case "deluser":
			return p.handleDelUser(ctx, cmd, server)
		case "whoami":
			return p.handleWhoAmI(ctx, cmd, server)
		case "genpass":
			return p.handleGenPass(ctx, cmd, server)
		case "list":
			return p.handleList(ctx, cmd, server)
		case "load":
			return p.handleLoad(ctx, cmd, server)
		case "save":
			return p.handleSave(ctx, cmd, server)
		}
	}
	return nil, errors.New("not implemented")
}

func (p Plugin) GetCommands() []utils.Command {
	return p.commands
}

func (p Plugin) handleAuth(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("AUTH not implemented")
}

func (p Plugin) handleGetUser(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("ACL GET USER not implemented")
}

func (p Plugin) handleCat(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL CAT not implemented")
}

func (p Plugin) handleUsers(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL USERS not implemented")
}

func (p Plugin) handleSetUser(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL SETUSER not implemented")
}

func (p Plugin) handleDelUser(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL DELUSER not implemented")
}

func (p Plugin) handleWhoAmI(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL WHOAMI not implemented")
}

func (p Plugin) handleGenPass(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL GENPASS not implemented")
}

func (p Plugin) handleList(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL LIST not implemented")
}

func (p Plugin) handleLoad(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL LOAD not implemented")
}

func (p Plugin) handleSave(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL SAVE not implemented")
}

var ACLPlugin Plugin

func NewACLPlugin(acl *ACL) Plugin {
	ACLPlugin = Plugin{
		acl:  acl,
		name: "ACLCommands",
		commands: []utils.Command{
			{
				Command:              "acl",
				Categories:           []string{},
				Description:          "List all the categories and commands inside a category",
				HandleWithConnection: false,
				Sync:                 false,
				Plugin:               ACLPlugin,
			},
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
