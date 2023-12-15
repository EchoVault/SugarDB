package acl

import (
	"context"
	"errors"
	"fmt"
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

var ACLPlugin Plugin

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() []utils.Command {
	return p.commands
}

func (p Plugin) Description() string {
	return p.description
}

func (p Plugin) HandleCommand(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if strings.EqualFold(cmd[0], "auth") {
		return p.handleAuth(ctx, cmd, server, conn)
	}
	if strings.EqualFold(cmd[0], "acl") {
		switch strings.ToLower(cmd[1]) {
		default:
			return nil, errors.New("not implemented")
		case "getuser":
			return p.handleGetUser(ctx, cmd, server, conn)
		case "cat":
			return p.handleCat(ctx, cmd, server)
		case "users":
			return p.handleUsers(ctx, cmd, server)
		case "setuser":
			return p.handleSetUser(ctx, cmd, server)
		case "deluser":
			return p.handleDelUser(ctx, cmd, server)
		case "whoami":
			return p.handleWhoAmI(ctx, cmd, server, conn)
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

func (p Plugin) handleAuth(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	if err := p.acl.AuthenticateConnection(conn, cmd); err != nil {
		return nil, err
	}
	return []byte(utils.OK_RESPONSE), nil
}

func (p Plugin) handleGetUser(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("ACL GET USER not implemented")
}

func (p Plugin) handleCat(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL CAT not implemented")
}

func (p Plugin) handleUsers(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	res := fmt.Sprintf("*%d\r\n", len(p.acl.Users))
	for _, user := range p.acl.Users {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(user.Username), user.Username)
	}
	res += "\n"
	return []byte(res), nil
}

func (p Plugin) handleSetUser(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL SETUSER not implemented")
}

func (p Plugin) handleDelUser(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL DELUSER not implemented")
}

func (p Plugin) handleWhoAmI(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	connectionInfo := p.acl.Connections[conn]
	return []byte(fmt.Sprintf("+%s\r\n\n", connectionInfo.User.Username)), nil
}

func (p Plugin) handleList(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL LIST not implemented")
}

func (p Plugin) handleLoad(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL LOAD not implemented")
}

func (p Plugin) handleSave(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	fmt.Println(p.acl)
	return nil, errors.New("ACL SAVE not implemented")
}

func NewModule(acl *ACL) Plugin {
	ACLPlugin = Plugin{
		acl:  acl,
		name: "ACLCommands",
		commands: []utils.Command{
			{
				Command:     "auth",
				Categories:  []string{utils.ConnectionCategory, utils.SlowCategory},
				Description: "(AUTH [username] password) Authenticates the connection",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "acl",
				Categories:  []string{},
				Description: "Access-Control-List commands",
				Sync:        false,
				SubCommands: []utils.SubCommand{
					{
						Command:     "cat",
						Categories:  []string{utils.SlowCategory},
						Description: "(ACL CAT [category]) List all the categories and commands inside a category.",
						Sync:        false,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "users",
						Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
						Description: "(ACL USERS) List all usersnames of the configured ACL users",
						Sync:        false,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "setuser",
						Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
						Description: "(ACL SETUSER) Configure a new or existing user",
						Sync:        true,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "getuser",
						Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
						Description: "(ACL GETUSER) List the ACL rules of a user",
						Sync:        false,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "deluser",
						Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
						Description: "(ACL DELUSER) Deletes users and terminates their connections",
						Sync:        true,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "whoami",
						Categories:  []string{utils.FastCategory},
						Description: "(ACL WHOAMI) Returns the authenticated user of the current connection",
						Sync:        true,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "list",
						Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
						Description: "(ACL LIST) Dumps effective acl rules in acl config file format",
						Sync:        true,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "load",
						Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
						Description: "(ACL LOAD) Reloads the rules from the configured ACL config file",
						Sync:        true,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
					{
						Command:     "save",
						Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
						Description: "(ACL SAVE) Saves the effective ACL rules the configured ACL config file",
						Sync:        true,
						KeyExtractionFunc: func(cmd []string) ([]string, error) {
							return []string{}, nil
						},
					},
				},
			},
		},
		description: "Internal plugin to handle ACL commands",
	}
	return ACLPlugin
}
