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
	if err := p.acl.AuthenticateConnection(ctx, conn, cmd); err != nil {
		return nil, err
	}
	return []byte(utils.OK_RESPONSE), nil
}

func (p Plugin) handleGetUser(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	var user *User
	userFound := false
	for _, u := range p.acl.Users {
		if u.Username == cmd[2] {
			user = u
			userFound = true
			break
		}
	}

	if !userFound {
		return nil, errors.New("user not found")
	}

	// username,
	res := fmt.Sprintf("*12\r\n+username\r\n*1\r\n+%s", user.Username)

	// flags
	var flags []string
	if user.Enabled {
		flags = append(flags, "on")
	} else {
		flags = append(flags, "off")
	}
	if user.NoPassword {
		flags = append(flags, "nopass")
	}

	res = res + fmt.Sprintf("\r\n+flags\r\n*%d", len(flags))
	for _, flag := range flags {
		res = fmt.Sprintf("%s\r\n+%s", res, flag)
	}

	// categories
	res = res + fmt.Sprintf("\r\n+categories\r\n*%d", len(user.IncludedCategories)+len(user.ExcludedCategories))
	for _, category := range user.IncludedCategories {
		if category == "*" {
			res = res + fmt.Sprintf("\r\n++@all")
			continue
		}
		res = res + fmt.Sprintf("\r\n++@%s", category)
	}
	for _, category := range user.ExcludedCategories {
		if category == "*" {
			res = res + fmt.Sprintf("\r\n+-@all")
			continue
		}
		res = res + fmt.Sprintf("\r\n+-@%s", category)
	}

	// commands
	res = res + fmt.Sprintf("\r\n+commands\r\n*%d", len(user.IncludedCommands)+len(user.ExcludedCommands))
	for _, command := range user.IncludedCommands {
		if command == "*" {
			res = res + fmt.Sprintf("\r\n++all")
			continue
		}
		res = res + fmt.Sprintf("\r\n++%s", command)
	}
	for _, command := range user.ExcludedCommands {
		if command == "*" {
			res = res + fmt.Sprintf("\r\n+-all")
			continue
		}
		res = res + fmt.Sprintf("\r\n+-%s", command)
	}

	// keys
	res = res + fmt.Sprintf("\r\n+keys\r\n*%d",
		len(user.IncludedKeys)+len(user.IncludedReadKeys)+len(user.IncludedWriteKeys))
	for _, key := range user.IncludedKeys {
		res = res + fmt.Sprintf("\r\n+%s~%s", "%RW", key)
	}
	for _, key := range user.IncludedReadKeys {
		res = res + fmt.Sprintf("\r\n+%s~%s", "%R", key)
	}
	for _, key := range user.IncludedWriteKeys {
		res = res + fmt.Sprintf("\r\n+%s~%s", "%W", key)
	}

	// channels
	res = res + fmt.Sprintf("\r\n+channels\r\n*%d",
		len(user.IncludedPubSubChannels)+len(user.ExcludedPubSubChannels))
	for _, channel := range user.IncludedPubSubChannels {
		res = res + fmt.Sprintf("\r\n++&%s", channel)
	}
	for _, channel := range user.ExcludedPubSubChannels {
		res = res + fmt.Sprintf("\r\n+-&%s", channel)
	}

	res += "\r\n\n"

	return []byte(res), nil
}

func (p Plugin) handleCat(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) > 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	categories := make(map[string][]string)

	commands := server.GetAllCommands(ctx)

	for _, command := range commands {
		if len(command.SubCommands) == 0 {
			for _, category := range command.Categories {
				categories[category] = append(categories[category], command.Command)
			}
			continue
		}
		for _, subcommand := range command.SubCommands {
			for _, category := range subcommand.Categories {
				categories[category] = append(categories[category],
					fmt.Sprintf("%s|%s", command.Command, subcommand.Command))
			}
		}
	}

	if len(cmd) == 2 {
		var cats []string
		length := 0
		for key, _ := range categories {
			cats = append(cats, key)
			length += 1
		}
		res := fmt.Sprintf("*%d", length)
		for i, cat := range cats {
			res = fmt.Sprintf("%s\r\n+%s", res, cat)
			if i == len(cats)-1 {
				res = res + "\r\n\n"
			}
		}
		return []byte(res), nil
	}

	if len(cmd) == 3 {
		var res string
		for category, commands := range categories {
			if strings.EqualFold(category, cmd[2]) {
				res = fmt.Sprintf("*%d", len(commands))
				for i, command := range commands {
					res = fmt.Sprintf("%s\r\n+%s", res, command)
					if i == len(commands)-1 {
						res = res + "\r\n\n"
					}
				}
				return []byte(res), nil
			}
		}
	}

	return nil, errors.New("category not found")
}

func (p Plugin) handleUsers(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	res := fmt.Sprintf("*%d", len(p.acl.Users))
	for _, user := range p.acl.Users {
		res += fmt.Sprintf("\r\n$%d\r\n%s", len(user.Username), user.Username)
	}
	res += "\r\n\n"
	return []byte(res), nil
}

func (p Plugin) handleSetUser(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if err := p.acl.SetUser(ctx, cmd[2:]); err != nil {
		return nil, err
	}
	return []byte(utils.OK_RESPONSE), nil
}

func (p Plugin) handleDelUser(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	if err := p.acl.DeleteUser(ctx, cmd[2:]); err != nil {
		return nil, err
	}
	return []byte(utils.OK_RESPONSE), nil
}

func (p Plugin) handleWhoAmI(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	connectionInfo := p.acl.Connections[conn]
	return []byte(fmt.Sprintf("+%s\r\n\n", connectionInfo.User.Username)), nil
}

func (p Plugin) handleList(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) > 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	res := fmt.Sprintf("*%d", len(p.acl.Users))
	s := ""
	for _, user := range p.acl.Users {
		s = user.Username
		// User enabled
		if user.Enabled {
			s += " on"
		} else {
			s += " off"
		}
		// Passwords
		for _, password := range user.Passwords {
			if strings.EqualFold(password.PasswordType, "plaintext") {
				s += fmt.Sprintf(" >%s", password.PasswordValue)
			}
			if strings.EqualFold(password.PasswordType, "SHA256") {
				s += fmt.Sprintf(" #%s", password.PasswordValue)
			}
		}
		// Included categories
		for _, category := range user.IncludedCategories {
			if category == "*" {
				s += " +@all"
				continue
			}
			s += fmt.Sprintf(" +@%s", category)
		}
		// Excluded categories
		for _, category := range user.ExcludedCategories {
			if category == "*" {
				s += " -@all"
				continue
			}
			s += fmt.Sprintf(" -@%s", category)
		}
		// Included commands
		for _, command := range user.IncludedCommands {
			if command == "*" {
				s += " +all"
				continue
			}
			s += fmt.Sprintf(" +%s", command)
		}
		// Excluded commands
		for _, command := range user.ExcludedCommands {
			if command == "*" {
				s += " -all"
				continue
			}
			s += fmt.Sprintf(" -%s", command)
		}
		// Included keys
		for _, key := range user.IncludedKeys {
			s += fmt.Sprintf(" %s~%s", "%RW", key)
		}
		// Included read keys
		for _, key := range user.IncludedReadKeys {
			s += fmt.Sprintf(" %s~%s", "%R", key)
		}
		// Included write keys
		for _, key := range user.IncludedReadKeys {
			s += fmt.Sprintf(" %s~%s", "%W", key)
		}
		// Included Pub/Sub channels
		for _, channel := range user.IncludedPubSubChannels {
			s += fmt.Sprintf(" +&%s", channel)
		}
		// Excluded Pup/Sub channels
		for _, channel := range user.ExcludedPubSubChannels {
			s += fmt.Sprintf(" -&%s", channel)
		}
		res = res + fmt.Sprintf("\r\n$%d\r\n%s", len(s), s)
	}

	res = res + "\r\n\n"
	return []byte(res), nil
}

func (p Plugin) handleLoad(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ACL LOAD not implemented")
}

func (p Plugin) handleSave(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	fmt.Println(p.acl)
	return nil, errors.New("ACL SAVE not implemented")
}

func NewModule(acl *ACL) Plugin {
	ACLPlugin := Plugin{
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
						Description: "(ACL DELUSER) Deletes users and terminates their connections. Cannot delete default user",
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
