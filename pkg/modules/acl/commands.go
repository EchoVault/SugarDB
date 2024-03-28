// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	internal_acl "github.com/echovault/echovault/internal/acl"
	"github.com/echovault/echovault/pkg/utils"
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"os"
	"path"
	"slices"
	"strings"
)

func handleAuth(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}
	if err := acl.AuthenticateConnection(ctx, conn, cmd); err != nil {
		return nil, err
	}
	return []byte(utils.OkResponse), nil
}

func handleGetUser(_ context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}

	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}

	var user *internal_acl.User
	userFound := false
	for _, u := range acl.Users {
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
	if user.NoKeys {
		flags = append(flags, "nokeys")
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
	allKeys := user.IncludedReadKeys
	for _, key := range append(user.IncludedWriteKeys, user.IncludedReadKeys...) {
		if !slices.Contains(allKeys, key) {
			allKeys = append(allKeys, key)
		}
	}
	res = res + fmt.Sprintf("\r\n+keys\r\n*%d", len(allKeys))
	for _, key := range allKeys {
		switch {
		case slices.Contains(user.IncludedWriteKeys, key) && slices.Contains(user.IncludedReadKeys, key):
			// Key is RW
			res = res + fmt.Sprintf("\r\n+%s~%s", "%RW", key)
		case slices.Contains(user.IncludedWriteKeys, key):
			// Keys is W-Only
			res = res + fmt.Sprintf("\r\n+%s~%s", "%W", key)
		case slices.Contains(user.IncludedReadKeys, key):
			// Key is R-Only
			res = res + fmt.Sprintf("\r\n+%s~%s", "%R", key)
		}
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

	res += "\r\n"

	return []byte(res), nil
}

func handleCat(_ context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	if len(cmd) > 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}

	categories := make(map[string][]string)

	commands := server.GetAllCommands()

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
				res = res + "\r\n"
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
						res = res + "\r\n"
					}
				}
				return []byte(res), nil
			}
		}
	}

	return nil, fmt.Errorf("category %s not found", strings.ToUpper(cmd[2]))
}

func handleUsers(_ context.Context, _ []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}
	res := fmt.Sprintf("*%d", len(acl.Users))
	for _, user := range acl.Users {
		res += fmt.Sprintf("\r\n$%d\r\n%s", len(user.Username), user.Username)
	}
	res += "\r\n"
	return []byte(res), nil
}

func handleSetUser(_ context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}
	if err := acl.SetUser(cmd[2:]); err != nil {
		return nil, err
	}
	return []byte(utils.OkResponse), nil
}

func handleDelUser(ctx context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}
	if err := acl.DeleteUser(ctx, cmd[2:]); err != nil {
		return nil, err
	}
	return []byte(utils.OkResponse), nil
}

func handleWhoAmI(_ context.Context, _ []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}
	connectionInfo := acl.Connections[conn]
	return []byte(fmt.Sprintf("+%s\r\n", connectionInfo.User.Username)), nil
}

func handleList(_ context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	if len(cmd) > 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}
	res := fmt.Sprintf("*%d", len(acl.Users))
	s := ""
	for _, user := range acl.Users {
		s = user.Username
		// User enabled
		if user.Enabled {
			s += " on"
		} else {
			s += " off"
		}
		// NoPassword
		if user.NoPassword {
			s += " nopass"
		}
		// No keys
		if user.NoKeys {
			s += " nokeys"
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
		// Included read keys
		for _, key := range user.IncludedReadKeys {
			if slices.Contains(user.IncludedWriteKeys, key) {
				s += fmt.Sprintf(" %s~%s", "%RW", key)
				continue
			}
			s += fmt.Sprintf(" %s~%s", "%R", key)
		}
		// Included write keys
		for _, key := range user.IncludedReadKeys {
			if !slices.Contains(user.IncludedReadKeys, key) {
				s += fmt.Sprintf(" %s~%s", "%W", key)
			}
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

	res = res + "\r\n"
	return []byte(res), nil
}

func handleLoad(_ context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}

	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}

	acl.LockUsers()
	defer acl.RUnlockUsers()

	f, err := os.Open(acl.Config.AclConfig)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	ext := path.Ext(f.Name())

	var users []*internal_acl.User

	if ext == ".json" {
		if err := json.NewDecoder(f).Decode(&users); err != nil {
			return nil, err
		}
	}

	if ext == ".yaml" || ext == ".yml" {
		if err := yaml.NewDecoder(f).Decode(&users); err != nil {
			return nil, err
		}
	}

	// Normalise each user
	for _, user := range users {
		user.Normalise()
		// Traverse the list of users.
		userFound := false
		for _, u := range acl.Users {
			if u.Username == user.Username {
				userFound = true
				// If we have a user with the current username and are in merge mode, merge the two users.
				if strings.EqualFold(cmd[2], "merge") {
					u.Merge(user)
				} else {
					// If we have a user with the current username and are in replace mode, merge the two users.
					u.Replace(user)
				}
				break
			}
		}
		// If the no user with current loaded username is already in acl list, then append the user to the list
		if !userFound {
			acl.Users = append(acl.Users, user)
		}
	}

	return []byte(utils.OkResponse), nil
}

func handleSave(_ context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	if len(cmd) > 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}

	acl, ok := server.GetACL().(*internal_acl.ACL)
	if !ok {
		return nil, errors.New("could not load ACL")
	}

	acl.RLockUsers()
	acl.RUnlockUsers()

	f, err := os.OpenFile(acl.Config.AclConfig, os.O_WRONLY|os.O_CREATE, os.ModeAppend)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	ext := path.Ext(f.Name())

	if ext == ".json" {
		// Write to JSON config file
		out, err := json.Marshal(acl.Users)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(out)
		if err != nil {
			return nil, err
		}
	}

	if ext == ".yaml" || ext == ".yml" {
		// Write to yaml file
		out, err := yaml.Marshal(acl.Users)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(out)
		if err != nil {
			return nil, err
		}
	}

	err = f.Sync()
	if err != nil {
		return nil, err
	}

	return []byte(utils.OkResponse), nil
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:     "auth",
			Module:      utils.ACLModule,
			Categories:  []string{utils.ConnectionCategory, utils.SlowCategory},
			Description: "(AUTH [username] password) Authenticates the connection",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: handleAuth,
		},
		{
			Command:     "acl",
			Module:      utils.ACLModule,
			Categories:  []string{},
			Description: "Access-Control-List commands",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			SubCommands: []utils.SubCommand{
				{
					Command:    "cat",
					Module:     utils.ACLModule,
					Categories: []string{utils.SlowCategory},
					Description: `(ACL CAT [category]) List all the categories. 
If the optional category is provided, list all the commands in the category`,
					Sync: false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleCat,
				},
				{
					Command:     "users",
					Module:      utils.ACLModule,
					Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
					Description: "(ACL USERS) List all usernames of the configured ACL users",
					Sync:        false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleUsers,
				},
				{
					Command:     "setuser",
					Module:      utils.ACLModule,
					Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
					Description: "(ACL SETUSER) Configure a new or existing user",
					Sync:        true,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleSetUser,
				},
				{
					Command:     "getuser",
					Module:      utils.ACLModule,
					Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
					Description: "(ACL GETUSER) List the ACL rules of a user",
					Sync:        false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleGetUser,
				},
				{
					Command:     "deluser",
					Module:      utils.ACLModule,
					Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
					Description: "(ACL DELUSER) Deletes users and terminates their connections. Cannot delete default user",
					Sync:        true,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleDelUser,
				},
				{
					Command:     "whoami",
					Module:      utils.ACLModule,
					Categories:  []string{utils.FastCategory},
					Description: "(ACL WHOAMI) Returns the authenticated user of the current connection",
					Sync:        true,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleWhoAmI,
				},
				{
					Command:     "list",
					Module:      utils.ACLModule,
					Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
					Description: "(ACL LIST) Dumps effective acl rules in acl config file format",
					Sync:        true,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleList,
				},
				{
					Command:    "load",
					Module:     utils.ACLModule,
					Categories: []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
					Description: `
(ACL LOAD <MERGE | REPLACE>) Reloads the rules from the configured ACL config file.
When 'MERGE' is passed, users from config file who share a username with users in memory will be merged.
When 'REPLACE' is passed, users from config file who share a username with users in memory will replace the user in memory.`,
					Sync: true,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleLoad,
				},
				{
					Command:     "save",
					Module:      utils.ACLModule,
					Categories:  []string{utils.AdminCategory, utils.SlowCategory, utils.DangerousCategory},
					Description: "(ACL SAVE) Saves the effective ACL rules the configured ACL config file",
					Sync:        true,
					KeyExtractionFunc: func(cmd []string) ([]string, error) {
						return []string{}, nil
					},
					HandlerFunc: handleSave,
				},
			},
		},
	}
}
