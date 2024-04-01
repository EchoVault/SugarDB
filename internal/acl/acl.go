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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/types"
	"github.com/gobwas/glob"
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"os"
	"path"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

type Connection struct {
	Authenticated bool  // Whether the connection has been authenticated
	User          *User // The user the connection is associated with
}

type ACL struct {
	Users        []*User                  // List of ACL user profiles
	UsersMutex   sync.RWMutex             // RWMutex for concurrency control when accessing ACL profile list
	Connections  map[*net.Conn]Connection // Connections to the echovault that are currently registered with the ACL module
	Config       config.Config            // EchoVault configuration that contains the relevant ACL config options
	GlobPatterns map[string]glob.Glob
}

func NewACL(config config.Config) *ACL {
	var users []*User

	// 1. Initialise default ACL user
	defaultUser := CreateUser("default")
	if config.RequirePass {
		defaultUser.NoPassword = false
		defaultUser.Passwords = []Password{
			{
				PasswordType:  GetPasswordType(config.Password),
				PasswordValue: config.Password,
			},
		}
	}

	// 2. Read and parse the ACL config file
	if config.AclConfig != "" {
		// Override acl configurations from file
		if f, err := os.Open(config.AclConfig); err != nil {
			panic(err)
		} else {
			defer func() {
				if err := f.Close(); err != nil {
					fmt.Println("acl config file close error: ", err)
				}
			}()

			ext := path.Ext(f.Name())

			if ext == ".json" {
				if err := json.NewDecoder(f).Decode(&users); err != nil {
					log.Fatal("could not load JSON ACL config: ", err)
				}
			}

			if ext == ".yaml" || ext == ".yml" {
				if err := yaml.NewDecoder(f).Decode(&users); err != nil {
					log.Fatal("could not load YAML ACL config: ", err)
				}
			}
		}
	}

	// 3. If default user was not loaded from file, add the created one
	defaultLoaded := false
	for _, user := range users {
		if user.Username == "default" {
			defaultLoaded = true
			break
		}
	}
	if !defaultLoaded {
		users = append([]*User{defaultUser}, users...)
	}

	// 4. Normalise all users
	for _, user := range users {
		user.Normalise()
	}

	acl := ACL{
		Users:        users,
		UsersMutex:   sync.RWMutex{},
		Connections:  make(map[*net.Conn]Connection),
		Config:       config,
		GlobPatterns: make(map[string]glob.Glob),
	}

	acl.CompileGlobs()

	return &acl
}

func (acl *ACL) RegisterConnection(conn *net.Conn) {
	acl.LockUsers()
	defer acl.UnlockUsers()

	// This is called only when a connection is established.
	defaultUserIdx := slices.IndexFunc(acl.Users, func(user *User) bool {
		return user.Username == "default"
	})
	defaultUser := acl.Users[defaultUserIdx]
	acl.Connections[conn] = Connection{
		Authenticated: defaultUser.NoPassword,
		User:          defaultUser,
	}
}

func (acl *ACL) SetUser(cmd []string) error {
	acl.LockUsers()
	defer acl.UnlockUsers()

	// Check if user with the given username already exists
	// If it does, replace user variable with this user
	for _, user := range acl.Users {
		if user.Username == cmd[0] {
			if err := user.UpdateUser(cmd); err != nil {
				return err
			} else {
				acl.CompileGlobs()
				return nil
			}
		}
	}

	user := CreateUser(cmd[0])
	if err := user.UpdateUser(cmd); err != nil {
		return err
	}

	user.Normalise()

	// Add user to ACL
	acl.Users = append(acl.Users, user)

	acl.CompileGlobs()

	return nil
}

func (acl *ACL) AddUsers(users []*User) {
	acl.LockUsers()
	defer acl.UnlockUsers()

	acl.Users = append(acl.Users, users...)
}

func (acl *ACL) DeleteUser(_ context.Context, usernames []string) error {
	acl.LockUsers()
	defer acl.UnlockUsers()

	var user *User
	for _, username := range usernames {
		if username == "default" {
			// Skip default user
			continue
		}
		// Extract the user
		for _, u := range acl.Users {
			if username == u.Username {
				user = u
			}
		}
		// Skip if the current username was not found in the ACL
		if username != user.Username {
			continue
		}
		// Terminate every connection attached to this user
		for connRef, connection := range acl.Connections {
			if connection.User.Username == user.Username {
				_ = (*connRef).SetReadDeadline(time.Now().Add(-1 * time.Second))
			}
		}
		// Delete the user from the ACL
		acl.Users = slices.DeleteFunc(acl.Users, func(u *User) bool {
			return u.Username == user.Username
		})
	}
	return nil
}

func (acl *ACL) AuthenticateConnection(_ context.Context, conn *net.Conn, cmd []string) error {
	acl.RLockUsers()
	defer acl.RUnlockUsers()

	var passwords []Password
	var user *User

	h := sha256.New()

	if len(cmd) == 2 {
		// Process AUTH <password>
		h.Write([]byte(cmd[1]))
		passwords = []Password{
			{PasswordType: "plaintext", PasswordValue: cmd[1]},
			{PasswordType: "SHA256", PasswordValue: string(h.Sum(nil))},
		}
		// Authenticate with default user
		idx := slices.IndexFunc(acl.Users, func(user *User) bool {
			return user.Username == "default"
		})
		user = acl.Users[idx]
	}

	if len(cmd) == 3 {
		// Process AUTH <username> <password>
		h.Write([]byte(cmd[2]))
		passwords = []Password{
			{PasswordType: "plaintext", PasswordValue: cmd[2]},
			{PasswordType: "SHA256", PasswordValue: string(h.Sum(nil))},
		}
		// Find user with the specified username
		userFound := false
		for _, u := range acl.Users {
			if u.Username == cmd[1] {
				user = u
				userFound = true
				break
			}
		}
		if !userFound {
			return fmt.Errorf("no user with username %s", cmd[1])
		}
	}

	// If user is not enabled, return error
	if !user.Enabled {
		return fmt.Errorf("user %s is disabled", user.Username)
	}

	// If user is set to NoPassword, then immediately authenticate connection without considering the password
	if user.NoPassword {
		acl.Connections[conn] = Connection{
			Authenticated: true,
			User:          user,
		}
		return nil
	}

	for _, userPassword := range user.Passwords {
		for _, password := range passwords {
			if strings.EqualFold(userPassword.PasswordType, password.PasswordType) &&
				userPassword.PasswordValue == password.PasswordValue &&
				user.Enabled {
				// Set the current connection to the selected user and set them as authenticated
				acl.Connections[conn] = Connection{
					Authenticated: true,
					User:          user,
				}
				return nil
			}
		}
	}

	return errors.New("could not authenticate user")
}

func (acl *ACL) AuthorizeConnection(conn *net.Conn, cmd []string, command types.Command, subCommand types.SubCommand) error {
	acl.RLockUsers()
	defer acl.RUnlockUsers()

	// Extract command, categories, and keys
	comm := command.Command
	categories := command.Categories

	keys, err := command.KeyExtractionFunc(cmd)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(subCommand, types.SubCommand{}) {
		comm = fmt.Sprintf("%s|%s", comm, subCommand.Command)
		categories = append(categories, subCommand.Categories...)
		keys, err = subCommand.KeyExtractionFunc(cmd)
		if err != nil {
			return err
		}
	}

	// Skip ack
	if strings.EqualFold(comm, "ack") {
		return nil
	}

	// Skip connection
	if strings.EqualFold(comm, "connection") {
		return nil
	}

	// If the command is 'auth', then return early and allow it
	if strings.EqualFold(comm, "auth") {
		return nil
	}

	// Get current connection ACL details
	connection := acl.Connections[conn]

	// If password is not required, allow the connection
	if !acl.Config.RequirePass {
		return nil
	}

	// 1. Check if password is required and if the user is authenticated
	if acl.Config.RequirePass && !connection.Authenticated {
		return errors.New("user must be authenticated")
	}

	// 2. Check if all categories are in IncludedCategories
	var notAllowed []string
	if !slices.ContainsFunc(categories, func(category string) bool {
		return slices.ContainsFunc(connection.User.IncludedCategories, func(includedCategory string) bool {
			if includedCategory == "*" || includedCategory == category {
				return true
			}
			notAllowed = append(notAllowed, fmt.Sprintf("@%s", category))
			return false
		})
	}) {
		if len(notAllowed) == 0 {
			notAllowed = []string{"@all"}
		}
		return fmt.Errorf("unauthorized access to the following categories: %+v", notAllowed)
	}

	// 3. Check if commands category is in ExcludedCategories
	if slices.ContainsFunc(categories, func(category string) bool {
		return slices.ContainsFunc(connection.User.ExcludedCategories, func(excludedCategory string) bool {
			if excludedCategory == "*" || excludedCategory == category {
				notAllowed = []string{fmt.Sprintf("@%s", category)}
				return true
			}
			return false
		})
	}) {
		return fmt.Errorf("unauthorized access to the following categories: %+v", notAllowed)
	}

	// 4. Check if commands are in IncludedCommands
	if !slices.ContainsFunc(connection.User.IncludedCommands, func(includedCommand string) bool {
		return includedCommand == "*" || includedCommand == comm
	}) {
		return fmt.Errorf("not authorised to run %s command", comm)
	}

	// 5. Check if command are in ExcludedCommands
	if slices.ContainsFunc(connection.User.ExcludedCommands, func(excludedCommand string) bool {
		return excludedCommand == "*" || excludedCommand == comm
	}) {
		return fmt.Errorf("not authorised to run %s command", comm)
	}

	// 6. PUBSUB authorisation.
	if slices.Contains(categories, constants.PubSubCategory) {
		// In PUBSUB, KeyExtractionFunc returns channels so keys[0] is aliased to channel
		channel := keys[0]
		// 2.1) Check if the channel is in IncludedPubSubChannels
		if !slices.ContainsFunc(connection.User.IncludedPubSubChannels, func(includedChannelGlob string) bool {
			return acl.GlobPatterns[includedChannelGlob].Match(channel)
		}) {
			return fmt.Errorf("not authorised to access channel &%s", channel)
		}
		// 2.2) Check if the channel is in ExcludedPubSubChannels
		if slices.ContainsFunc(connection.User.ExcludedPubSubChannels, func(excludedChannelGlob string) bool {
			return acl.GlobPatterns[excludedChannelGlob].Match(channel)
		}) {
			return fmt.Errorf("not authorised to access channel &%s", channel)
		}
		return nil
	}

	if len(keys) > 0 {
		// 7. Check if nokeys is true
		if connection.User.NoKeys {
			return errors.New("not authorised to access any keys")
		}

		// 8. If @read is in the list of categories, check if keys are in IncludedReadKeys
		if slices.Contains(categories, constants.ReadCategory) {
			if !slices.ContainsFunc(keys, func(key string) bool {
				return slices.ContainsFunc(connection.User.IncludedReadKeys, func(readKeyGlob string) bool {
					if acl.GlobPatterns[readKeyGlob].Match(key) {
						return true
					}
					notAllowed = append(notAllowed, fmt.Sprintf("%s~%s", "%R", key))
					return false
				})
			}) {
				return fmt.Errorf("not authorised to access the following keys %+v", notAllowed)
			}
		}

		// 9. If @write is in the list of categories, check if keys are in IncludedWriteKeys
		if slices.Contains(categories, constants.WriteCategory) {
			if !slices.ContainsFunc(keys, func(key string) bool {
				return slices.ContainsFunc(connection.User.IncludedWriteKeys, func(writeKeyGlob string) bool {
					if acl.GlobPatterns[writeKeyGlob].Match(key) {
						return true
					}
					notAllowed = append(notAllowed, fmt.Sprintf("%s~%s", "%W", key))
					return false
				})
			}) {
				return fmt.Errorf("not authorised to access the following keys %+v", notAllowed)
			}
		}
	}

	return nil
}

func (acl *ACL) CompileGlobs() {
	// Extract all the relevant globs from all the users
	var allGlobs []string
	var userGlobs []string
	for _, user := range acl.Users {
		userGlobs = append(userGlobs, user.IncludedPubSubChannels...)
		userGlobs = append(userGlobs, user.ExcludedPubSubChannels...)
		userGlobs = append(userGlobs, user.IncludedReadKeys...)
		userGlobs = append(userGlobs, user.IncludedWriteKeys...)
		for _, g := range userGlobs {
			if !slices.Contains(allGlobs, g) {
				allGlobs = append(allGlobs, g)
			}
		}
		userGlobs = []string{}
	}
	// Compile the globs that have not been compiled yet
	for _, g := range allGlobs {
		if acl.GlobPatterns[g] == nil {
			acl.GlobPatterns[g] = glob.MustCompile(g)
		}
	}
}

func (acl *ACL) LockUsers() {
	acl.UsersMutex.Lock()
}

func (acl *ACL) UnlockUsers() {
	acl.UsersMutex.Unlock()
}

func (acl *ACL) RLockUsers() {
	acl.UsersMutex.RLock()
}

func (acl *ACL) RUnlockUsers() {
	acl.UsersMutex.RUnlock()
}
