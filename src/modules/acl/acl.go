package acl

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"time"
)

type Password struct {
	PasswordType  string `json:"PasswordType" yaml:"PasswordType"` // plaintext, SHA256
	PasswordValue string `json:"PasswordValue" yaml:"PasswordValue"`
}

type User struct {
	Username   string `json:"Username" yaml:"Username"`
	Enabled    bool   `json:"Enabled" yaml:"Enabled"`
	NoPassword bool   `json:"NoPassword" yaml:"NoPassword"`

	Passwords []Password `json:"Passwords" yaml:"Passwords"`

	IncludedCategories []string `json:"IncludedCategories" yaml:"IncludedCategories"`
	ExcludedCategories []string `json:"ExcludedCategories" yaml:"ExcludedCategories"`

	IncludedCommands []string `json:"IncludedCommands" yaml:"IncludedCommands"`
	ExcludedCommands []string `json:"ExcludedCommands" yaml:"ExcludedCommands"`

	IncludedKeys      []string `json:"IncludedKeys" yaml:"IncludedKeys"`
	IncludedReadKeys  []string `json:"IncludedReadKeys" yaml:"IncludedReadKeys"`
	IncludedWriteKeys []string `json:"IncludedWriteKeys" yaml:"IncludedWriteKeys"`

	IncludedPubSubChannels []string `json:"IncludedPubSubChannels" yaml:"IncludedPubSubChannels"`
	ExcludedPubSubChannels []string `json:"ExcludedPubSubChannels" yaml:"ExcludedPubSubChannels"`
}

type Connection struct {
	Authenticated bool
	User          *User
}

type ACL struct {
	Users       []*User
	Connections map[*net.Conn]Connection
	Config      utils.Config
}

func NewACL(config utils.Config) *ACL {
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

	// 3.
	// i) Merge created default user and loaded default user
	// ii) Merge other users with sensible defaults
	for i, user := range users {
		if user.Username == "default" {
			err := MergeUser(defaultUser, user)
			if err != nil {
				fmt.Println(err)
				continue
			}
		} else {
			u := CreateUser(user.Username)
			err := MergeUser(u, user)
			if err != nil {
				fmt.Println(err)
				continue
			}
			users[i] = u
		}
	}

	acl := ACL{
		Users:       users,
		Connections: make(map[*net.Conn]Connection),
		Config:      config,
	}

	return &acl
}

func (acl *ACL) RegisterConnection(conn *net.Conn) {
	// This is called only when a connection is established.
	defaultUser := utils.Filter(acl.Users, func(elem *User) bool {
		return elem.Username == "default"
	})[0]
	acl.Connections[conn] = Connection{
		Authenticated: !defaultUser.NoPassword,
		User:          defaultUser,
	}
}

func (acl *ACL) SetUser(ctx context.Context, cmd []string) error {
	user := CreateUser(cmd[0])

	// Check if user with the given username already exists
	// If it does, replace user variable with this user
	for _, u := range acl.Users {
		if u.Username == cmd[0] {
			user = u
		}
	}

	for _, str := range cmd {
		// Parse enabled
		if strings.EqualFold(str, "on") {
			user.Enabled = true
		}
		if strings.EqualFold(str, "off") {
			user.Enabled = false
		}
		// Parse passwords
		if str[0] == '>' || str[0] == '#' {
			user.Passwords = append(user.Passwords, Password{
				PasswordType:  GetPasswordType(str),
				PasswordValue: str[1:],
			})
			user.NoPassword = false
			continue
		}
		if str[0] == '<' {
			user.Passwords = utils.Filter(user.Passwords, func(password Password) bool {
				if strings.EqualFold(password.PasswordType, "SHA256") {
					return true
				}
				return password.PasswordValue == str[1:]
			})
			continue
		}
		if str[0] == '!' {
			user.Passwords = utils.Filter(user.Passwords, func(password Password) bool {
				if strings.EqualFold(password.PasswordType, "plaintext") {
					return true
				}
				return password.PasswordValue == str[1:]
			})
			continue
		}
		// Parse categories
		if strings.EqualFold(str, "nocommands") {
			user.ExcludedCategories = []string{"*"}
			user.ExcludedCommands = []string{"*"}
			continue
		}
		if strings.EqualFold(str, "allCategories") {
			user.IncludedCategories = []string{"*"}
			continue
		}
		if len(str) > 3 && str[1] == '@' {
			if str[0] == '+' {
				user.IncludedCategories = append(user.IncludedCategories, str[2:])
				continue
			}
			if str[0] == '-' {
				user.ExcludedCategories = append(user.ExcludedCategories, str[2:])
				continue
			}
		}
		// Parse keys
		if strings.EqualFold(str, "allKeys") {
			user.IncludedKeys = []string{"*"}
			user.IncludedReadKeys = []string{"*"}
			user.IncludedWriteKeys = []string{"*"}
			continue
		}
		if len(str) > 1 && str[0] == '~' {
			user.IncludedKeys = append(user.IncludedKeys, str[1:])
			continue
		}
		if len(str) > 4 && strings.EqualFold(str[0:4], "%RW~") {
			user.IncludedKeys = append(user.IncludedKeys, str[3:])
			continue
		}
		if len(str) > 3 && strings.EqualFold(str[0:4], "%R~") {
			user.IncludedReadKeys = append(user.IncludedReadKeys, str[2:])
			continue
		}
		if len(str) > 3 && strings.EqualFold(str[0:4], "%w~") {
			user.IncludedWriteKeys = append(user.IncludedWriteKeys, str[2:])
			continue
		}
		// Parse channels
		if strings.EqualFold(str, "allChannels") {
			user.IncludedPubSubChannels = []string{"*"}
		}
		if len(str) > 2 && str[1] == '&' {
			if str[0] == '+' {
				user.IncludedPubSubChannels = append(user.IncludedPubSubChannels, str[2:])
				continue
			}
			if str[0] == '-' {
				user.ExcludedPubSubChannels = append(user.ExcludedPubSubChannels, str[2:])
				continue
			}
		}
		// Parse commands
		if strings.EqualFold(str, "allCommands") {
			user.IncludedCommands = []string{"*"}
			continue
		}
		if len(str) > 2 && !utils.Contains([]uint8{'&', '@'}, str[1]) {
			if str[0] == '+' {
				user.IncludedCommands = append(user.IncludedCommands, str[1:])
				continue
			}
			if str[0] == '-' {
				user.ExcludedCommands = append(user.ExcludedCommands, str[1:])
				continue
			}
		}
	}

	// If nopass is provided, delete all passwords
	for _, str := range cmd {
		if strings.EqualFold(str, "nopass") {
			user.Passwords = []Password{}
			user.NoPassword = true
		}
	}

	// If resetpass is provided, delete all passwords and set NoPassword to false
	for _, str := range cmd {
		if strings.EqualFold(str, "resetpass") {
			user.Passwords = []Password{}
			user.NoPassword = false
		}
	}

	// Add user to ACL
	acl.Users = append(utils.Filter(acl.Users, func(u *User) bool {
		return u.Username != user.Username
	}), user)

	fmt.Printf("%+v\n", user)

	return nil
}

func (acl *ACL) DeleteUser(ctx context.Context, usernames []string) error {
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
				(*connRef).SetReadDeadline(time.Now().Add(-1 * time.Second))
			}
		}
		// Delete the user from the ACL
		acl.Users = utils.Filter(acl.Users, func(u *User) bool {
			return u.Username != user.Username
		})
	}
	return nil
}

func (acl *ACL) AuthenticateConnection(ctx context.Context, conn *net.Conn, cmd []string) error {
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
		user = utils.Filter(acl.Users, func(user *User) bool {
			return user.Username == "default"
		})[0]
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

func (acl *ACL) AuthorizeConnection(conn *net.Conn, cmd []string, command utils.Command, subCommand interface{}) error {
	// 1. Check if password is required and if we're authorized
	// 2. Check if commands category is in IncludedCommands
	// 3. Check if commands category is in ExcludedCommands
	// 4. Check if commands is in IncludedCommands
	// 5. Check if commands is in ExcludedCommands
	// 6. Check if keys are in IncludedKeys
	// 7. Check if keys are in ExcludedKeys
	return nil
}

func GetPasswordType(password string) string {
	if password[0] == '#' {
		return "SHA256"
	}
	return "plaintext"
}

func CreateUser(username string) *User {
	return &User{
		Username:               username,
		Enabled:                true,
		NoPassword:             false,
		Passwords:              []Password{},
		IncludedCategories:     []string{},
		ExcludedCategories:     []string{},
		IncludedCommands:       []string{},
		ExcludedCommands:       []string{},
		IncludedKeys:           []string{},
		IncludedReadKeys:       []string{},
		IncludedWriteKeys:      []string{},
		IncludedPubSubChannels: []string{},
		ExcludedPubSubChannels: []string{},
	}
}

func RemoveDuplicates(slice []string) []string {
	entries := make(map[string]int)
	keys := []string{}

	for _, s := range slice {
		entries[s] += 1
	}

	for key, _ := range entries {
		keys = append(keys, key)
	}

	return keys
}

func NormaliseAllEntries(slice []string, allAlias string, defaultAllIncluded bool) []string {
	result := slice
	if utils.Contains(result, "*") || utils.Contains(result, allAlias) {
		result = []string{"*"}
	}
	if len(result) == 0 && defaultAllIncluded {
		result = []string{"*"}
	}
	return result
}

func NormaliseUser(user *User) {
	// Normalise the user object
	user.IncludedCategories =
		NormaliseAllEntries(RemoveDuplicates(user.IncludedCategories), "allCategories", true)
	user.ExcludedCategories =
		NormaliseAllEntries(RemoveDuplicates(user.ExcludedCategories), "allCategories", false)
	user.IncludedCommands =
		NormaliseAllEntries(RemoveDuplicates(user.IncludedCommands), "allCommands", true)
	user.ExcludedCommands =
		NormaliseAllEntries(RemoveDuplicates(user.ExcludedCommands), "allCommands", false)
	user.IncludedKeys =
		NormaliseAllEntries(RemoveDuplicates(user.IncludedKeys), "allKeys", true)
	user.IncludedReadKeys =
		NormaliseAllEntries(RemoveDuplicates(user.IncludedReadKeys), "allKeys", true)
	user.IncludedWriteKeys =
		NormaliseAllEntries(RemoveDuplicates(user.IncludedWriteKeys), "allKeys", true)
	user.IncludedPubSubChannels =
		NormaliseAllEntries(RemoveDuplicates(user.IncludedPubSubChannels), "allChannels", true)
	user.ExcludedPubSubChannels =
		NormaliseAllEntries(RemoveDuplicates(user.ExcludedPubSubChannels), "allChannels", false)
}

func MergeUser(base, target *User) error {
	if base.Username != target.Username {
		return fmt.Errorf("cannot merge user with username %s to user with username %s",
			base.Username, target.Username)
	}

	base.Enabled = target.Enabled
	base.Passwords = append(base.Passwords, target.Passwords...)
	base.IncludedCategories = append(base.IncludedCategories, target.IncludedCategories...)
	base.ExcludedCategories = append(base.ExcludedCategories, target.ExcludedCategories...)
	base.IncludedCommands = append(base.IncludedCommands, target.IncludedCommands...)
	base.ExcludedCommands = append(base.ExcludedCommands, target.ExcludedCommands...)
	base.IncludedReadKeys = append(base.IncludedReadKeys, target.IncludedReadKeys...)
	base.IncludedWriteKeys = append(base.IncludedWriteKeys, target.IncludedWriteKeys...)
	base.IncludedPubSubChannels = append(base.IncludedPubSubChannels, target.IncludedPubSubChannels...)
	base.ExcludedPubSubChannels = append(base.ExcludedPubSubChannels, target.ExcludedPubSubChannels...)

	NormaliseUser(base)

	return nil
}
