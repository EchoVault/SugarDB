package acl

import (
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
)

type Password struct {
	PasswordType  string `json:"PasswordType" yaml:"PasswordType"` // plaintext, SHA256
	PasswordValue string `json:"PasswordValue" yaml:"PasswordValue"`
}

type User struct {
	Username string `json:"Username" yaml:"Username"`
	Enabled  bool   `json:"Enabled" yaml:"Enabled"`

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
	User          User
}

type ACL struct {
	Users       []User
	Connections map[*net.Conn]Connection
	Config      utils.Config
}

func NewACL(config utils.Config) *ACL {
	var users []User

	// 1. Initialise default ACL user
	defaultUser := CreateUser("default", true)
	if config.RequirePass {
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
			u, err := MergeUser(defaultUser, user)
			if err != nil {
				fmt.Println(err)
				continue
			}
			users[i] = u
		} else {
			u, err := MergeUser(CreateUser(user.Username, user.Enabled), user)
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
	defaultUser := utils.Filter(acl.Users, func(elem User) bool {
		return elem.Username == "default"
	})[0]
	acl.Connections[conn] = Connection{
		Authenticated: false,
		User:          defaultUser,
	}
}

func (acl *ACL) AuthenticateConnection(conn *net.Conn, cmd []string) error {
	var passwords []Password
	var user User

	h := sha256.New()

	if len(cmd) == 2 {
		// Process AUTH <password>
		h.Write([]byte(cmd[1]))
		passwords = []Password{
			{PasswordType: "plaintext", PasswordValue: cmd[1]},
			{PasswordType: "SHA256", PasswordValue: string(h.Sum(nil))},
		}
		// Authenticate with default user
		user = utils.Filter(acl.Users, func(elem User) bool {
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
	if strings.Split(password, "")[0] == "#" {
		return "SHA256"
	}
	return "plaintext"
}

func CreateUser(username string, enabled bool) User {
	return User{
		Username:               username,
		Enabled:                enabled,
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

func NormaliseUser(user User) User {
	// Normalise the user object
	result := user

	result.IncludedCategories =
		NormaliseAllEntries(RemoveDuplicates(result.IncludedCategories), "allCategories", true)
	result.ExcludedCategories =
		NormaliseAllEntries(RemoveDuplicates(result.ExcludedCategories), "allCategories", false)
	result.IncludedCommands =
		NormaliseAllEntries(RemoveDuplicates(result.IncludedCommands), "allCommands", true)
	result.ExcludedCommands =
		NormaliseAllEntries(RemoveDuplicates(result.ExcludedCommands), "allCommands", false)
	result.IncludedKeys =
		NormaliseAllEntries(RemoveDuplicates(result.IncludedKeys), "allKeys", true)
	result.IncludedReadKeys =
		NormaliseAllEntries(RemoveDuplicates(result.IncludedReadKeys), "allKeys", true)
	result.IncludedWriteKeys =
		NormaliseAllEntries(RemoveDuplicates(result.IncludedWriteKeys), "allKeys", true)
	result.IncludedPubSubChannels =
		NormaliseAllEntries(RemoveDuplicates(result.IncludedPubSubChannels), "allChannels", true)
	result.ExcludedPubSubChannels =
		NormaliseAllEntries(RemoveDuplicates(result.ExcludedPubSubChannels), "allChannels", false)

	return result
}

func MergeUser(base, target User) (User, error) {
	if base.Username != target.Username {
		return User{},
			fmt.Errorf("cannot merge user with username %s to user with username %s", base.Username, target.Username)
	}

	result := base

	result.Enabled = target.Enabled
	result.Passwords = append(base.Passwords, target.Passwords...)
	result.IncludedCategories = append(base.IncludedCategories, target.IncludedCategories...)
	result.ExcludedCategories = append(base.ExcludedCategories, target.ExcludedCategories...)
	result.IncludedCommands = append(base.IncludedCommands, target.IncludedCommands...)
	result.ExcludedCommands = append(base.ExcludedCommands, target.ExcludedCommands...)
	result.IncludedReadKeys = append(base.IncludedReadKeys, target.IncludedReadKeys...)
	result.IncludedWriteKeys = append(base.IncludedWriteKeys, target.IncludedWriteKeys...)
	result.IncludedPubSubChannels = append(base.IncludedPubSubChannels, target.IncludedPubSubChannels...)
	result.ExcludedPubSubChannels = append(base.ExcludedPubSubChannels, target.ExcludedPubSubChannels...)

	return NormaliseUser(result), nil
}
