package acl

import (
	"encoding/json"
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

type UserPassword struct {
	Passwords []Password `json:"Passwords" yaml:"Passwords"`
}

type User struct {
	Username string `json:"Username" yaml:"Username"`
	Enabled  bool   `json:"Enabled" yaml:"Enabled"`

	Authentication UserPassword `json:"Authentication" yaml:"Authentication"`

	IncludedCategories []string `json:"IncludedCategories" yaml:"IncludedCategories"`
	ExcludedCategories []string `json:"ExcludedCategories" yaml:"ExcludedCategories"`

	IncludedCommands []string `json:"IncludedCommands" yaml:"IncludedCommands"`
	ExcludedCommands []string `json:"ExcludedCommands" yaml:"ExcludedCommands"`

	IncludedKeys      []string `json:"IncludedKeys" yaml:"IncludedKeys"`
	ExcludedKeys      []string `json:"ExcludedKeys" yaml:"ExcludedKeys"`
	IncludedReadKeys  []string `json:"IncludedReadKeys" yaml:"IncludedReadKeys"`
	IncludedWriteKeys []string `json:"IncludedWriteKeys" yaml:"IncludedWriteKeys"`

	IncludedPubSubChannels []string `json:"IncludedPubSubChannels" yaml:"IncludedPubSubChannels"`
	ExcludedPubSubChannels []string `json:"ExcludedPubSubChannels" yaml:"ExcludedPubSubChannels"`
}

type ACL struct {
	Users       []User
	Connections map[*net.Conn]*User
	Config      utils.Config
}

func NewACL(config utils.Config) *ACL {
	users := []User{}

	// 1. Initialise default ACL user
	defaultUser := User{
		Username: "default",
		Enabled:  true,
	}
	if config.RequirePass {
		defaultUser.Authentication = UserPassword{
			Passwords: []Password{
				{
					PasswordType:  GetPasswordType(config.Password),
					PasswordValue: config.Password,
				},
			},
		}
	}

	// 2. Read and parse the ACL config file and set the
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

	// 3. If users parsed from file do not contain "default" user, add the one we initialised in step 1
	hasDefault := false

	for _, user := range users {
		if user.Username == "default" {
			hasDefault = true
			break
		}
	}

	if !hasDefault {
		users = append([]User{defaultUser}, users...)
	}

	// 4. Normalise the ACL user Config.

	return &ACL{
		Users:       users,
		Connections: make(map[*net.Conn]*User),
		Config:      config,
	}
}

func (acl *ACL) RegisterConnection(conn *net.Conn) {
	fmt.Println("Register connection...")
}

func (acl *ACL) AuthenticateConnection(conn *net.Conn, cmd []string) error {
	return nil
}

func (acl *ACL) AuthorizeConnection(conn *net.Conn, cmd []string) error {
	return nil
}

func GetPasswordType(password string) string {
	if strings.Split(password, "")[0] == "#" {
		return "SHA256"
	}
	return "plaintext"
}
