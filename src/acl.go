package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path"
)

type Password struct {
	PasswordType  string `json:"PasswordType" yaml:"PasswordType"` // plaintext, SHA256
	PasswordValue string `json:"PasswordValue" yaml:"PasswordValue"`
}

type UserPassword struct {
	Enabled   bool       `json:"Enabled" yaml:"Enabled"`
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
	Users []User
}

func NewACL(aclConfig string) *ACL {
	users := []User{}

	// 1. Initialise default ACL user

	// 2. Read and parse the ACL config file and set the
	if aclConfig != "" {
		// Override acl configurations from file
		if f, err := os.Open(aclConfig); err != nil {
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

	// 3. Validate the ACL Config that has been loaded from the file

	return &ACL{
		Users: users,
	}
}
