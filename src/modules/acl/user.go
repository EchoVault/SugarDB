package acl

import (
	"github.com/kelvinmwinuka/memstore/src/utils"
)

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

func (user *User) Normalise() {
	user.IncludedCategories = RemoveDuplicateEntries(user.IncludedCategories, "allCategories")
	user.ExcludedCategories = RemoveDuplicateEntries(user.ExcludedCategories, "allCategories")
	if utils.Contains(user.ExcludedCategories, "*") {
		user.IncludedCategories = []string{}
	}

	user.IncludedCommands = RemoveDuplicateEntries(user.IncludedCommands, "allCommands")
	user.ExcludedCommands = RemoveDuplicateEntries(user.ExcludedCommands, "allCommands")
	if utils.Contains(user.ExcludedCommands, "*") {
		user.IncludedCommands = []string{}
	}

	user.IncludedKeys = RemoveDuplicateEntries(user.IncludedKeys, "allKeys")
	if len(user.IncludedKeys) == 0 {
		user.IncludedKeys = []string{"*"}
	}
	user.IncludedReadKeys = RemoveDuplicateEntries(user.IncludedReadKeys, "allKeys")
	if len(user.IncludedReadKeys) == 0 {
		user.IncludedReadKeys = []string{"*"}
	}
	user.IncludedWriteKeys = RemoveDuplicateEntries(user.IncludedWriteKeys, "allKeys")
	if len(user.IncludedWriteKeys) == 0 {
		user.IncludedWriteKeys = []string{"*"}
	}

	user.IncludedPubSubChannels = RemoveDuplicateEntries(user.IncludedPubSubChannels, "allChannels")
	if len(user.IncludedPubSubChannels) == 0 {
		user.IncludedPubSubChannels = []string{"*"}
	}
	user.ExcludedPubSubChannels = RemoveDuplicateEntries(user.ExcludedPubSubChannels, "allChannels")
	if utils.Contains(user.ExcludedPubSubChannels, "*") {
		user.IncludedPubSubChannels = []string{}
	}
}

func RemoveDuplicateEntries(entries []string, allAlias string) (res []string) {
	entriesMap := make(map[string]int)
	for _, entry := range entries {
		if entry == allAlias {
			entriesMap["*"] += 1
			continue
		}
		entriesMap[entry] += 1
	}
	for key, _ := range entriesMap {
		if key == "*" {
			res = []string{"*"}
			return
		}
		res = append(res, key)
	}
	return
}

func GetPasswordType(password string) string {
	if password[0] == '#' {
		return "SHA256"
	}
	return "plaintext"
}
