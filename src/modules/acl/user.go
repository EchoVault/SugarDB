package acl

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
	user.IncludedCommands = RemoveDuplicateEntries(user.IncludedCommands, "allCommands")
	user.ExcludedCommands = RemoveDuplicateEntries(user.ExcludedCommands, "allCommands")
	user.IncludedKeys = RemoveDuplicateEntries(user.IncludedKeys, "allKeys")
	user.IncludedReadKeys = RemoveDuplicateEntries(user.IncludedReadKeys, "allKeys")
	user.IncludedWriteKeys = RemoveDuplicateEntries(user.IncludedWriteKeys, "allKeys")
	user.IncludedPubSubChannels = RemoveDuplicateEntries(user.IncludedPubSubChannels, "allChannels")
	user.ExcludedPubSubChannels = RemoveDuplicateEntries(user.ExcludedPubSubChannels, "allChannels")
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
