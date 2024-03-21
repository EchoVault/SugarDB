package acl

import (
	"slices"
	"strings"
)

const (
	PasswordPlainText = "plaintext"
	PasswordSHA256    = "SHA256"
)

type Password struct {
	PasswordType  string `json:"PasswordType" yaml:"PasswordType"` // plaintext, SHA256
	PasswordValue string `json:"PasswordValue" yaml:"PasswordValue"`
}

type User struct {
	Username   string `json:"Username" yaml:"Username"`
	Enabled    bool   `json:"Enabled" yaml:"Enabled"`
	NoPassword bool   `json:"NoPassword" yaml:"NoPassword"`
	NoKeys     bool   `json:"NoKeys" yaml:"NoKeys"`

	Passwords []Password `json:"Passwords" yaml:"Passwords"`

	IncludedCategories []string `json:"IncludedCategories" yaml:"IncludedCategories"`
	ExcludedCategories []string `json:"ExcludedCategories" yaml:"ExcludedCategories"`

	IncludedCommands []string `json:"IncludedCommands" yaml:"IncludedCommands"`
	ExcludedCommands []string `json:"ExcludedCommands" yaml:"ExcludedCommands"`

	IncludedReadKeys  []string `json:"IncludedReadKeys" yaml:"IncludedReadKeys"`
	IncludedWriteKeys []string `json:"IncludedWriteKeys" yaml:"IncludedWriteKeys"`

	IncludedPubSubChannels []string `json:"IncludedPubSubChannels" yaml:"IncludedPubSubChannels"`
	ExcludedPubSubChannels []string `json:"ExcludedPubSubChannels" yaml:"ExcludedPubSubChannels"`
}

func (user *User) Normalise() {
	user.IncludedCategories = RemoveDuplicateEntries(user.IncludedCategories, "allCategories")
	if len(user.IncludedCategories) == 0 {
		user.IncludedCategories = []string{"*"}
	}
	user.ExcludedCategories = RemoveDuplicateEntries(user.ExcludedCategories, "allCategories")
	if slices.Contains(user.ExcludedCategories, "*") {
		user.IncludedCategories = []string{}
	}

	user.IncludedCommands = RemoveDuplicateEntries(user.IncludedCommands, "allCommands")
	if len(user.IncludedCommands) == 0 {
		user.IncludedCommands = []string{"*"}
	}
	user.ExcludedCommands = RemoveDuplicateEntries(user.ExcludedCommands, "allCommands")
	if slices.Contains(user.ExcludedCommands, "*") {
		user.IncludedCommands = []string{}
	}

	user.IncludedReadKeys = RemoveDuplicateEntries(user.IncludedReadKeys, "allKeys")
	if len(user.IncludedReadKeys) == 0 && !user.NoKeys {
		user.IncludedReadKeys = []string{"*"}
	}
	user.IncludedWriteKeys = RemoveDuplicateEntries(user.IncludedWriteKeys, "allKeys")
	if len(user.IncludedWriteKeys) == 0 && !user.NoKeys {
		user.IncludedWriteKeys = []string{"*"}
	}

	user.IncludedPubSubChannels = RemoveDuplicateEntries(user.IncludedPubSubChannels, "allChannels")
	if len(user.IncludedPubSubChannels) == 0 {
		user.IncludedPubSubChannels = []string{"*"}
	}
	user.ExcludedPubSubChannels = RemoveDuplicateEntries(user.ExcludedPubSubChannels, "allChannels")
	if slices.Contains(user.ExcludedPubSubChannels, "*") {
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

func (user *User) UpdateUser(cmd []string) error {
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
			user.Passwords = slices.DeleteFunc(user.Passwords, func(password Password) bool {
				if strings.EqualFold(password.PasswordType, PasswordSHA256) {
					return false
				}
				return password.PasswordValue == str[1:]
			})
			continue
		}
		if str[0] == '!' {
			user.Passwords = slices.DeleteFunc(user.Passwords, func(password Password) bool {
				if strings.EqualFold(password.PasswordType, PasswordPlainText) {
					return false
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
			user.IncludedReadKeys = []string{"*"}
			user.IncludedWriteKeys = []string{"*"}
			user.NoKeys = false
			continue
		}
		if (len(str) > 1 && str[0] == '~') || len(str) > 4 && strings.EqualFold(str[0:4], "%RW~") {
			startIndex := strings.Index(str, "~") + 1
			user.IncludedReadKeys = append(user.IncludedReadKeys, str[startIndex:])
			user.IncludedWriteKeys = append(user.IncludedWriteKeys, str[startIndex:])
			user.NoKeys = false
			continue
		}
		if len(str) > 3 && strings.EqualFold(str[0:3], "%R~") {
			user.IncludedReadKeys = append(user.IncludedReadKeys, str[3:])
			user.NoKeys = false
			continue
		}
		if len(str) > 3 && strings.EqualFold(str[0:3], "%W~") {
			user.IncludedWriteKeys = append(user.IncludedWriteKeys, str[3:])
			user.NoKeys = false
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
		if len(str) > 2 && !slices.Contains([]uint8{'&', '@'}, str[1]) {
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

	for _, str := range cmd {
		// If resetpass is provided, delete all passwords and set NoPassword to false
		if strings.EqualFold(str, "resetpass") {
			user.Passwords = []Password{}
			user.NoPassword = false
		}
		// If nocommands is provided, disable all commands for this user
		if strings.EqualFold(str, "nocommands") {
			user.ExcludedCommands = []string{"*"}
		}
		// If resetkeys is provided, reset all keys that the user can access
		if strings.EqualFold(str, "resetkeys") {
			user.IncludedReadKeys = []string{}
			user.IncludedWriteKeys = []string{}
			user.NoKeys = true
		}
		// If resetchannels is provided, remove all the pub/sub channels that the user can access
		if strings.EqualFold(str, "resetchannels") {
			user.ExcludedPubSubChannels = []string{"*"}
		}
	}
	return nil
}

func (user *User) Merge(new *User) {
	user.Enabled = new.Enabled
	user.NoKeys = new.NoKeys
	user.NoPassword = new.NoPassword
	user.Passwords = append(user.Passwords, new.Passwords...)
	user.IncludedCategories = append(user.IncludedCategories, new.IncludedCategories...)
	user.ExcludedCategories = append(user.ExcludedCategories, new.ExcludedCategories...)
	user.IncludedCommands = append(user.IncludedCommands, new.IncludedCommands...)
	user.ExcludedCommands = append(user.ExcludedCommands, new.ExcludedCommands...)
	user.IncludedReadKeys = append(user.IncludedReadKeys, new.IncludedReadKeys...)
	user.IncludedWriteKeys = append(user.IncludedWriteKeys, new.IncludedWriteKeys...)
	user.IncludedPubSubChannels = append(user.IncludedPubSubChannels, new.IncludedPubSubChannels...)
	user.ExcludedPubSubChannels = append(user.ExcludedPubSubChannels, new.ExcludedPubSubChannels...)
	user.Normalise()
}

func (user *User) Replace(new *User) {
	user.Enabled = new.Enabled
	user.NoKeys = new.NoKeys
	user.NoPassword = new.NoPassword
	user.Passwords = new.Passwords
	user.IncludedCategories = new.IncludedCategories
	user.ExcludedCategories = new.ExcludedCategories
	user.IncludedCommands = new.IncludedCommands
	user.ExcludedCommands = new.ExcludedCommands
	user.IncludedReadKeys = new.IncludedReadKeys
	user.IncludedWriteKeys = new.IncludedWriteKeys
	user.IncludedPubSubChannels = new.IncludedPubSubChannels
	user.ExcludedPubSubChannels = new.ExcludedPubSubChannels
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
		IncludedReadKeys:       []string{},
		IncludedWriteKeys:      []string{},
		IncludedPubSubChannels: []string{},
		ExcludedPubSubChannels: []string{},
	}
}

func GetPasswordType(password string) string {
	if password[0] == '#' {
		return PasswordSHA256
	}
	return PasswordPlainText
}
