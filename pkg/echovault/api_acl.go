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

package echovault

import (
	"bytes"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/tidwall/resp"
)

// ACLLOADOptions modifies the behaviour of the ACL_LOAD function.
// If Merge is true, the ACL configuration from the file will be merged with the in-memory ACL configuration.
// If Replace is set to true, the ACL configuration from the file will replace the in-memory ACL configuration.
// If both flags are set to true, Merge will be prioritised.
type ACLLOADOptions struct {
	Merge   bool
	Replace bool
}

// User is the user object passed to the ACL_SETUSER function to update an existing user or create a new user.
//
// Username - string - the user's username.
//
// Enabled - bool - whether the user should be enabled (i.e connections can authenticate with this user).
//
// NoPassword - bool - if true, this user can be authenticated against without a password.
//
// NoKeys - bool - if true, this user will not be allowed to access any keys.
//
// NoCommands - bool - if true, this user will not be allowed to execute any commands.
//
// ResetPass - bool - if true, all the user's configured passwords are removed and NoPassword is set to false.
//
// ResetKeys - bool - if true, the user's NoKeys flag is set to true and all their currently accessible keys are cleared.
//
// ResetChannels - bool - if true, the user will be allowed to access all PubSub channels.
//
// AddPlainPasswords - []string - the list of plaintext passwords to add to the user's passwords.
//
// RemovePlainPasswords - []string - the list of plaintext passwords to remove from the user's passwords.
//
// AddHashPasswords - []string - the list of SHA256 password hashes to add to the user's passwords.
//
// RemoveHashPasswords - []string - the list of SHA256 password hashes to add to the user's passwords.
//
// IncludeCategories - []string - the list of ACL command categories to allow this user to access, default is all.
//
// ExcludeCategories - []string - the list of ACL command categories to bar the user from accessing. The default is none.
//
// IncludeCommands - []string - the list of commands to allow the user to execute. The default is none. If you want to
// specify a subcommand, use the format "command|subcommand".
//
// ExcludeCommands - []string - the list of commands to bar the user from executing.
// The default is none. If you want to specify a subcommand, use the format "command|subcommand".
//
// IncludeReadWriteKeys - []string - the list of keys the user is allowed read and write access to. The default is all.
// This field accepts glob pattern strings.
//
// IncludeReadKeys - []string - the list of keys the user is allowed read access to. The default is all.
// This field accepts glob pattern strings.
//
// IncludeWriteKeys - []string - the list of keys the user is allowed write access to. The default is all.
// This field accepts glob pattern strings.
//
// IncludeChannels - []string - the list of PubSub channels the user is allowed to access ("SUBSCRIBE" and "PUBLISH").
// This field accepts glob pattern strings.
//
// ExcludeChannels - []string - the list of PubSub channels the user cannot access ("SUBSCRIBE" and "PUBLISH").
// This field accepts glob pattern strings.
type User struct {
	Username      string
	Enabled       bool
	NoPassword    bool
	NoKeys        bool
	NoCommands    bool
	ResetPass     bool
	ResetKeys     bool
	ResetChannels bool

	AddPlainPasswords    []string
	RemovePlainPasswords []string
	AddHashPasswords     []string
	RemoveHashPasswords  []string

	IncludeCategories []string
	ExcludeCategories []string

	IncludeCommands []string
	ExcludeCommands []string

	IncludeReadWriteKeys []string
	IncludeReadKeys      []string
	IncludeWriteKeys     []string

	IncludeChannels []string
	ExcludeChannels []string
}

// ACL_CAT returns either the list of all categories or the list of commands within a specified category.
//
// Parameters:
//
// `category` - ...string - an optional string specifying the category. If more than one category is passed,
// only the first one will be used.
//
// Returns: string slice of categories loaded in EchoVault if category is not specified. Otherwise, returns string
// slice of commands within the specified category.
//
// Errors:
//
// "category <category> not found" - when the provided category is not found in the loaded commands.
func (server *EchoVault) ACL_CAT(category ...string) ([]string, error) {
	cmd := []string{"ACL", "CAT"}
	if len(category) > 0 {
		cmd = append(cmd, category[0])
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// ACL_USERS returns a string slice containing the usernames of all the loaded users in the ACL module.
func (server *EchoVault) ACL_USERS() ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "USERS"}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// ACL_SETUSER modifies or creates a new user. If the user with the specified username exists, the ACL user will be modified.
// Otherwise, a new User is created.
//
// Parameters:
//
// `user` - User - The user object to add/update.
//
// Returns: "OK" if the user is successfully created/updated.
func (server *EchoVault) ACL_SETUSER(user User) (string, error) {
	cmd := []string{"ACL", "SETUSER", user.Username}

	if user.Enabled {
		cmd = append(cmd, "on")
	} else {
		cmd = append(cmd, "off")
	}

	if user.NoPassword {
		cmd = append(cmd, "nopass")
	}

	if user.NoKeys {
		cmd = append(cmd, "nokeys")
	}

	if user.NoCommands {
		cmd = append(cmd, "nocommands")
	}

	if user.ResetPass {
		cmd = append(cmd, "resetpass")
	}

	if user.ResetKeys {
		cmd = append(cmd, "resetkeys")
	}

	if user.ResetChannels {
		cmd = append(cmd, "resetchannels")
	}

	for _, password := range user.AddPlainPasswords {
		cmd = append(cmd, fmt.Sprintf(">%s", password))
	}

	for _, password := range user.RemovePlainPasswords {
		cmd = append(cmd, fmt.Sprintf("<%s", password))
	}

	for _, password := range user.AddHashPasswords {
		cmd = append(cmd, fmt.Sprintf("#%s", password))
	}

	for _, password := range user.RemoveHashPasswords {
		cmd = append(cmd, fmt.Sprintf("!%s", password))
	}

	for _, category := range user.IncludeCategories {
		cmd = append(cmd, fmt.Sprintf("+@%s", category))
	}

	for _, category := range user.ExcludeCategories {
		cmd = append(cmd, fmt.Sprintf("-@%s", category))
	}

	for _, command := range user.IncludeCommands {
		cmd = append(cmd, fmt.Sprintf("+%s", command))
	}

	for _, command := range user.ExcludeCommands {
		cmd = append(cmd, fmt.Sprintf("-%s", command))
	}

	for _, key := range user.IncludeReadWriteKeys {
		cmd = append(cmd, fmt.Sprintf("%s~%s", "%RW", key))
	}

	for _, key := range user.IncludeReadKeys {
		cmd = append(cmd, fmt.Sprintf("%s~%s", "%R", key))
	}

	for _, key := range user.IncludeWriteKeys {
		cmd = append(cmd, fmt.Sprintf("%s~%s", "%W", key))
	}

	for _, channel := range user.IncludeChannels {
		cmd = append(cmd, fmt.Sprintf("+&%s", channel))
	}

	for _, channel := range user.ExcludeChannels {
		cmd = append(cmd, fmt.Sprintf("-&%s", channel))
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

// ACL_GETUSER gets the ACL configuration of the name with the given username.
//
// Parameters:
//
// `username` - string - the username whose ACL rules you'd like to retrieve.
//
// Returns: A map[string][]string map where each key is the rule category and each value is a string slice of relevant values.
// The map returned has the following structure:
//
// "username" - string slice containing the user's username.
//
// "flags" - string slices containing the following values: "on" if the user is enabled, otherwise "off",
// "nokeys" if the user is not allowed to access any keys (and NoKeys is true),
// "nopass" if the user has no passwords (and NoPass is true).
//
// "categories" - string slice af ACL command categories associated with the user.
// If the user is allowed to access all categories, it will contain "+@*".
// For each category the user is allowed to access, the slice will contain "+@<category>".
// If the user is not allowed to access any categories, it will contain "-@*".
// For each category the user is not allowed to access, the slice will contain "-@<category>".
//
// "commands" - string slice af commands associated with the user.
// If the user is allowed to execute all commands, it will contain "+all".
// For each command the user is allowed to execute, the slice will contain "+<command>".
// If the user is not allowed to execute any commands, it will contain "-all".
// For each command the user is not allowed to execute, the slice will contain "-<category>".
//
// "keys" - string slice af keys associated with the user.
// If the user is allowed read/write access all keys, the slice will contain "%RW~*".
// For each key glob pattern the user has read/write access to, the slice will contain "%RW~<pattern>".
// If the user is allowed read access to all keys, the slice will contain "%R~*".
// For each key glob pattern the user has read access to, the slice will contain "%R~<pattern>".
// If the user is allowed write access to all keys, the slice will contain "%W~*".
// For each key glob pattern the user has write access to, the slice will contain "%W~<pattern>".
//
// "channels" - string slice af pubsub channels associated with the user.
// If the user is allowed to access all channels, the slice will contain "+&*".
// For each channel the user is allowed to access, the slice will contain "+&<channel>".
// If the user is not allowed to access any channels, the slice will contain "-&*".
// For each channel the user is not allowed to access, the slice will contain "-&<channel>".
//
// Errors:
//
// "user not found" - if the user requested does not exist in the ACL rules.
func (server *EchoVault) ACL_GETUSER(username string) (map[string][]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "GETUSER", username}), nil, false)
	if err != nil {
		return nil, err
	}

	r := resp.NewReader(bytes.NewReader(b))
	v, _, err := r.ReadValue()
	if err != nil {
		return nil, err
	}

	arr := v.Array()
	result := make(map[string][]string, len(arr)/2)

	for i := 0; i < len(arr); i += 2 {
		key := arr[i].String()
		value := arr[i+1].Array()

		result[key] = make([]string, len(value))

		for j := 0; j < len(value); j++ {
			result[key][i] = value[i].String()
		}
	}

	return result, nil
}

// ACL_DELUSER deletes all the users with the specified usernames.
//
// Parameters:
//
// `usernames` - ...string - A string of usernames to delete from the ACL module.
//
// Returns: "OK" if the deletion is successful.
func (server *EchoVault) ACL_DELUSER(usernames ...string) (string, error) {
	cmd := append([]string{"ACL", "DELUSER"}, usernames...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}

// ACL_LIST lists all the currently loaded ACL users and their rules.
func (server *EchoVault) ACL_LIST() ([]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "LIST"}), nil, false)
	if err != nil {
		return nil, err
	}
	return internal.ParseStringArrayResponse(b)
}

// ACL_LOAD loads the ACL configuration from the configured ACL file. The load function can either merge the loaded
// config with the in-memory config, or replace the in-memory config with the loaded config entirely.
//
// Parameters:
//
// `options` - ACLLOADOptions - modifies the load behaviour.
//
// Returns: "OK" if the load is successful.
func (server *EchoVault) ACL_LOAD(options ACLLOADOptions) (string, error) {
	cmd := []string{"ACL", "LOAD"}
	switch {
	case options.Merge:
		cmd = append(cmd, "MERGE")
	case options.Replace:
		cmd = append(cmd, "REPLACE")
	default:
		cmd = append(cmd, "REPLACE")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return "", err
	}

	return internal.ParseStringResponse(b)
}

// ACL_SAVE saves the current ACL configuration to the configured ACL file.
//
// Returns: "OK" if the save is successful.
func (server *EchoVault) ACL_SAVE() (string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ACL", "SAVE"}), nil, false)
	if err != nil {
		return "", err
	}
	return internal.ParseStringResponse(b)
}
