// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package echovault

import (
	"crypto/sha256"
	"fmt"
	"github.com/echovault/echovault/internal/constants"
	"slices"
	"strings"
	"testing"
)

func TestEchoVault_ACLCat(t *testing.T) {
	server := createEchoVault()

	getCategoryCommands := func(category string) []string {
		var commands []string
		for _, command := range server.commands {
			if slices.Contains(command.Categories, category) && (command.SubCommands == nil || len(command.SubCommands) == 0) {
				commands = append(commands, strings.ToLower(command.Command))
				continue
			}
			for _, subcommand := range command.SubCommands {
				if slices.Contains(subcommand.Categories, category) {
					commands = append(commands, strings.ToLower(fmt.Sprintf("%s|%s", command.Command, subcommand.Command)))
				}
			}
		}
		return commands
	}

	tests := []struct {
		name    string
		args    []string
		want    []string
		wantErr bool
	}{
		{
			name: "1. Get all ACL categories loaded on the server",
			args: make([]string, 0),
			want: []string{
				constants.AdminCategory, constants.ConnectionCategory, constants.DangerousCategory,
				constants.HashCategory, constants.FastCategory, constants.KeyspaceCategory, constants.ListCategory,
				constants.PubSubCategory, constants.ReadCategory, constants.WriteCategory, constants.SetCategory,
				constants.SortedSetCategory, constants.SlowCategory, constants.StringCategory,
			},
			wantErr: false,
		},
		{
			name:    "2. Get all commands within the admin category",
			args:    []string{constants.AdminCategory},
			want:    getCategoryCommands(constants.AdminCategory),
			wantErr: false,
		},
		{
			name:    "3. Get all commands within the connection category",
			args:    []string{constants.ConnectionCategory},
			want:    getCategoryCommands(constants.ConnectionCategory),
			wantErr: false,
		},
		{
			name:    "4. Get all the commands within the dangerous category",
			args:    []string{constants.DangerousCategory},
			want:    getCategoryCommands(constants.DangerousCategory),
			wantErr: false,
		},
		{
			name:    "5. Get all the commands within the hash category",
			args:    []string{constants.HashCategory},
			want:    getCategoryCommands(constants.HashCategory),
			wantErr: false,
		},
		{
			name:    "6. Get all the commands within the fast category",
			args:    []string{constants.FastCategory},
			want:    getCategoryCommands(constants.FastCategory),
			wantErr: false,
		},
		{
			name:    "7. Get all the commands within the keyspace category",
			args:    []string{constants.KeyspaceCategory},
			want:    getCategoryCommands(constants.KeyspaceCategory),
			wantErr: false,
		},
		{
			name:    "8. Get all the commands within the list category",
			args:    []string{constants.ListCategory},
			want:    getCategoryCommands(constants.ListCategory),
			wantErr: false,
		},
		{
			name:    "9. Get all the commands within the pubsub category",
			args:    []string{constants.PubSubCategory},
			want:    getCategoryCommands(constants.PubSubCategory),
			wantErr: false,
		},
		{
			name:    "10. Get all the commands within the read category",
			args:    []string{constants.ReadCategory},
			want:    getCategoryCommands(constants.ReadCategory),
			wantErr: false,
		},
		{
			name:    "11. Get all the commands within the write category",
			args:    []string{constants.WriteCategory},
			want:    getCategoryCommands(constants.WriteCategory),
			wantErr: false,
		},
		{
			name:    "12. Get all the commands within the set category",
			args:    []string{constants.SetCategory},
			want:    getCategoryCommands(constants.SetCategory),
			wantErr: false,
		},
		{
			name:    "13. Get all the commands within the sortedset category",
			args:    []string{constants.SortedSetCategory},
			want:    getCategoryCommands(constants.SortedSetCategory),
			wantErr: false,
		},
		{
			name:    "14. Get all the commands within the slow category",
			args:    []string{constants.SlowCategory},
			want:    getCategoryCommands(constants.SlowCategory),
			wantErr: false,
		},
		{
			name:    "15. Get all the commands within the string category",
			args:    []string{constants.StringCategory},
			want:    getCategoryCommands(constants.StringCategory),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := server.ACLCat(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ACLCat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("ACLCat() got length = %d, want length %d", len(got), len(tt.want))
			}
			for _, item := range got {
				if !slices.Contains(tt.want, item) {
					t.Errorf("ACLCat() got unexpected element = %s, want %v", item, tt.want)
				}
			}
		})
	}
}

func TestEchoVault_ACLUsers(t *testing.T) {
	server := createEchoVault()

	// Set Users
	users := []User{
		{
			Username:             "user1",
			Enabled:              true,
			NoPassword:           true,
			NoKeys:               true,
			NoCommands:           true,
			AddPlainPasswords:    []string{},
			AddHashPasswords:     []string{},
			IncludeCategories:    []string{},
			IncludeReadWriteKeys: []string{},
			IncludeReadKeys:      []string{},
			IncludeWriteKeys:     []string{},
			IncludeChannels:      []string{},
			ExcludeChannels:      []string{},
		},
		{
			Username:          "user2",
			Enabled:           true,
			NoPassword:        false,
			NoKeys:            false,
			NoCommands:        false,
			AddPlainPasswords: []string{"password1", "password2"},
			AddHashPasswords: []string{
				func() string {
					h := sha256.New()
					h.Write([]byte("password1"))
					return string(h.Sum(nil))
				}(),
			},
			IncludeCategories:    []string{constants.FastCategory, constants.SlowCategory, constants.HashCategory},
			ExcludeCategories:    []string{constants.AdminCategory, constants.DangerousCategory},
			IncludeCommands:      []string{"*"},
			ExcludeCommands:      []string{"acl|load", "acl|save"},
			IncludeReadWriteKeys: []string{"user2-profile-*"},
			IncludeReadKeys:      []string{"user2-privileges-*"},
			IncludeWriteKeys:     []string{"write-key"},
			IncludeChannels:      []string{"posts-*"},
			ExcludeChannels:      []string{"actions-*"},
		},
	}

	for _, user := range users {
		ok, err := server.ACLSetUser(user)
		if err != nil {
			t.Errorf("ACLSetUser() err = %v", err)
		}
		if !ok {
			t.Errorf("ACLSetUser() ok = %v", ok)
		}
	}

	// Get users
	aclUsers, err := server.ACLUsers()
	if err != nil {
		t.Errorf("ACLUsers() err = %v", err)
	}
	if len(aclUsers) != len(users)+1 {
		t.Errorf("ACLUsers() got length %d, want %d", len(aclUsers), len(users)+1)
	}
	for _, username := range aclUsers {
		if !slices.Contains([]string{"default", "user1", "user2"}, username) {
			t.Errorf("ACLUsers() unexpected username = %s", username)
		}
	}

	// Get specific user.
	user, err := server.ACLGetUser("user2")
	if err != nil {
		t.Errorf("ACLGetUser() err = %v", err)
	}
	if user == nil {
		t.Errorf("ACLGetUser() user is nil")
	}

	// Delete user
	ok, err := server.ACLDelUser("user1")
	if err != nil {
		t.Errorf("ACLDelUser() err = %v", err)
	}
	if !ok {
		t.Errorf("ACLDelUser() could not delete user user1")
	}
	aclUsers, err = server.ACLUsers()
	if err != nil {
		t.Errorf("ACLDelUser() err = %v", err)
	}
	if slices.Contains(aclUsers, "user1") {
		t.Errorf("ACLDelUser() unexpected username user1")
	}

	// Get list of currently loaded ACL rules.
	list, err := server.ACLList()
	if err != nil {
		t.Errorf("ACLList() err = %v", err)
	}
	if len(list) != 2 {
		t.Errorf("ACLList() got list length %d, want %d", len(list), 2)
	}
}
