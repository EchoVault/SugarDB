package commands

import (
	"github.com/echovault/echovault/pkg/modules/acl"
	"github.com/echovault/echovault/pkg/modules/admin"
	"github.com/echovault/echovault/pkg/modules/connection"
	"github.com/echovault/echovault/pkg/modules/generic"
	"github.com/echovault/echovault/pkg/modules/hash"
	"github.com/echovault/echovault/pkg/modules/list"
	"github.com/echovault/echovault/pkg/modules/pubsub"
	"github.com/echovault/echovault/pkg/modules/set"
	"github.com/echovault/echovault/pkg/modules/sorted_set"
	str "github.com/echovault/echovault/pkg/modules/string"
	"github.com/echovault/echovault/pkg/utils"
)

// All returns all the commands currently available on EchoVault
func All() []utils.Command {
	var commands []utils.Command
	commands = append(commands, acl.Commands()...)
	commands = append(commands, admin.Commands()...)
	commands = append(commands, generic.Commands()...)
	commands = append(commands, hash.Commands()...)
	commands = append(commands, list.Commands()...)
	commands = append(commands, connection.Commands()...)
	commands = append(commands, pubsub.Commands()...)
	commands = append(commands, set.Commands()...)
	commands = append(commands, sorted_set.Commands()...)
	commands = append(commands, str.Commands()...)
	return commands
}
