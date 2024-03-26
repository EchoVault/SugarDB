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

// ByCategory only returns commands with at least one of the categories in the categories parameter
func ByCategory(categories []string) []utils.Command {
	commands := All()
	// TODO: Filter commands and subcommands by category
	return commands
}

// ByModule only returns commands that belong to one of the modules in the modules parameter
func ByModule(modules []string) []utils.Command {
	commands := All()
	// TODO: Filter commands by module
	return commands
}

// ExcludeCategories returns all commands except ones that have a category contained in the categories parameter.
func ExcludeCategories(categories []string) []utils.Command {
	commands := All()
	// TODO: Filter out commands and subcommands in the specified categories
	return commands
}

// ExcludeModules returns all commands except ones in a module included in the modules parameter
func ExcludeModules(modules []string) []utils.Command {
	commands := All()
	// TODO: Filter out commands in the specified modules
	return commands
}
