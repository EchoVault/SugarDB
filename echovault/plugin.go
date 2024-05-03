package echovault

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"plugin"
	"slices"
	"strings"
)

// TODO: Add godoc comment
func (server *EchoVault) LoadModule(path string, args ...string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("plugin open: %v", err)
	}

	commandSymbol, err := p.Lookup("Command")
	if err != nil {
		return err
	}
	command, ok := commandSymbol.(*string)
	if !ok {
		return errors.New("command symbol is not a string")
	}

	categoriesSymbol, err := p.Lookup("Categories")
	if err != nil {
		return err
	}
	categories, ok := categoriesSymbol.(*[]string)
	if !ok {
		return errors.New("categories symbol not a string slice")
	}

	descriptionSymbol, err := p.Lookup("Description")
	if err != nil {
		return err
	}
	description, ok := descriptionSymbol.(*string)
	if !ok {
		return errors.New("description symbol is no a string")
	}

	syncSymbol, err := p.Lookup("Sync")
	if err != nil {
		return err
	}
	sync, ok := syncSymbol.(*bool)
	if !ok {
		return errors.New("sync symbol is not a bool")
	}

	keyExtractionFuncSymbol, err := p.Lookup("KeyExtractionFunc")
	if err != nil {
		return fmt.Errorf("key extraction func symbol: %v", err)
	}
	keyExtractionFunc, ok := keyExtractionFuncSymbol.(func(cmd []string, args ...string) ([]string, []string, error))
	if !ok {
		return errors.New("key extraction function has unexpected signature")
	}

	handlerFuncSymbol, err := p.Lookup("HandlerFunc")
	if err != nil {
		return fmt.Errorf("handler func symbol: %v", err)
	}
	handlerFunc, ok := handlerFuncSymbol.(func(
		ctx context.Context,
		command []string,
		keyExists func(ctx context.Context, key string) bool,
		keyLock func(ctx context.Context, key string) (bool, error),
		keyUnlock func(ctx context.Context, key string),
		keyRLock func(ctx context.Context, key string) (bool, error),
		keyRUnlock func(ctx context.Context, key string),
		createKeyAndLock func(ctx context.Context, key string) (bool, error),
		getValue func(ctx context.Context, key string) interface{},
		setValue func(ctx context.Context, key string, value interface{}) error,
		args ...string,
	) ([]byte, error))
	if !ok {
		return errors.New("handler function has unexpected signature")
	}

	server.commandsRWMut.Lock()
	defer server.commandsRWMut.Unlock()

	server.commands = append(server.commands, internal.Command{
		Command: *command,
		Module:  path,
		Categories: func() []string {
			// Convert all the categories to lower case for uniformity
			cats := make([]string, len(*categories))
			for i, cat := range *categories {
				cats[i] = strings.ToLower(cat)
			}
			return cats
		}(),
		Description: *description,
		Sync:        *sync,
		SubCommands: make([]internal.SubCommand, 0),
		KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
			readKeys, writeKeys, err := keyExtractionFunc(cmd, args...)
			if err != nil {
				return internal.KeyExtractionFuncResult{}, err
			}
			return internal.KeyExtractionFuncResult{
				Channels:  make([]string, 0),
				ReadKeys:  readKeys,
				WriteKeys: writeKeys,
			}, nil
		},
		HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
			return handlerFunc(
				params.Context,
				params.Command,
				params.KeyExists,
				params.KeyLock,
				params.KeyUnlock,
				params.KeyRLock,
				params.KeyRUnlock,
				params.CreateKeyAndLock,
				params.GetValue,
				params.SetValue,
			)
		},
	})

	return nil
}

// TODO: Add godoc comment
func (server *EchoVault) UnloadModule(module string) {
	server.commandsRWMut.Lock()
	defer server.commandsRWMut.Unlock()
	server.commands = slices.DeleteFunc(server.commands, func(command internal.Command) bool {
		return strings.EqualFold(command.Module, module)
	})
}

// TODO: Add godoc comment
func (server *EchoVault) ListModules() []string {
	server.commandsRWMut.RLock()
	defer server.commandsRWMut.RUnlock()
	var modules []string
	for _, command := range server.commands {
		if !slices.ContainsFunc(modules, func(module string) bool {
			return strings.EqualFold(module, command.Module)
		}) {
			modules = append(modules, strings.ToLower(command.Module))
		}
	}
	return modules
}
