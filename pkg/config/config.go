package config

import "github.com/echovault/echovault/internal"

// Config returns the default configuration.
func Config() (internal.Config, error) {
	return internal.GetConfig()
}
