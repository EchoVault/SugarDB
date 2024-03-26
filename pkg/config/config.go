package config

import (
	"github.com/echovault/echovault/internal/config"
)

// DefaultConfig returns the default configuration.
// / This should be used when using EchoVault as an embedded library.
func DefaultConfig() config.Config {
	return config.DefaultConfig()
}
