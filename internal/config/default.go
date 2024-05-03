package config

import (
	"github.com/echovault/echovault/internal/constants"
	"time"
)

func DefaultConfig() Config {
	return Config{
		TLS:                false,
		MTLS:               false,
		CertKeyPairs:       make([][]string, 0),
		ClientCAs:          make([]string, 0),
		Port:               7480,
		ServerID:           "",
		JoinAddr:           "",
		BindAddr:           "localhost",
		RaftBindPort:       7481,
		MemberListBindPort: 7946,
		InMemory:           false,
		DataDir:            ".",
		BootstrapCluster:   false,
		AclConfig:          "",
		ForwardCommand:     false,
		RequirePass:        false,
		Password:           "",
		SnapShotThreshold:  1000,
		SnapshotInterval:   5 * time.Minute,
		RestoreAOF:         false,
		RestoreSnapshot:    false,
		AOFSyncStrategy:    "everysec",
		MaxMemory:          0,
		EvictionPolicy:     constants.NoEviction,
		EvictionSample:     20,
		EvictionInterval:   100 * time.Millisecond,
		Modules:            make([]string, 0),
	}
}
