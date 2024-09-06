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
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
	"github.com/echovault/echovault/internal/constants"
	"time"
)

// DefaultConfig returns the default configuration.
// This should be used when using EchoVault as an embedded library.
func DefaultConfig() config.Config {
	return config.DefaultConfig()
}

func (server *EchoVault) GetServerInfo() internal.ServerInfo {
	return internal.ServerInfo{
		Server:  "echovault",
		Version: constants.Version,
		Id:      server.config.ServerID,
		Mode: func() string {
			if server.isInCluster() {
				return "cluster"
			}
			return "standalone"
		}(),
		Role: func() string {
			if !server.isInCluster() {
				return "master"
			}
			if server.raft.IsRaftLeader() {
				return "master"
			}
			return "replica"
		}(),
		Modules: server.ListModules(),
	}
}

// WithTLS is an option to the NewEchoVault function that allows you to pass a
// custom TLS to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithTLS(b ...bool) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		if len(b) > 0 {
			echovault.config.TLS = b[0]
		} else {
			echovault.config.TLS = true
		}
	}
}

// WithMTLS is an option to the NewEchoVault function that allows you to pass a
// custom MTLS to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithMTLS(b ...bool) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		if len(b) > 0 {
			echovault.config.MTLS = b[0]
		} else {
			echovault.config.MTLS = true
		}
	}
}

// CertKeyPair defines the paths to the cert and key pair files respectively.
type CertKeyPair struct {
	Cert string
	Key  string
}

// WithCertKeyPairs is an option to the NewEchoVault function that allows you to pass a
// custom CertKeyPairs to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithCertKeyPairs(certKeyPairs []CertKeyPair) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		for _, pair := range certKeyPairs {
			echovault.config.CertKeyPairs = append(echovault.config.CertKeyPairs, []string{pair.Cert, pair.Key})
		}
	}
}

// WithClientCAs is an option to the NewEchoVault function that allows you to pass a
// custom ClientCAs to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithClientCAs(clientCAs []string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.ClientCAs = clientCAs
	}
}

// WithPort is an option to the NewEchoVault function that allows you to pass a
// custom Port to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithPort(port uint16) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.Port = port
	}
}

// WithServerID is an option to the NewEchoVault function that allows you to pass a
// custom ServerID to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithServerID(serverID string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.ServerID = serverID
	}
}

// WithJoinAddr is an option to the NewEchoVault function that allows you to pass a
// custom JoinAddr to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithJoinAddr(joinAddr string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.JoinAddr = joinAddr
	}
}

// WithBindAddr is an option to the NewEchoVault function that allows you to pass a
// custom BindAddr to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithBindAddr(bindAddr string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.BindAddr = bindAddr
	}
}

// WithDataDir is an option to the NewEchoVault function that allows you to pass a
// custom DataDir to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithDataDir(dataDir string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.DataDir = dataDir
	}
}

// WithBootstrapCluster is an option to the NewEchoVault function that allows you to pass a
// custom BootstrapCluster to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithBootstrapCluster(b ...bool) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		if len(b) > 0 {
			echovault.config.BootstrapCluster = b[0]
		} else {
			echovault.config.BootstrapCluster = true
		}
	}
}

// WithAclConfig is an option to the NewEchoVault function that allows you to pass a
// custom AclConfig to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithAclConfig(aclConfig string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.AclConfig = aclConfig
	}
}

// WithForwardCommand is an option to the NewEchoVault function that allows you to pass a
// custom ForwardCommand to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithForwardCommand(b ...bool) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		if len(b) > 0 {
			echovault.config.ForwardCommand = b[0]
		} else {
			echovault.config.ForwardCommand = true
		}
	}
}

// WithRequirePass is an option to the NewEchoVault function that allows you to pass a
// custom RequirePass to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithRequirePass(b ...bool) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		if len(b) > 0 {
			echovault.config.RequirePass = b[0]
		} else {
			echovault.config.RequirePass = true
		}
	}
}

// WithPassword is an option to the NewEchoVault function that allows you to pass a
// custom Password to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithPassword(password string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.Password = password
	}
}

// WithSnapShotThreshold is an option to the NewEchoVault function that allows you to pass a
// custom SnapShotThreshold to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithSnapShotThreshold(snapShotThreshold uint64) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.SnapShotThreshold = snapShotThreshold
	}
}

// WithSnapshotInterval is an option to the NewEchoVault function that allows you to pass a
// custom SnapshotInterval to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithSnapshotInterval(snapshotInterval time.Duration) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.SnapshotInterval = snapshotInterval
	}
}

// WithRestoreSnapshot is an option to the NewEchoVault function that allows you to pass a
// custom RestoreSnapshot to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithRestoreSnapshot(b ...bool) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		if len(b) > 0 {
			echovault.config.RestoreSnapshot = b[0]
		} else {
			echovault.config.RestoreSnapshot = true
		}
	}
}

// WithRestoreAOF is an option to the NewEchoVault function that allows you to pass a
// custom RestoreAOF to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithRestoreAOF(b ...bool) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		if len(b) > 0 {
			echovault.config.RestoreAOF = b[0]
		} else {
			echovault.config.RestoreAOF = true
		}
	}
}

// WithAOFSyncStrategy is an option to the NewEchoVault function that allows you to pass a
// custom AOFSyncStrategy to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithAOFSyncStrategy(aOFSyncStrategy string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.AOFSyncStrategy = aOFSyncStrategy
	}
}

// WithMaxMemory is an option to the NewEchoVault function that allows you to pass a
// custom MaxMemory to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithMaxMemory(maxMemory uint64) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.MaxMemory = maxMemory
	}
}

// WithEvictionPolicy is an option to the NewEchoVault function that allows you to pass a
// custom EvictionPolicy to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithEvictionPolicy(evictionPolicy string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.EvictionPolicy = evictionPolicy
	}
}

// WithEvictionSample is an option to the NewEchoVault function that allows you to pass a
// custom EvictionSample to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithEvictionSample(evictionSample uint) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.EvictionSample = evictionSample
	}
}

// WithEvictionInterval is an option to the NewEchoVault function that allows you to pass a
// custom EvictionInterval to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithEvictionInterval(evictionInterval time.Duration) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.EvictionInterval = evictionInterval
	}
}

// WithModules is an option to the NewEchoVault function that allows you to pass a
// custom Modules to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithModules(modules []string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.Modules = modules
	}
}

// WithDiscoveryPort is an option to the NewEchoVault function that allows you to pass a
// custom DiscoveryPort to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithDiscoveryPort(discoveryPort uint16) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.DiscoveryPort = discoveryPort
	}
}

// WithRaftBindAddr is an option to the NewEchoVault function that allows you to pass a
// custom RaftBindAddr to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithRaftBindAddr(raftBindAddr string) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.RaftBindAddr = raftBindAddr
	}
}

// WithRaftBindPort is an option to the NewEchoVault function that allows you to pass a
// custom RaftBindPort to EchoVault.
// If not specified, EchoVault will use the default configuration from config.DefaultConfig().
func WithRaftBindPort(raftBindPort uint16) func(echovault *EchoVault) {
	return func(echovault *EchoVault) {
		echovault.config.RaftBindPort = raftBindPort
	}
}
