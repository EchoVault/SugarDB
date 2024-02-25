package utils

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	TLS                bool          `json:"TLS" yaml:"TLS"`
	MTLS               bool          `json:"MTLS" yaml:"MTLS"`
	CertKeyPairs       [][]string    `json:"CertKeyPairs" yaml:"CertKeyPairs"`
	ClientCAs          []string      `json:"ClientCAs" yaml:"ClientCAs"`
	Port               uint16        `json:"Port" yaml:"Port"`
	PluginDir          string        `json:"Plugins" yaml:"Plugins"`
	ServerID           string        `json:"ServerId" yaml:"ServerId"`
	JoinAddr           string        `json:"JoinAddr" yaml:"JoinAddr"`
	BindAddr           string        `json:"BindAddr" yaml:"BindAddr"`
	RaftBindPort       uint16        `json:"RaftPort" yaml:"RaftPort"`
	MemberListBindPort uint16        `json:"MlPort" yaml:"MlPort"`
	InMemory           bool          `json:"InMemory" yaml:"InMemory"`
	DataDir            string        `json:"DataDir" yaml:"DataDir"`
	BootstrapCluster   bool          `json:"BootstrapCluster" yaml:"BootstrapCluster"`
	AclConfig          string        `json:"AclConfig" yaml:"AclConfig"`
	ForwardCommand     bool          `json:"ForwardCommand" yaml:"ForwardCommand"`
	RequirePass        bool          `json:"RequirePass" yaml:"RequirePass"`
	Password           string        `json:"Password" yaml:"Password"`
	SnapShotThreshold  uint64        `json:"SnapshotThreshold" yaml:"SnapshotThreshold"`
	SnapshotInterval   time.Duration `json:"SnapshotInterval" yaml:"SnapshotInterval"`
	RestoreSnapshot    bool          `json:"RestoreSnapshot" yaml:"RestoreSnapshot"`
	RestoreAOF         bool          `json:"RestoreAOF" yaml:"RestoreAOF"`
	AOFSyncStrategy    string        `json:"AOFSyncStrategy" yaml:"AOFSyncStrategy"`
}

func GetConfig() (Config, error) {
	var certKeyPairs [][]string
	var clientCAs []string

	flag.Func("certKeyPair",
		"A pair of file paths representing the signed certificate and it's corresponding key separated by a comma.",
		func(s string) error {
			pair := strings.Split(strings.TrimSpace(s), ",")
			for i := 0; i < len(pair); i++ {
				pair[i] = strings.TrimSpace(pair[i])
			}
			if len(pair) != 2 {
				return errors.New("certKeyPair must be 2 comma separated strings")
			}
			certKeyPairs = append(certKeyPairs, pair)
			return nil
		})

	flag.Func("clientCA", "Path to certificate authority used to verify client certificates.", func(s string) error {
		clientCAs = append(clientCAs, s)
		return nil
	})

	var aofSyncStrategy string = "everysec"
	flag.Func("aofSyncStrategy", `How often to flush the file contents written to append only file.
The options are 'always' for syncing on each command, 'everysec' to sync every second, and 'no' to leave it up to the os.`,
		func(option string) error {
			if !slices.ContainsFunc([]string{"always", "everysec", "no"}, func(s string) bool {
				return strings.EqualFold(s, option)
			}) {
				return errors.New("aofSyncStrategy must be 'always', 'everysec' or 'no'")
			}
			aofSyncStrategy = strings.ToLower(option)
			return nil
		})

	tls := flag.Bool("tls", false, "Start the server in TLS mode. Default is false")
	mtls := flag.Bool("mtls", false, "Use mTLS to verify the client.")
	port := flag.Int("port", 7480, "Port to use. Default is 7480")
	pluginDir := flag.String("pluginDir", "", "Directory where plugins are located.")
	serverId := flag.String("serverId", "1", "Server ID in raft cluster. Leave empty for client.")
	joinAddr := flag.String("joinAddr", "", "Address of cluster member in a cluster to you want to join.")
	bindAddr := flag.String("bindAddr", "", "Address to bind the server to.")
	raftBindPort := flag.Uint("raftPort", 7481, "Port to use for intra-cluster communication. Leave on the client.")
	mlBindPort := flag.Uint("mlPort", 7946, "Port to use for memberlist communication.")
	inMemory := flag.Bool("inMemory", false, "Whether to use memory or persistent storage for raft logs and snapshots.")
	dataDir := flag.String("dataDir", "/var/lib/memstore", "Directory to store raft snapshots and logs.")
	bootstrapCluster := flag.Bool("bootstrapCluster", false, "Whether this instance should bootstrap a new cluster.")
	aclConfig := flag.String("aclConfig", "", "ACL config file path.")
	snapshotThreshold := flag.Uint64("snapshotThreshold", 1000, "The number of entries that trigger a snapshot. Default is 1000.")
	snapshotInterval := flag.Duration("snapshotInterval", 5*time.Minute, "The time interval between snapshots (in seconds). Default is 5 minutes.")
	restoreSnapshot := flag.Bool("restoreSnapshot", false, "This flag prompts the server to restore state from snapshot when set to true. Only works in standalone mode. Higher priority than restoreAOF.")
	restoreAOF := flag.Bool("restoreAOF", false, "This flag prompts the server to restore state from append-only logs. Only works in standalone mode. Lower priority than restoreSnapshot.")
	forwardCommand := flag.Bool(
		"forwardCommand",
		false,
		"If the node is a follower, this flag forwards mutation command to the leader when set to true")
	requirePass := flag.Bool(
		"requirePass",
		false,
		"Whether the server should require a password before allowing commands. Default is false.",
	)
	password := flag.String(
		"password",
		"",
		`The password for the default user. ACL config file will overwrite this value. 
It is a plain text value by default but you can provide a SHA256 hash by adding a '#' before the hash.`,
	)

	config := flag.String(
		"config",
		"",
		`File path to a JSON or YAML config file.The values in this config file will override the flag values.`,
	)

	flag.Parse()

	conf := Config{
		CertKeyPairs:       certKeyPairs,
		ClientCAs:          clientCAs,
		TLS:                *tls,
		MTLS:               *mtls,
		PluginDir:          *pluginDir,
		Port:               uint16(*port),
		ServerID:           *serverId,
		JoinAddr:           *joinAddr,
		BindAddr:           *bindAddr,
		RaftBindPort:       uint16(*raftBindPort),
		MemberListBindPort: uint16(*mlBindPort),
		InMemory:           *inMemory,
		DataDir:            *dataDir,
		BootstrapCluster:   *bootstrapCluster,
		AclConfig:          *aclConfig,
		ForwardCommand:     *forwardCommand,
		RequirePass:        *requirePass,
		Password:           *password,
		SnapShotThreshold:  *snapshotThreshold,
		SnapshotInterval:   *snapshotInterval,
		RestoreSnapshot:    *restoreSnapshot,
		RestoreAOF:         *restoreAOF,
		AOFSyncStrategy:    aofSyncStrategy,
	}

	if len(*config) > 0 {
		// Override configurations from file
		if f, err := os.Open(*config); err != nil {
			panic(err)
		} else {
			defer func() {
				if err = f.Close(); err != nil {
					log.Println(err)
				}
			}()

			ext := path.Ext(f.Name())

			if ext == ".json" {
				err := json.NewDecoder(f).Decode(&conf)
				if err != nil {
					return Config{}, nil
				}
			}

			if ext == ".yaml" || ext == ".yml" {
				err := yaml.NewDecoder(f).Decode(&conf)
				if err != nil {
					return Config{}, err
				}
			}
		}

	}

	// If requirePass is set to true, then password must be provided as well
	var err error = nil

	if conf.RequirePass && conf.Password == "" {
		err = errors.New("password cannot be empty if requirePass is etc to true")
	}

	return conf, err
}
