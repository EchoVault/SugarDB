package echovault

import (
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/tidwall/resp"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
)

type ClientServerPair struct {
	serverId         string
	bindAddr         string
	port             int
	raftPort         int
	mlPort           int
	bootstrapCluster bool
	client           *resp.Conn
	server           *EchoVault
}

var bindLock sync.Mutex
var bindNum byte = 10

func getBindAddrNet(network byte) net.IP {
	bindLock.Lock()
	defer bindLock.Unlock()

	result := net.IPv4(127, 0, network, bindNum)
	bindNum++
	if bindNum > 255 {
		bindNum = 10
	}

	return result
}

func getBindAddr() net.IP {
	return getBindAddrNet(0)
}

var setupLock sync.Mutex

func setupServer(
	serverId string,
	bootstrapCluster bool,
	bindAddr,
	joinAddr string,
	port,
	raftPort,
	mlPort int,
) (*EchoVault, error) {
	setupLock.Lock()
	defer setupLock.Unlock()

	config := DefaultConfig()
	config.DataDir = "./testdata"
	config.BindAddr = bindAddr
	config.JoinAddr = joinAddr
	config.Port = uint16(port)
	config.InMemory = true
	config.ServerID = serverId
	config.RaftBindPort = uint16(raftPort)
	config.MemberListBindPort = uint16(mlPort)
	config.BootstrapCluster = bootstrapCluster
	return NewEchoVault(WithConfig(config))
}

func MakeCluster(size int) ([]ClientServerPair, error) {
	pairs := make([]ClientServerPair, size)

	for i := 0; i < len(pairs); i++ {
		serverId := fmt.Sprintf("SERVER-%d", i)
		bindAddr := getBindAddr().String()
		bootstrapCluster := i == 0
		joinAddr := ""
		if !bootstrapCluster {
			joinAddr = fmt.Sprintf("%s/%s:%d", pairs[0].serverId, pairs[0].bindAddr, pairs[0].mlPort)
		}
		port, err := internal.GetFreePort()
		if err != nil {
			return nil, fmt.Errorf("could not get free port: %v", err)
		}
		raftPort, err := internal.GetFreePort()
		if err != nil {
			return nil, fmt.Errorf("could not get free raft port: %v", err)
		}
		memberlistPort, err := internal.GetFreePort()
		if err != nil {
			return nil, fmt.Errorf("could not get free memberlist port: %v", err)
		}
		server, err := setupServer(serverId, bootstrapCluster, bindAddr, joinAddr, port, raftPort, memberlistPort)
		if err != nil {
			return nil, fmt.Errorf("could not start server; %v", err)
		}

		// Start the server
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			wg.Done()
			server.Start()
		}()
		wg.Wait()

		if i == 0 {
			// If node is a leader, wait until it's established itself as a leader of the raft cluster.
			for {
				if server.raft.IsRaftLeader() {
					break
				}
			}
		} else {
			// If the node is a follower, wait until it's joined the raft cluster before moving forward.
			for {
				if server.raft.HasJoinedCluster() {
					break
				}
			}
		}

		// Setup client connection.
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
		if err != nil {
			return nil, fmt.Errorf("could not open tcp connection: %v", err)
		}
		for {
			// Wait until connection is no longer nil
			if conn != nil {
				break
			}
		}
		client := resp.NewConn(conn)

		pairs[i] = ClientServerPair{
			serverId:         serverId,
			bindAddr:         bindAddr,
			port:             port,
			raftPort:         raftPort,
			mlPort:           memberlistPort,
			bootstrapCluster: bootstrapCluster,
			client:           client,
			server:           server,
		}
	}

	return pairs, nil
}

func Test_ClusterReplication(t *testing.T) {
	nodes, err := MakeCluster(5)
	if err != nil {
		t.Error(err)
		return
	}

	// Prepare the write data for the cluster
	tests := []struct {
		key   string
		value string
	}{
		{
			key:   "key1",
			value: "value1",
		},
		{
			key:   "key2",
			value: "value2",
		},
		{
			key:   "key3",
			value: "value3",
		},
	}

	// Write all the data to the cluster leader
	for i, test := range tests {
		node := nodes[0]
		if err := node.client.WriteArray([]resp.Value{
			resp.StringValue("SET"),
			resp.StringValue(test.key),
			resp.StringValue(test.value),
		}); err != nil {
			t.Errorf("could not write data to leader node (test %d): %v", i, err)
		}
		// Read response and make sure we received "ok" response.
		rd, _, err := node.client.ReadValue()
		if err != nil {
			t.Errorf("could not read response from leader node (test %d): %v", i, err)
		}
		if !strings.EqualFold(rd.String(), "ok") {
			t.Errorf("expected response for test %d to be \"OK\", got %s", i, rd.String())
		}
	}

	// Check if the data has been replicated on a quorum (majority of the cluster).
	quorum := int(math.Ceil(float64(len(nodes)/2)) + 1)
	for i, test := range tests {
		count := 0
		for j := 0; j < len(nodes); j++ {
			node := nodes[j]
			if err := node.client.WriteArray([]resp.Value{
				resp.StringValue("GET"),
				resp.StringValue(test.key),
			}); err != nil {
				t.Errorf("could not write data to follower node %d (test %d): %v", j, i, err)
			}
			rd, _, err := node.client.ReadValue()
			if err != nil {
				t.Errorf("could not read data from follower node %d (test %d): %v", j, i, err)
			}
			if rd.String() == test.value {
				count += 1 // If the expected value is found, increment the count.
			}
		}
		// Fail if count is less than quorum.
		if count < quorum {
			t.Errorf("could not find value %s at key %s in cluster quorum", test.value, test.key)
		}
	}
}
