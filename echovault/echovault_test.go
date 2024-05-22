package echovault

import (
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/tidwall/resp"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
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

func setupServer(
	serverId string,
	bootstrapCluster bool,
	bindAddr,
	joinAddr string,
	port,
	raftPort,
	mlPort int,
) (*EchoVault, error) {
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

func Test_ClusterReplication(t *testing.T) {
	pairs := make([]ClientServerPair, 3)

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
			t.Errorf("could not get free port: %v", err)
		}
		raftPort, err := internal.GetFreePort()
		if err != nil {
			t.Errorf("could not get free raft port: %v", err)
		}
		memberlistPort, err := internal.GetFreePort()
		if err != nil {
			t.Errorf("could not get free memberlist port: %v", err)
		}
		server, err := setupServer(serverId, bootstrapCluster, bindAddr, joinAddr, port, raftPort, memberlistPort)
		if err != nil {
			t.Errorf("could not start server; %v", err)
		}

		// Start the server
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			wg.Done()
			server.Start()
		}()

		<-time.After(5 * time.Second)

		// Setup client connection
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
		if err != nil {
			t.Errorf("could not open tcp connection: %v", err)
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
		node := pairs[0]
		if err := node.client.WriteArray([]resp.Value{
			resp.StringValue("SET"),
			resp.StringValue(test.key),
			resp.StringValue(test.value),
		}); err != nil {
			t.Errorf("could not write data to leader node (test %d): %v", i, err)
		}
		// Read response and make sure we received "ok" response
		rd, _, err := node.client.ReadValue()
		if err != nil {
			t.Errorf("could not read response from leader node (test %d): %v", i, err)
		}
		if !strings.EqualFold(rd.String(), "ok") {
			t.Errorf("expected response for test %d to be \"OK\", got %s", i, rd.String())
		}
	}

	// On each of the follower nodes, get the values and check if they have been replicated
	for i, test := range tests {
		for j := 1; j < len(pairs); j++ {
			node := pairs[i]
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
			if rd.String() != test.value {
				t.Errorf("exptected value \"%s\" for follower node %d (test %d), got \"%s\"", test.value, j, i, rd.String())
			}
		}
	}
}
