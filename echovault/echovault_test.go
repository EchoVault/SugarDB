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
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/tidwall/resp"
	"io"
	"math"
	"net"
	"os"
	"path"
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
	raw              net.Conn
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
	config.ForwardCommand = true
	config.BindAddr = bindAddr
	config.JoinAddr = joinAddr
	config.Port = uint16(port)
	config.InMemory = true
	config.ServerID = serverId
	config.RaftBindPort = uint16(raftPort)
	config.MemberListBindPort = uint16(mlPort)
	config.BootstrapCluster = bootstrapCluster

	return NewEchoVault(
		WithContext(context.Background()),
		WithConfig(config),
	)
}

func makeCluster(size int) ([]ClientServerPair, error) {
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
		client := resp.NewConn(conn)

		pairs[i] = ClientServerPair{
			serverId:         serverId,
			bindAddr:         bindAddr,
			port:             port,
			raftPort:         raftPort,
			mlPort:           memberlistPort,
			bootstrapCluster: bootstrapCluster,
			raw:              conn,
			client:           client,
			server:           server,
		}
	}

	return pairs, nil
}

func Test_Cluster(t *testing.T) {
	nodes, err := makeCluster(5)
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		for _, node := range nodes {
			_ = node.raw.Close()
			node.server.ShutDown()
		}
	}()

	// Prepare the write data for the cluster.
	tests := map[string][]struct {
		key   string
		value string
	}{
		"replication": {
			{key: "key1", value: "value1"},
			{key: "key2", value: "value2"},
			{key: "key3", value: "value3"},
		},
		"deletion": {
			{key: "key4", value: "value4"},
			{key: "key5", value: "value4"},
			{key: "key6", value: "value5"},
		},
		"raft-apply-delete": {
			{key: "key7", value: "value7"},
			{key: "key8", value: "value8"},
			{key: "key9", value: "value9"},
		},
		"forward": {
			{key: "key10", value: "value10"},
			{key: "key11", value: "value11"},
			{key: "key12", value: "value12"},
		},
	}

	t.Run("Test_Replication", func(t *testing.T) {
		tests := tests["replication"]
		// Write all the data to the cluster leader.
		for i, test := range tests {
			node := nodes[0]
			if err := node.client.WriteArray([]resp.Value{
				resp.StringValue("SET"), resp.StringValue(test.key), resp.StringValue(test.value),
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

		<-time.After(200 * time.Millisecond) // Yield

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
	})

	t.Run("Test_DeleteKey", func(t *testing.T) {
		tests := tests["deletion"]
		// Write all the data to the cluster leader.
		for i, test := range tests {
			node := nodes[0]
			_, ok, err := node.server.Set(test.key, test.value, SetOptions{})
			if err != nil {
				t.Errorf("could not write command to leader node (test %d): %v", i, err)
			}
			if !ok {
				t.Errorf("expected set for test %d ok = true, got ok = false", i)
			}
		}

		<-time.After(200 * time.Millisecond) // Yield

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
					t.Errorf("could not write command to follower node %d (test %d): %v", j, i, err)
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
				return
			}
		}

		// Delete the key on the leader node
		// 1. Prepare delete command.
		command := []resp.Value{resp.StringValue("DEL")}
		for _, test := range tests {
			command = append(command, resp.StringValue(test.key))
		}
		// 2. Send delete command.
		if err := nodes[0].client.WriteArray(command); err != nil {
			t.Error(err)
			return
		}
		res, _, err := nodes[0].client.ReadValue()
		if err != nil {
			t.Error(err)
			return
		}
		// 3. Check the delete count is equal to length of tests.
		if res.Integer() != len(tests) {
			t.Errorf("expected delete response to be %d, got %d", len(tests), res.Integer())
		}

		<-time.After(200 * time.Millisecond) // Yield

		// Check if the data is absent in quorum (majority of the cluster).
		for i, test := range tests {
			count := 0
			for j := 0; j < len(nodes); j++ {
				node := nodes[j]
				if err := node.client.WriteArray([]resp.Value{
					resp.StringValue("GET"),
					resp.StringValue(test.key),
				}); err != nil {
					t.Errorf("could not write command to follower node %d (test %d): %v", j, i, err)
				}
				rd, _, err := node.client.ReadValue()
				if err != nil {
					t.Errorf("could not read data from follower node %d (test %d): %v", j, i, err)
				}
				if rd.IsNull() {
					count += 1 // If the expected value is found, increment the count.
				}
			}
			// Fail if count is less than quorum.
			if count < quorum {
				t.Errorf("could not find value %s at key %s in cluster quorum", test.value, test.key)
			}
		}
	})

	t.Run("Test_raftApplyDeleteKey", func(t *testing.T) {
		tests := tests["raft-apply-delete"]
		// Write all the data to the cluster leader.
		for i, test := range tests {
			node := nodes[0]
			_, ok, err := node.server.Set(test.key, test.value, SetOptions{})
			if err != nil {
				t.Errorf("could not write command to leader node (test %d): %v", i, err)
			}
			if !ok {
				t.Errorf("expected set for test %d ok = true, got ok = false", i)
			}
		}

		<-time.After(200 * time.Millisecond) // Yield

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
					t.Errorf("could not write command to follower node %d (test %d): %v", j, i, err)
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
				return
			}
		}

		// Delete the keys using raftApplyDelete method.
		for _, test := range tests {
			if err := nodes[0].server.raftApplyDeleteKey(nodes[0].server.context, test.key); err != nil {
				t.Error(err)
			}
		}

		<-time.After(200 * time.Millisecond) // Yield to give key deletion time to take effect across cluster.

		// Check if the data is absent in quorum (majority of the cluster).
		for i, test := range tests {
			count := 0
			for j := 0; j < len(nodes); j++ {
				node := nodes[j]
				if err := node.client.WriteArray([]resp.Value{
					resp.StringValue("GET"),
					resp.StringValue(test.key),
				}); err != nil {
					t.Errorf("could not write command to follower node %d (test %d): %v", j, i, err)
				}
				rd, _, err := node.client.ReadValue()
				if err != nil {
					t.Errorf("could not read data from follower node %d (test %d): %v", j, i, err)
				}
				if rd.IsNull() {
					count += 1 // If the expected value is found, increment the count.
				}
			}
			// Fail if count is less than quorum.
			if count < quorum {
				t.Errorf("found value %s at key %s in cluster quorum", test.value, test.key)
			}
		}
	})

	// t.Run("Test_ForwardCommand", func(t *testing.T) {
	// 	tests := tests["forward"]
	// 	// Write all the data a random cluster follower.
	// 	for i, test := range tests {
	// 		// Send write command to follower node.
	// 		node := nodes[1]
	// 		if err := node.client.WriteArray([]resp.Value{
	// 			resp.StringValue("SET"),
	// 			resp.StringValue(test.key),
	// 			resp.StringValue(test.value),
	// 		}); err != nil {
	// 			t.Errorf("could not write data to follower node (test %d): %v", i, err)
	// 		}
	// 		// Read response and make sure we received "ok" response.
	// 		rd, _, err := node.client.ReadValue()
	// 		if err != nil {
	// 			t.Errorf("could not read response from follower node (test %d): %v", i, err)
	// 		}
	// 		if !strings.EqualFold(rd.String(), "ok") {
	// 			t.Errorf("expected response for test %d to be \"OK\", got %s", i, rd.String())
	// 		}
	// 	}
	//
	// 	<-time.After(200 * time.Millisecond) // Short yield to allow change to take effect.
	//
	// 	// Check if the data has been replicated on a quorum (majority of the cluster).
	// 	quorum := int(math.Ceil(float64(len(nodes)/2)) + 1)
	// 	for i, test := range tests {
	// 		count := 0
	// 		for j := 0; j < len(nodes); j++ {
	// 			node := nodes[j]
	// 			if err := node.client.WriteArray([]resp.Value{
	// 				resp.StringValue("GET"),
	// 				resp.StringValue(test.key),
	// 			}); err != nil {
	// 				t.Errorf("could not write data to follower node %d (test %d): %v", j, i, err)
	// 			}
	// 			rd, _, err := node.client.ReadValue()
	// 			if err != nil {
	// 				t.Errorf("could not read data from follower node %d (test %d): %v", j, i, err)
	// 			}
	// 			if rd.String() == test.value {
	// 				count += 1 // If the expected value is found, increment the count.
	// 			}
	// 		}
	// 		// Fail if count is less than quorum.
	// 		if count < quorum {
	// 			t.Errorf("could not find value %s at key %s in cluster quorum", test.value, test.key)
	// 		}
	// 	}
	// })
}

func Test_TLS(t *testing.T) {
	port, err := internal.GetFreePort()
	if err != nil {
		t.Error(err)
	}

	conf := DefaultConfig()
	conf.DataDir = ""
	conf.BindAddr = "localhost"
	conf.Port = uint16(port)
	conf.TLS = true
	conf.CertKeyPairs = [][]string{
		{
			path.Join("..", "openssl", "server", "server1.crt"),
			path.Join("..", "openssl", "server", "server1.key"),
		},
		{
			path.Join("..", "openssl", "server", "server2.crt"),
			path.Join("..", "openssl", "server", "server2.key"),
		},
	}

	server, err := NewEchoVault(WithConfig(conf))
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		server.Start()
	}()
	wg.Wait()

	// Dial with ServerCAs
	serverCAs := x509.NewCertPool()
	f, err := os.Open(path.Join("..", "openssl", "server", "rootCA.crt"))
	if err != nil {
		t.Error(err)
	}
	cert, err := io.ReadAll(bufio.NewReader(f))
	if err != nil {
		t.Error(err)
	}
	ok := serverCAs.AppendCertsFromPEM(cert)
	if !ok {
		t.Error("could not load server CA")
	}

	conn, err := tls.Dial("tcp", fmt.Sprintf("localhost:%d", port), &tls.Config{
		RootCAs: serverCAs,
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_ = conn.Close()
		server.ShutDown()
	}()
	client := resp.NewConn(conn)

	// Test that we can set and get a value from the server.
	key := "key1"
	value := "value1"
	err = client.WriteArray([]resp.Value{
		resp.StringValue("SET"), resp.StringValue(key), resp.StringValue(value),
	})
	if err != nil {
		t.Error(err)
	}

	res, _, err := client.ReadValue()
	if err != nil {
		t.Error(err)
	}

	if !strings.EqualFold(res.String(), "ok") {
		t.Errorf("expected response OK, got \"%s\"", res.String())
	}

	err = client.WriteArray([]resp.Value{resp.StringValue("GET"), resp.StringValue(key)})
	if err != nil {
		t.Error(err)
	}

	res, _, err = client.ReadValue()
	if err != nil {
		t.Error(err)
	}

	if res.String() != value {
		t.Errorf("expected response at key \"%s\" to be \"%s\", got \"%s\"", key, value, res.String())
	}
}

func Test_MTLS(t *testing.T) {
	port, err := internal.GetFreePort()
	if err != nil {
		t.Error(err)
	}

	conf := DefaultConfig()
	conf.DataDir = ""
	conf.BindAddr = "localhost"
	conf.Port = uint16(port)
	conf.TLS = true
	conf.MTLS = true
	conf.ClientCAs = []string{
		path.Join("..", "openssl", "client", "rootCA.crt"),
	}
	conf.CertKeyPairs = [][]string{
		{
			path.Join("..", "openssl", "server", "server1.crt"),
			path.Join("..", "openssl", "server", "server1.key"),
		},
		{
			path.Join("..", "openssl", "server", "server2.crt"),
			path.Join("..", "openssl", "server", "server2.key"),
		},
	}

	server, err := NewEchoVault(WithConfig(conf))
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		server.Start()
	}()
	wg.Wait()

	// Dial with ServerCAs and client certificates
	clientCertKeyPairs := [][]string{
		{
			path.Join("..", "openssl", "client", "client1.crt"),
			path.Join("..", "openssl", "client", "client1.key"),
		},
		{
			path.Join("..", "openssl", "client", "client2.crt"),
			path.Join("..", "openssl", "client", "client2.key"),
		},
	}
	var certificates []tls.Certificate
	for _, pair := range clientCertKeyPairs {
		c, err := tls.LoadX509KeyPair(pair[0], pair[1])
		if err != nil {
			t.Error(err)
		}
		certificates = append(certificates, c)
	}

	serverCAs := x509.NewCertPool()
	f, err := os.Open(path.Join("..", "openssl", "server", "rootCA.crt"))
	if err != nil {
		t.Error(err)
	}
	cert, err := io.ReadAll(bufio.NewReader(f))
	if err != nil {
		t.Error(err)
	}
	ok := serverCAs.AppendCertsFromPEM(cert)
	if !ok {
		t.Error("could not load server CA")
	}

	conn, err := tls.Dial("tcp", fmt.Sprintf("localhost:%d", port), &tls.Config{
		RootCAs:      serverCAs,
		Certificates: certificates,
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_ = conn.Close()
		server.ShutDown()
	}()
	client := resp.NewConn(conn)

	// Test that we can set and get a value from the server.
	key := "key1"
	value := "value1"
	err = client.WriteArray([]resp.Value{
		resp.StringValue("SET"), resp.StringValue(key), resp.StringValue(value),
	})
	if err != nil {
		t.Error(err)
	}

	res, _, err := client.ReadValue()
	if err != nil {
		t.Error(err)
	}

	if !strings.EqualFold(res.String(), "ok") {
		t.Errorf("expected response OK, got \"%s\"", res.String())
	}

	err = client.WriteArray([]resp.Value{resp.StringValue("GET"), resp.StringValue(key)})
	if err != nil {
		t.Error(err)
	}

	res, _, err = client.ReadValue()
	if err != nil {
		t.Error(err)
	}

	if res.String() != value {
		t.Errorf("expected response at key \"%s\" to be \"%s\", got \"%s\"", key, value, res.String())
	}
}
