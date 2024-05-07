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
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"net"
	"sync"
	"testing"
	"time"
)

type Node struct {
	name   string
	port   uint16
	server *EchoVault
}

var bindLock sync.Mutex
var bindNum byte = 1

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

func buildReplicationCluster(size int) ([]Node, error) {
	doneChan := make(chan []Node, 1)
	errChan := make(chan error, 1)

	go func() {
		nodes := make([]Node, size)

		for i := 0; i < len(nodes); i++ {
			// Set node name
			nodes[i].name = fmt.Sprintf("Node %d", i+1)

			// Set up port for current node
			port, err := internal.GetFreePort()
			if err != nil {
				errChan <- err
				return
			}
			nodes[i].port = uint16(port)

			// Set up memberlist port
			memberlistPort, err := internal.GetFreePort()
			if err != nil {
				errChan <- err
				return
			}

			// Set up raft port
			raftPort, err := internal.GetFreePort()
			if err != nil {
				errChan <- err
				return
			}

			// If index is > 0, then add a join address to join the cluster
			joinAddress := ""
			if i > 0 {
				joinAddress = fmt.Sprintf("%s:%d", nodes[0].server.config.BindAddr, nodes[0].server.config.MemberListBindPort)
			}

			// Set up echovault instance on node field for current node
			conf := DefaultConfig()
			conf.ServerID = fmt.Sprintf("Server:%d", i)
			conf.BindAddr = getBindAddr().String()
			conf.Port = uint16(port)
			conf.InMemory = true
			conf.BootstrapCluster = i == 0 // Bootstrap cluster if it's the first node
			conf.MemberListBindPort = uint16(memberlistPort)
			conf.RaftBindPort = uint16(raftPort)
			conf.JoinAddr = joinAddress

			server, err := NewEchoVault(WithConfig(conf))
			if err != nil {
				errChan <- err
				return
			}
			nodes[i].server = server

			// Wait until the node has joined the raft cluster
			for {
				if i == 0 {
					// If index is 0, wait until the node is a raft cluster leader
					if server.raft.IsRaftLeader() {
						break
					}
				} else {
					// If index is > 0, wait until the node is a raft follower
					if server.raft.HasJoinedCluster() && !server.raft.IsRaftLeader() {
						break
					}
				}
			}

		}

		doneChan <- nodes
	}()

	select {
	case err := <-errChan:
		return nil, err
	case nodes := <-doneChan:
		return nodes, nil
	case <-time.After(10 * time.Second):
		return nil, errors.New("build cluster timeout")
	}
}

func Test_DataReplication(t *testing.T) {
	cluster, err := buildReplicationCluster(3)
	if err != nil {
		t.Error(err)
		return
	}

	// Set a value on the leader node
	res, err := cluster[0].server.Set("key1", "value1", SetOptions{})
	if err != nil {
		t.Error(err)
	}
	if res != "OK" {
		t.Errorf("expected response \"OK\", got %s", res)
	}

}
