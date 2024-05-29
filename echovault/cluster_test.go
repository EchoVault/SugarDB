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

// func Test_raftApplyDeleteKey(t *testing.T) {
// 	nodes, err := makeCluster(5)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
//
// 	// Prepare the write data for the cluster.
// 	tests := []struct {
// 		key   string
// 		value string
// 	}{
// 		{
// 			key:   "key1",
// 			value: "value1",
// 		},
// 		{
// 			key:   "key2",
// 			value: "value2",
// 		},
// 		{
// 			key:   "key3",
// 			value: "value3",
// 		},
// 	}
//
// 	// Write all the data to the cluster leader.
// 	for i, test := range tests {
// 		node := nodes[0]
// 		if err := node.client.WriteArray([]resp.Value{
// 			resp.StringValue("SET"),
// 			resp.StringValue(test.key),
// 			resp.StringValue(test.value),
// 		}); err != nil {
// 			t.Errorf("could not write data to leader node (test %d): %v", i, err)
// 		}
// 		// Read response and make sure we received "ok" response.
// 		rd, _, err := node.client.ReadValue()
// 		if err != nil {
// 			t.Errorf("could not read response from leader node (test %d): %v", i, err)
// 		}
// 		if !strings.EqualFold(rd.String(), "ok") {
// 			t.Errorf("expected response for test %d to be \"OK\", got %s", i, rd.String())
// 		}
// 	}
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
// 		// Delete key across raft cluster.
// 		if err = nodes[0].server.raftApplyDeleteKey(nodes[0].server.context, test.key); err != nil {
// 			t.Error(err)
// 		}
// 	}
//
// 	<-time.After(200 * time.Millisecond) // Yield to give key deletion time to take effect across cluster.
//
// 	// Check if the data is absent in quorum (majority of the cluster).
// 	for i, test := range tests {
// 		count := 0
// 		for j := 0; j < len(nodes); j++ {
// 			node := nodes[j]
// 			if err := node.client.WriteArray([]resp.Value{
// 				resp.StringValue("GET"),
// 				resp.StringValue(test.key),
// 			}); err != nil {
// 				t.Errorf("could not write command to follower node %d (test %d): %v", j, i, err)
// 			}
// 			rd, _, err := node.client.ReadValue()
// 			if err != nil {
// 				t.Errorf("could not read data from follower node %d (test %d): %v", j, i, err)
// 			}
// 			if rd.IsNull() {
// 				count += 1 // If the expected value is found, increment the count.
// 			}
// 		}
// 		// Fail if count is less than quorum.
// 		if count < quorum {
// 			t.Errorf("found value %s at key %s in cluster quorum", test.value, test.key)
// 		}
// 	}
// }
