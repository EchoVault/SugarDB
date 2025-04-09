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

package sugardb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestSugarDB_PubSub(t *testing.T) {
	server := createSugarDB()
	t.Cleanup(func() {
		server.ShutDown()
	})

	t.Run("TestSugarDB_(P)Subscribe", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			action      string // subscribe | psubscribe
			tag         string
			channels    []string
			pubChannels []string // Channels to publish messages to after subscriptions
			wantMsg     []string // Expected messages from after publishing
			subFunc     func(tag string, channels ...string) (*MessageReader, error)
			unsubFunc   func(tag string, channels ...string)
		}{
			{
				name:   "1. Subscribe to channels",
				action: "subscribe",
				tag:    "tag_test_subscribe",
				channels: []string{
					"channel1",
					"channel2",
				},
				pubChannels: []string{"channel1", "channel2"},
				wantMsg: []string{
					"message for channel1",
					"message for channel2",
				},
				subFunc:   server.Subscribe,
				unsubFunc: server.Unsubscribe,
			},
			{
				name:   "2. Subscribe to patterns",
				action: "psubscribe",
				tag:    "tag_test_psubscribe",
				channels: []string{
					"channel[34]",
					"pattern[12]",
				},
				pubChannels: []string{
					"channel3",
					"channel4",
					"pattern1",
					"pattern2",
				},
				wantMsg: []string{
					"message for channel3",
					"message for channel4",
					"message for pattern1",
					"message for pattern2",
				},
				subFunc:   server.PSubscribe,
				unsubFunc: server.PUnsubscribe,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				t.Cleanup(func() {
					tt.unsubFunc(tt.tag, tt.channels...)
				})

				// Subscribe to channels.
				readMessage, err := tt.subFunc(tt.tag, tt.channels...)
				if err != nil {
					t.Errorf("(P)SUBSCRIBE() error = %v", err)
				}

				for i := 0; i < len(tt.channels); i++ {
					p := make([]byte, 1024)
					_, err := readMessage.Read(p)
					if err != nil {
						t.Errorf("(P)SUBSCRIBE() read error: %+v", err)
					}
					var message []string
					if err = json.Unmarshal(bytes.TrimRight(p, "\x00"), &message); err != nil {
						t.Errorf("(P)SUBSCRIBE() json unmarshal error: %+v", err)
					}
					// Check that we've received the subscribe messages.
					if message[0] != tt.action {
						t.Errorf("(P)SUBSCRIBE() expected index 0 for message at %d to be \"subscribe\", got %s", i, message[0])
					}
					if !slices.Contains(tt.channels, message[1]) {
						t.Errorf("(P)SUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[1], i)
					}
				}

				// Publish some messages to the channels.
				for _, channel := range tt.pubChannels {
					ok, err := server.Publish(channel, fmt.Sprintf("message for %s", channel))
					if err != nil {
						t.Errorf("PUBLISH() err = %v", err)
					}
					if !ok {
						t.Errorf("PUBLISH() could not publish message to channel %s", channel)
					}
				}

				// Read messages from the channels
				for i := 0; i < len(tt.pubChannels); i++ {
					p := make([]byte, 1024)
					_, err := readMessage.Read(p)

					doneChan := make(chan struct{}, 1)
					go func() {
						for {
							if err != nil && err == io.EOF {
								_, err = readMessage.Read(p)
								continue
							}
							doneChan <- struct{}{}
							break
						}
					}()

					select {
					case <-time.After(500 * time.Millisecond):
						t.Errorf("(P)SUBSCRIBE() timeout")
					case <-doneChan:
						if err != nil {
							t.Errorf("(P)SUBSCRIBE() read error: %+v", err)
						}
					}

					var message []string
					if err = json.Unmarshal(bytes.TrimRight(p, "\x00"), &message); err != nil {
						t.Errorf("(P)SUBSCRIBE() json unmarshal error: %+v", err)
					}
					// Check that we've received the messages.
					if message[0] != "message" {
						t.Errorf("(P)SUBSCRIBE() expected index 0 for message at %d to be \"message\", got %s", i, message[0])
					}
					if !slices.Contains(tt.channels, message[1]) {
						t.Errorf("(P)SUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[1], i)
					}
					if !slices.Contains(tt.wantMsg, message[2]) {
						t.Errorf("(P)SUBSCRIBE() unexpected string \"%s\" at index 2 for message %d", message[1], i)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_PubSubChannels", func(t *testing.T) {
		tests := []struct {
			name     string
			tag      string
			channels []string
			patterns []string
			pattern  string
			want     []string
			wantErr  bool
		}{
			{
				name:     "1. Get number of active channels",
				tag:      "tag_test_channels_1",
				channels: []string{"channel1", "channel2", "channel3", "channel4"},
				patterns: []string{"channel[56]"},
				pattern:  "channel[123456]",
				want:     []string{"channel1", "channel2", "channel3", "channel4"},
				wantErr:  false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				// Subscribe to channels
				_, err := server.Subscribe(tt.tag, tt.channels...)
				if err != nil {
					t.Errorf("PubSubChannels() error = %v", err)
				}

				// Subscribe to patterns
				_, err = server.PSubscribe(tt.tag, tt.patterns...)
				if err != nil {
					t.Errorf("PubSubChannels() error = %v", err)
				}

				got, err := server.PubSubChannels(tt.pattern)
				if (err != nil) != tt.wantErr {
					t.Errorf("PubSubChannels() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if len(got) != len(tt.want) {
					t.Errorf("PubSubChannels() got response length %d, want %d", len(got), len(tt.want))
				}
				for _, item := range got {
					if !slices.Contains(tt.want, item) {
						t.Errorf("PubSubChannels() unexpected item \"%s\", in response", item)
					}
				}
			})
		}
	})

	t.Run("TestSugarDB_PubSubNumPat", func(t *testing.T) {
		tests := []struct {
			name     string
			tag      string
			patterns []string
			want     int
			wantErr  bool
		}{
			{
				name:     "1. Get number of active patterns on the server",
				tag:      "tag_test_numpat_1",
				patterns: []string{"channel[56]", "channel[78]"},
				want:     2,
				wantErr:  false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				// Subscribe to patterns
				_, err := server.PSubscribe(tt.tag, tt.patterns...)
				if err != nil {
					t.Errorf("PubSubNumPat() error = %v", err)
				}

				got, err := server.PubSubNumPat()
				if (err != nil) != tt.wantErr {
					t.Errorf("PubSubNumPat() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("PubSubNumPat() got = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TestSugarDB_PubSubNumSub", func(t *testing.T) {
		tests := []struct {
			name          string
			subscriptions map[string]struct {
				channels []string
				patterns []string
			}
			channels []string
			want     map[string]int
			wantErr  bool
		}{
			{
				name: "1. Get number of subscriptions for the given channels",
				subscriptions: map[string]struct {
					channels []string
					patterns []string
				}{
					"tag1_test_numsub_1": {
						channels: []string{"test_num_sub_channel1", "test_num_sub_channel2"},
						patterns: []string{"test_num_sub_channel[34]"},
					},
					"tag2_test_numsub_2": {
						channels: []string{"test_num_sub_channel2", "test_num_sub_channel3"},
						patterns: []string{"test_num_sub_channel[23]"},
					},
					"tag3_test_numsub_3": {
						channels: []string{"test_num_sub_channel2", "test_num_sub_channel4"},
						patterns: []string{},
					},
				},
				channels: []string{
					"test_num_sub_channel1",
					"test_num_sub_channel2",
					"test_num_sub_channel3",
					"test_num_sub_channel4",
					"test_num_sub_channel5",
				},
				want: map[string]int{
					"test_num_sub_channel1": 1,
					"test_num_sub_channel2": 3,
					"test_num_sub_channel3": 1,
					"test_num_sub_channel4": 1,
					"test_num_sub_channel5": 0,
				},
				wantErr: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				for tag, subs := range tt.subscriptions {
					_, err := server.PSubscribe(tag, subs.patterns...)
					if err != nil {
						t.Errorf("PubSubNumSub() error = %v", err)
					}

					_, err = server.Subscribe(tag, subs.channels...)
					if err != nil {
						t.Errorf("PubSubNumSub() error = %v", err)
					}

				}
				got, err := server.PubSubNumSub(tt.channels...)
				if (err != nil) != tt.wantErr {
					t.Errorf("PubSubNumSub() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("PubSubNumSub() got = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
