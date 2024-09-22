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
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func Test_Subscribe(t *testing.T) {
	server := createSugarDB()

	// Subscribe to channels.
	tag := "tag"
	channels := []string{"channel1", "channel2"}
	readMessage, err := server.Subscribe(tag, channels...)
	if err != nil {
		t.Errorf("SUBSCRIBE() error = %v", err)
	}

	for i := 0; i < len(channels); i++ {
		message := readMessage()
		// Check that we've received the subscribe messages.
		if message[0] != "subscribe" {
			t.Errorf("SUBSCRIBE() expected index 0 for message at %d to be \"subscribe\", got %s", i, message[0])
		}
		if !slices.Contains(channels, message[1]) {
			t.Errorf("SUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[1], i)
		}
	}

	// Publish some messages to the channels.
	for _, channel := range channels {
		ok, err := server.Publish(channel, fmt.Sprintf("message for %s", channel))
		if err != nil {
			t.Errorf("PUBLISH() err = %v", err)
		}
		if !ok {
			t.Errorf("PUBLISH() could not publish message to channel %s", channel)
		}
	}

	// Read messages from the channels
	for i := 0; i < len(channels); i++ {
		message := readMessage()
		// Check that we've received the messages.
		if message[0] != "message" {
			t.Errorf("SUBSCRIBE() expected index 0 for message at %d to be \"message\", got %s", i, message[0])
		}
		if !slices.Contains(channels, message[1]) {
			t.Errorf("SUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[1], i)
		}
		if !slices.Contains([]string{"message for channel1", "message for channel2"}, message[2]) {
			t.Errorf("SUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[1], i)
		}
	}

	// Unsubscribe from channels
	server.Unsubscribe(tag, channels...)
}

func TestSugarDB_PSubscribe(t *testing.T) {
	server := createSugarDB()

	// Subscribe to channels.
	tag := "tag"
	patterns := []string{"channel[12]", "pattern[12]"}
	readMessage, err := server.PSubscribe(tag, patterns...)
	if err != nil {
		t.Errorf("PSubscribe() error = %v", err)
	}

	for i := 0; i < len(patterns); i++ {
		message := readMessage()
		// Check that we've received the subscribe messages.
		if message[0] != "psubscribe" {
			t.Errorf("PSUBSCRIBE() expected index 0 for message at %d to be \"psubscribe\", got %s", i, message[0])
		}
		if !slices.Contains(patterns, message[1]) {
			t.Errorf("PSUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[1], i)
		}
	}

	// Publish some messages to the channels.
	for _, channel := range []string{"channel1", "channel2", "pattern1", "pattern2"} {
		ok, err := server.Publish(channel, fmt.Sprintf("message for %s", channel))
		if err != nil {
			t.Errorf("PUBLISH() err = %v", err)
		}
		if !ok {
			t.Errorf("PUBLISH() could not publish message to channel %s", channel)
		}
	}

	// Read messages from the channels
	for i := 0; i < len(patterns)*2; i++ {
		message := readMessage()
		// Check that we've received the messages.
		if message[0] != "message" {
			t.Errorf("SUBSCRIBE() expected index 0 for message at %d to be \"message\", got %s", i, message[0])
		}
		if !slices.Contains(patterns, message[1]) {
			t.Errorf("SUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[1], i)
		}
		if !slices.Contains([]string{
			"message for channel1", "message for channel2", "message for pattern1", "message for pattern2"}, message[2]) {
			t.Errorf("SUBSCRIBE() unexpected string \"%s\" at index 1 for message %d", message[2], i)
		}
	}

	// Unsubscribe from channels
	server.PUnsubscribe(tag, patterns...)
}

func TestSugarDB_PubSubChannels(t *testing.T) {
	server := createSugarDB()
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
			tag:      "tag",
			channels: []string{"channel1", "channel2", "channel3", "channel4"},
			patterns: []string{"channel[56]"},
			pattern:  "channel[123456]",
			want:     []string{"channel1", "channel2", "channel3", "channel4"},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Subscribe to channels
			readChannelMessages, err := server.Subscribe(tt.tag, tt.channels...)
			if err != nil {
				t.Errorf("PubSubChannels() error = %v", err)
			}

			for i := 0; i < len(tt.channels); i++ {
				readChannelMessages()
			}
			// Subscribe to patterns
			readPatternMessages, err := server.PSubscribe(tt.tag, tt.patterns...)
			if err != nil {
				t.Errorf("PubSubChannels() error = %v", err)
			}

			for i := 0; i < len(tt.patterns); i++ {
				readPatternMessages()
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
}

func TestSugarDB_PubSubNumPat(t *testing.T) {
	server := createSugarDB()
	tests := []struct {
		name     string
		tag      string
		patterns []string
		want     int
		wantErr  bool
	}{
		{
			name:     "1. Get number of active patterns on the server",
			tag:      "tag",
			patterns: []string{"channel[56]", "channel[78]"},
			want:     2,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Subscribe to patterns
			readPatternMessages, err := server.PSubscribe(tt.tag, tt.patterns...)
			if err != nil {
				t.Errorf("PubSubNumPat() error = %v", err)
			}
			for i := 0; i < len(tt.patterns); i++ {
				readPatternMessages()
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
}

func TestSugarDB_PubSubNumSub(t *testing.T) {
	server := createSugarDB()
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
			name: "Get number of subscriptions for the given channels",
			subscriptions: map[string]struct {
				channels []string
				patterns []string
			}{
				"tag1": {
					channels: []string{"channel1", "channel2"},
					patterns: []string{"channel[34]"},
				},
				"tag2": {
					channels: []string{"channel2", "channel3"},
					patterns: []string{"channel[23]"},
				},
				"tag3": {
					channels: []string{"channel2", "channel4"},
					patterns: []string{},
				},
			},
			channels: []string{"channel1", "channel2", "channel3", "channel4", "channel5"},
			want:     map[string]int{"channel1": 1, "channel2": 3, "channel3": 1, "channel4": 1, "channel5": 0},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for tag, subs := range tt.subscriptions {
				readPat, err := server.PSubscribe(tag, subs.patterns...)
				if err != nil {
					t.Errorf("PubSubNumSub() error = %v", err)
				}
				for _, _ = range subs.patterns {
					readPat()
				}
				readChan, err := server.Subscribe(tag, subs.channels...)
				if err != nil {
					t.Errorf("PubSubNumSub() error = %v", err)
				}
				for _, _ = range subs.channels {
					readChan()
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
}
