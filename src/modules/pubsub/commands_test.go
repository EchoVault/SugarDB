package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"net"
	"slices"
	"testing"
	"time"
)

var pubsub *PubSub
var mockServer *server.Server

var bindAddr = "localhost"
var port uint16 = 7490

func init() {
	pubsub = NewPubSub()
	mockServer = server.NewServer(server.Opts{
		PubSub:   pubsub,
		Commands: Commands(),
		Config: utils.Config{
			BindAddr:       bindAddr,
			Port:           port,
			DataDir:        "",
			EvictionPolicy: utils.NoEviction,
		},
	})
	go func() {
		mockServer.Start(context.Background())
	}()
}

func Test_HandleSubscribe(t *testing.T) {
	ctx := context.WithValue(context.Background(), "test_name", "SUBSCRIBE/PSUBSCRIBE")

	numOfConnection := 20
	connections := make([]*net.Conn, numOfConnection)

	for i := 0; i < numOfConnection; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
		if err != nil {
			t.Error(err)
		}
		connections[i] = &conn
	}
	defer func() {
		for _, conn := range connections {
			if err := (*conn).Close(); err != nil {
				t.Error(err)
			}
		}
	}()

	// Test subscribe to channels
	channels := []string{"sub_channel1", "sub_channel2", "sub_channel3"}
	for _, conn := range connections {
		if _, err := handleSubscribe(ctx, append([]string{"SUBSCRIBE"}, channels...), mockServer, conn); err != nil {
			t.Error(err)
		}
	}
	for _, channel := range channels {
		// Check if the channel exists in the pubsub module
		if !slices.ContainsFunc(pubsub.channels, func(c *Channel) bool {
			return c.name == channel
		}) {
			t.Errorf("expected pubsub to contain channel \"%s\" but it was not found", channel)
		}
		for _, c := range pubsub.channels {
			if c.name == channel {
				// Check if channel has nil pattern
				if c.pattern != nil {
					t.Errorf("expected channel \"%s\" to have nil pattern, found pattern \"%s\"", channel, c.name)
				}
				// Check if the channel has all the connections from above
				for _, conn := range connections {
					if _, ok := c.subscribers[conn]; !ok {
						t.Errorf("could not find all expected connection in the \"%s\"", channel)
					}
				}
			}
		}
	}

	// Test subscribe to patterns
	patterns := []string{"psub_channel*"}
	for _, conn := range connections {
		if _, err := handleSubscribe(ctx, append([]string{"PSUBSCRIBE"}, patterns...), mockServer, conn); err != nil {
			t.Error(err)
		}
	}
	for _, pattern := range patterns {
		// Check if pattern channel exists in pubsub module
		if !slices.ContainsFunc(pubsub.channels, func(c *Channel) bool {
			return c.name == pattern
		}) {
			t.Errorf("expected pubsub to contain pattern channel \"%s\" but it was not found", pattern)
		}
		for _, c := range pubsub.channels {
			if c.name == pattern {
				// Check if channel has non-nil pattern
				if c.pattern == nil {
					t.Errorf("expected channel \"%s\" to have pattern \"%s\", found nil pattern", pattern, c.name)
				}
				// Check if the channel has all the connections from above
				for _, conn := range connections {
					if _, ok := c.subscribers[conn]; !ok {
						t.Errorf("could not find all expected connection in the \"%s\"", pattern)
					}
				}
			}
		}
	}
}

func Test_HandleUnsubscribe(t *testing.T) {
	generateConnections := func(noOfConnections int) []*net.Conn {
		connections := make([]*net.Conn, noOfConnections)
		for i := 0; i < noOfConnections; i++ {
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
			if err != nil {
				t.Error(err)
			}
			connections[i] = &conn
		}
		return connections
	}

	closeConnections := func(conns []*net.Conn) {
		for _, conn := range conns {
			if err := (*conn).Close(); err != nil {
				t.Error(err)
			}
		}
	}

	verifyResponse := func(res []byte, expectedResponse [][]string) {
		rd := resp.NewReader(bytes.NewReader(res))
		rv, _, err := rd.ReadValue()
		if err != nil {
			t.Error(err)
		}
		v := rv.Array()
		if len(v) != len(expectedResponse) {
			fmt.Println(v)
			t.Errorf("expected subscribe response of length %d, but got %d", len(expectedResponse), len(v))
		}
		for _, item := range v {
			arr := item.Array()
			if len(arr) != 3 {
				t.Errorf("expected subscribe response item to be length %d, but got %d", 3, len(arr))
			}
			if !slices.ContainsFunc(expectedResponse, func(strings []string) bool {
				return strings[0] == arr[0].String() && strings[1] == arr[1].String() && strings[2] == arr[2].String()
			}) {
				t.Errorf("expected to find item \"%s\" in response, did not find it.", arr[1].String())
			}
		}
	}

	tests := []struct {
		subChannels       []string              // All channels to subscribe to
		subPatterns       []string              // All patterns to subscribe to
		unSubChannels     []string              // Channels to unsubscribe from
		unSubPatterns     []string              // Patterns to unsubscribe from
		remainChannels    []string              // Channels to remain subscribed to
		remainPatterns    []string              // Patterns to remain subscribed to
		targetConn        *net.Conn             // Connection used to test unsubscribe functionality
		otherConnections  []*net.Conn           // Connections to fill the subscribers list for channels and patterns
		expectedResponses map[string][][]string // The expected response from the handler
	}{
		{ // 1. Unsubscribe from channels and patterns
			subChannels:      []string{"xx_channel_one", "xx_channel_two", "xx_channel_three", "xx_channel_four"},
			subPatterns:      []string{"xx_pattern_[ab]", "xx_pattern_[cd]", "xx_pattern_[ef]", "xx_pattern_[gh]"},
			unSubChannels:    []string{"xx_channel_one", "xx_channel_two"},
			unSubPatterns:    []string{"xx_pattern_[ab]"},
			remainChannels:   []string{"xx_channel_three", "xx_channel_four"},
			remainPatterns:   []string{"xx_pattern_[cd]", "xx_pattern_[ef]", "xx_pattern_[gh]"},
			targetConn:       generateConnections(1)[0],
			otherConnections: generateConnections(20),
			expectedResponses: map[string][][]string{
				"channel": {
					{"unsubscribe", "xx_channel_one", "1"},
					{"unsubscribe", "xx_channel_two", "2"},
				},
				"pattern": {
					{"punsubscribe", "xx_pattern_[ab]", "1"},
				},
			},
		},
		{ // 2. Unsubscribe from all channels no channel or pattern is passed to command
			subChannels:      []string{"xx_channel_one", "xx_channel_two", "xx_channel_three", "xx_channel_four"},
			subPatterns:      []string{"xx_pattern_[ab]", "xx_pattern_[cd]", "xx_pattern_[ef]", "xx_pattern_[gh]"},
			unSubChannels:    []string{},
			unSubPatterns:    []string{},
			remainChannels:   []string{},
			remainPatterns:   []string{},
			targetConn:       generateConnections(1)[0],
			otherConnections: generateConnections(20),
			expectedResponses: map[string][][]string{
				"channel": {
					{"unsubscribe", "xx_channel_one", "1"},
					{"unsubscribe", "xx_channel_two", "2"},
					{"unsubscribe", "xx_channel_three", "3"},
					{"unsubscribe", "xx_channel_four", "4"},
				},
				"pattern": {
					{"punsubscribe", "xx_pattern_[ab]", "1"},
					{"punsubscribe", "xx_pattern_[cd]", "2"},
					{"punsubscribe", "xx_pattern_[ef]", "3"},
					{"punsubscribe", "xx_pattern_[gh]", "4"},
				},
			},
		},
		{ // 3. Don't unsubscribe from any channels or patterns if the provided ones are non-existent
			subChannels:      []string{"xx_channel_one", "xx_channel_two", "xx_channel_three", "xx_channel_four"},
			subPatterns:      []string{"xx_pattern_[ab]", "xx_pattern_[cd]", "xx_pattern_[ef]", "xx_pattern_[gh]"},
			unSubChannels:    []string{"xx_channel_non_existent_channel"},
			unSubPatterns:    []string{"xx_channel_non_existent_pattern_[ae]"},
			remainChannels:   []string{"xx_channel_one", "xx_channel_two", "xx_channel_three", "xx_channel_four"},
			remainPatterns:   []string{"xx_pattern_[ab]", "xx_pattern_[cd]", "xx_pattern_[ef]", "xx_pattern_[gh]"},
			targetConn:       generateConnections(1)[0],
			otherConnections: generateConnections(20),
			expectedResponses: map[string][][]string{
				"channel": {},
				"pattern": {},
			},
		},
	}

	for i, test := range tests {
		ctx := context.WithValue(context.Background(), "test_name", fmt.Sprintf("UNSUBSCRIBE/PUNSUBSCRIBE, %d", i))

		// Subscribe all the connections to the channels and patterns
		for _, conn := range append(test.otherConnections, test.targetConn) {
			_, err := handleSubscribe(ctx, append([]string{"SUBSCRIBE"}, test.subChannels...), mockServer, conn)
			if err != nil {
				t.Error(err)
			}
			_, err = handleSubscribe(ctx, append([]string{"PSUBSCRIBE"}, test.subPatterns...), mockServer, conn)
			if err != nil {
				t.Error(err)
			}
		}

		// Unsubscribe the target connection from the unsub channels and patterns
		res, err := handleUnsubscribe(ctx, append([]string{"UNSUBSCRIBE"}, test.unSubChannels...), mockServer, test.targetConn)
		if err != nil {
			t.Error(err)
		}
		verifyResponse(res, test.expectedResponses["channel"])

		res, err = handleUnsubscribe(ctx, append([]string{"PUNSUBSCRIBE"}, test.unSubPatterns...), mockServer, test.targetConn)
		if err != nil {
			t.Error(err)
		}
		verifyResponse(res, test.expectedResponses["pattern"])

		for _, channel := range append(test.unSubChannels, test.unSubPatterns...) {
			for _, pubsubChannel := range pubsub.channels {
				if pubsubChannel.name == channel {
					// Assert that target connection is no longer in the unsub channels and patterns
					if _, ok := pubsubChannel.subscribers[test.targetConn]; ok {
						t.Errorf("found unexpected target connection after unsubscrining in channel \"%s\"", channel)
					}
					for _, conn := range test.otherConnections {
						if _, ok := pubsubChannel.subscribers[conn]; !ok {
							t.Errorf("did not find expected other connection in channel \"%s\"", channel)
						}
					}
				}
			}
		}

		// Assert that the target connection is still in the remain channels and patterns
		for _, channel := range append(test.remainChannels, test.remainPatterns...) {
			for _, pubsubChannel := range pubsub.channels {
				if pubsubChannel.name == channel {
					if _, ok := pubsubChannel.subscribers[test.targetConn]; !ok {
						t.Errorf("could not find expected target connection in channel \"%s\"", channel)
					}
				}
			}
		}
	}

	for _, test := range tests {
		// Close all the connections
		closeConnections(append(test.otherConnections, test.targetConn))
	}
}

func Test_HandlePublish(t *testing.T) {
	ctx := context.WithValue(context.Background(), "test_name", "PUBLISH")

	// verifyChannelMessage reads the message from the connection and asserts whether
	// it's the message we expect to read as a subscriber of a channel or pattern.
	verifyEvent := func(c *net.Conn, r *resp.Conn, expected []string) {
		if err := (*c).SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Error(err)
		}
		rv, _, err := r.ReadValue()
		if err != nil {
			t.Error(err)
		}
		v := rv.Array()
		for i := 0; i < len(v); i++ {
			if v[i].String() != expected[i] {
				t.Errorf("expected item at index %d to be \"%s\", got \"%s\"", i, expected[i], v[i].String())
			}
		}
		fmt.Println(v)
	}

	// The subscribe function handles subscribing the connection to the given
	// channels and patterns and reading/verifying the message sent by the server after
	// subscription.
	subscribe := func(ctx context.Context, channels []string, patterns []string, c *net.Conn, r *resp.Conn) {
		// Subscribe to channels
		go func() {
			_, _ = handleSubscribe(ctx, append([]string{"SUBSCRIBE"}, channels...), mockServer, c)
		}()
		// Verify all the responses for each channel subscription
		for i := 0; i < len(channels); i++ {
			verifyEvent(c, r, []string{"subscribe", channels[i], fmt.Sprintf("%d", i+1)})
		}
		// Subscribe to all the patterns
		go func() {
			_, _ = handleSubscribe(ctx, append([]string{"PSUBSCRIBE"}, patterns...), mockServer, c)
		}()
		// Verify all the responses for each pattern subscription
		for i := 0; i < len(patterns); i++ {
			verifyEvent(c, r, []string{"psubscribe", patterns[i], fmt.Sprintf("%d", i+1)})
		}
	}

	subscriptions := map[string]map[string][]string{
		"subscriber1": {
			"channels": {"pub_channel_1", "pub_channel_2", "pub_channel_3"}, // Channels to subscribe to
			"patterns": {"pub_channel_[456]"},                               // Patterns to subscribe to
		},
		"subscriber2": {
			"channels": {"pub_channel_6", "pub_channel_7"}, // Channels to subscribe to
			"patterns": {"pub_channel_[891]"},              // Patterns to subscribe to
		},
	}

	// Create subscriber one and subscribe to channels and patterns
	r1, w1 := net.Pipe()
	rc1 := resp.NewConn(r1)
	subscribe(ctx, subscriptions["subscriber1"]["channels"], subscriptions["subscriber1"]["patterns"], &w1, rc1)

	// Create subscriber two and subscribe to channels and patterns
	r2, w2 := net.Pipe()
	rc2 := resp.NewConn(r2)
	subscribe(ctx, subscriptions["subscriber2"]["channels"], subscriptions["subscriber2"]["patterns"], &w2, rc2)

	type SubscriberType struct {
		c *net.Conn
		r *resp.Conn
		l string
	}

	tests := []struct {
		channel     string
		message     string
		subscribers []SubscriberType
	}{
		{
			channel: "pub_channel_1",
			message: "Test both subscribers 1",
			subscribers: []SubscriberType{
				{c: &r1, r: rc1, l: "pub_channel_1"},
				{c: &r2, r: rc2, l: "pub_channel_[891]"},
			},
		},
		{
			channel: "pub_channel_6",
			message: "Test both subscribers 2",
			subscribers: []SubscriberType{
				{c: &r1, r: rc1, l: "pub_channel_[456]"},
				{c: &r2, r: rc2, l: "pub_channel_6"},
			},
		},
		{
			channel: "pub_channel_2",
			message: "Test subscriber 1 1",
			subscribers: []SubscriberType{
				{c: &r1, r: rc1, l: "pub_channel_2"},
			},
		},
		{
			channel: "pub_channel_3",
			message: "Test subscriber 1 2",
			subscribers: []SubscriberType{
				{c: &r1, r: rc1, l: "pub_channel_3"},
			},
		},
		{
			channel: "pub_channel_4",
			message: "Test both subscribers 2",
			subscribers: []SubscriberType{
				{c: &r1, r: rc1, l: "pub_channel_[456]"},
			},
		},
		{
			channel: "pub_channel_5",
			message: "Test subscriber 1 3",
			subscribers: []SubscriberType{
				{c: &r1, r: rc1, l: "pub_channel_[456]"},
			},
		},
		{
			channel: "pub_channel_7",
			message: "Test subscriber 2 1",
			subscribers: []SubscriberType{
				{c: &r2, r: rc2, l: "pub_channel_7"},
			},
		},
		{
			channel: "pub_channel_8",
			message: "Test subscriber 2 2",
			subscribers: []SubscriberType{
				{c: &r1, r: rc2, l: "pub_channel_[891]"},
			},
		},
		{
			channel: "pub_channel_9",
			message: "Test subscriber 2 3",
			subscribers: []SubscriberType{
				{c: &r2, r: rc2, l: "pub_channel_[891]"},
			},
		},
	}

	// Dial server to make publisher connection
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err = conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	w := resp.NewConn(conn)

	for _, test := range tests {
		err = w.WriteArray([]resp.Value{
			resp.StringValue("PUBLISH"),
			resp.StringValue(test.channel),
			resp.StringValue(test.message),
		})
		if err != nil {
			t.Error(err)
		}

		rv, _, err := w.ReadValue()
		if err != nil {
			t.Error(err)
		}
		if rv.String() != "OK" {
			t.Errorf("Expected publish response to be \"OK\", got \"%s\"", rv.String())
		}

		for _, sub := range test.subscribers {
			verifyEvent(sub.c, sub.r, []string{"message", sub.l, test.message})
		}
	}
}

func Test_HandlePubSubChannels(t *testing.T) {}

func Test_HandleNumPat(t *testing.T) {}

func Test_HandleNumSub(t *testing.T) {}
