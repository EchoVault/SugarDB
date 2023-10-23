package main

import (
	"net"
	"sync"
)

// Consumer group allows multiple subscribers to share the consumption load of a channel.
// Only one subscriber in the consumer group will receive messages published to the channel.
// Once a message is consumed, the subscriber will be moved to the back of the queue and the next
// subscriber will receive the next message.
type ConsumerGroup struct {
	name        string
	subscribers []*net.TCPConn
}

func (cg *ConsumerGroup) Subscribe(conn *net.TCPConn) error {
	return nil
}

func (cg *ConsumerGroup) Unsubscribe(conn *net.TCPConn) error {
	return nil
}

func (cg *ConsumerGroup) Publish(message interface{}) error {
	return nil
}

// A channel can be subscribed to directly, or via a consumer group.
// All direct subscribers to the channel will receive any message published to the channel.
// Only one subscriber of a channel's consumer group will receive a message posted to the channel.
type Channel struct {
	name           string
	subscribers    []*net.TCPConn
	consumerGroups []*ConsumerGroup
}

func (ch *Channel) Subscribe(conn *net.TCPConn, consumerGroup string) error {
	return nil
}

func (ch *Channel) Unsubscribe(conn *net.TCPConn) error {
	return nil
}

func (ch *Channel) Publish(message interface{}) error {
	return nil
}

// Pub/Sub container
type PubSub struct {
	mut      sync.Mutex
	channels []*Channel
}

func (ps *PubSub) Subscribe(conn *net.TCPConn, channel string, consumerGroup string) error {
	return nil
}

func (ps *PubSub) Unsubscribe(conn *net.TCPConn, channel string) error {
	return nil
}

func (ps *PubSub) Publish(message interface{}, channel string) error {
	return nil
}
