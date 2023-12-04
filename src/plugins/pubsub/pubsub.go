package main

import (
	"bufio"
	"container/ring"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// ConsumerGroup allows multiple subscribers to share the consumption load of a channel.
// Only one subscriber in the consumer group will receive messages published to the channel.
type ConsumerGroup struct {
	name             string
	subscribersRWMut sync.RWMutex
	subscribers      *ring.Ring
	messageChan      *chan string
}

func NewConsumerGroup(name string) *ConsumerGroup {
	messageChan := make(chan string)

	return &ConsumerGroup{
		name:             name,
		subscribersRWMut: sync.RWMutex{},
		subscribers:      nil,
		messageChan:      &messageChan,
	}
}

func (cg *ConsumerGroup) SendMessage(message string) {
	cg.subscribersRWMut.RLock()

	conn := cg.subscribers.Value.(*net.Conn)

	cg.subscribersRWMut.RUnlock()

	rw := bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn))
	rw.WriteString(fmt.Sprintf("$%d\r\n%s\r\n\n", len(message), message))
	rw.Flush()

	// Wait for an ACK
	// If no ACK is received within a time limit, remove this connection from subscribers and retry
	(*conn).SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	if msg, err := ReadMessage(rw); err != nil {
		// Remove the connection from subscribers list
		cg.Unsubscribe(conn)
		// Reset the deadline
		(*conn).SetReadDeadline(time.Time{})
		// Retry sending the message
		cg.SendMessage(message)
	} else {
		if strings.TrimSpace(msg) != "+ACK" {
			cg.Unsubscribe(conn)
			(*conn).SetReadDeadline(time.Time{})
			cg.SendMessage(message)
		}
	}

	(*conn).SetDeadline(time.Time{})
	cg.subscribers = cg.subscribers.Next()
}

func (cg *ConsumerGroup) Start() {
	go func() {
		for {
			message := <-*cg.messageChan
			if cg.subscribers != nil {
				cg.SendMessage(message)
			}
		}
	}()
}

func (cg *ConsumerGroup) Subscribe(conn *net.Conn) {
	cg.subscribersRWMut.Lock()
	defer cg.subscribersRWMut.Unlock()

	r := ring.New(1)
	for i := 0; i < r.Len(); i++ {
		r.Value = conn
		r = r.Next()
	}

	if cg.subscribers == nil {
		cg.subscribers = r
		return
	}

	cg.subscribers = cg.subscribers.Link(r)
}

func (cg *ConsumerGroup) Unsubscribe(conn *net.Conn) {
	cg.subscribersRWMut.Lock()
	defer cg.subscribersRWMut.Unlock()

	// If length is 1 and the connection passed is the one contained within, unlink it
	if cg.subscribers.Len() == 1 {
		if cg.subscribers.Value == conn {
			cg.subscribers = nil
		}
		return
	}

	for i := 0; i < cg.subscribers.Len(); i++ {
		if cg.subscribers.Value == conn {
			cg.subscribers = cg.subscribers.Prev()
			cg.subscribers.Unlink(1)
			break
		}
		cg.subscribers = cg.subscribers.Next()
	}
}

func (cg *ConsumerGroup) Publish(message string) {
	*cg.messageChan <- message
}

// Channel - A channel can be subscribed to directly, or via a consumer group.
// All direct subscribers to the channel will receive any message published to the channel.
// Only one subscriber of a channel's consumer group will receive a message posted to the channel.
type Channel struct {
	name             string
	subscribersRWMut sync.RWMutex
	subscribers      []*net.Conn
	consumerGroups   []*ConsumerGroup
	messageChan      *chan string
}

func NewChannel(name string) *Channel {
	messageChan := make(chan string)

	return &Channel{
		name:             name,
		subscribersRWMut: sync.RWMutex{},
		subscribers:      []*net.Conn{},
		consumerGroups:   []*ConsumerGroup{},
		messageChan:      &messageChan,
	}
}

func (ch *Channel) Start() {
	go func() {
		for {
			message := <-*ch.messageChan

			ch.subscribersRWMut.RLock()

			for _, conn := range ch.subscribers {
				go func(conn *net.Conn) {
					rw := bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn))
					rw.WriteString(fmt.Sprintf("$%d\r\n%s\r\n\n", len(message), message))
					rw.Flush()

					(*conn).SetReadDeadline(time.Now().Add(200 * time.Millisecond))
					defer func() {
						(*conn).SetReadDeadline(time.Time{})
					}()

					if msg, err := ReadMessage(rw); err != nil {
						ch.Unsubscribe(conn)
					} else {
						if strings.TrimSpace(msg) != "+ACK" {
							ch.Unsubscribe(conn)
						}
					}
				}(conn)
			}

			ch.subscribersRWMut.RUnlock()
		}
	}()
}

func (ch *Channel) Subscribe(conn *net.Conn, consumerGroupName interface{}) {
	if consumerGroupName == nil && !Contains[*net.Conn](ch.subscribers, conn) {
		ch.subscribersRWMut.Lock()
		defer ch.subscribersRWMut.Unlock()
		ch.subscribers = append(ch.subscribers, conn)
		return
	}

	groups := Filter[*ConsumerGroup](ch.consumerGroups, func(group *ConsumerGroup) bool {
		return group.name == consumerGroupName.(string)
	})

	if len(groups) == 0 {
		go func() {
			newGroup := NewConsumerGroup(consumerGroupName.(string))
			newGroup.Start()
			newGroup.Subscribe(conn)
			ch.consumerGroups = append(ch.consumerGroups, newGroup)
		}()
		return
	}

	for _, group := range groups {
		go group.Subscribe(conn)
	}
}

func (ch *Channel) Unsubscribe(conn *net.Conn) {
	ch.subscribersRWMut.Lock()
	defer ch.subscribersRWMut.Unlock()

	ch.subscribers = Filter[*net.Conn](ch.subscribers, func(c *net.Conn) bool {
		return c != conn
	})

	for _, group := range ch.consumerGroups {
		go group.Unsubscribe(conn)
	}
}

func (ch *Channel) Publish(message string) {
	for _, group := range ch.consumerGroups {
		go group.Publish(message)
	}
	*ch.messageChan <- message
}

// PubSub container
type PubSub struct {
	channels []*Channel
}

func NewPubSub() *PubSub {
	return &PubSub{
		channels: []*Channel{},
	}
}

func (ps *PubSub) Subscribe(ctx context.Context, conn *net.Conn, channelName interface{}, consumerGroup interface{}) {
	// If no channel name is given, subscribe to all channels
	if channelName == nil {
		for _, channel := range ps.channels {
			go channel.Subscribe(conn, nil)
		}
		return
	}

	// Check if channel with given name exists
	// If it does, subscribe the connection to the channel
	// If it does not, create the channel and subscribe to it
	channels := Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})

	if len(channels) <= 0 {
		go func() {
			newChan := NewChannel(channelName.(string))
			newChan.Start()
			newChan.Subscribe(conn, consumerGroup)
			ps.channels = append(ps.channels, newChan)
		}()
		return
	}

	for _, channel := range channels {
		go channel.Subscribe(conn, consumerGroup)
	}
}

func (ps *PubSub) Unsubscribe(ctx context.Context, conn *net.Conn, channelName interface{}) {
	if channelName == nil {
		for _, channel := range ps.channels {
			go channel.Unsubscribe(conn)
		}
		return
	}

	channels := Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})

	for _, channel := range channels {
		go channel.Unsubscribe(conn)
	}
}

func (ps *PubSub) Publish(ctx context.Context, message string, channelName interface{}) {
	if channelName == nil {
		for _, channel := range ps.channels {
			go channel.Publish(message)
		}
		return
	}

	channels := Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})

	for _, channel := range channels {
		go channel.Publish(message)
	}
}
