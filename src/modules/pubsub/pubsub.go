package pubsub

import (
	"bytes"
	"container/ring"
	"context"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"io"
	"net"
	"slices"
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

	w := io.Writer(*conn)
	r := io.Reader(*conn)

	if _, err := w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n\n", len(message), message))); err != nil {
		// TODO: Log error at configured logger
		fmt.Println(err)
	}
	// Wait for an ACK
	// If no ACK is received within a time limit, remove this connection from subscribers and retry
	if err := (*conn).SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		// TODO: Log error at configured logger
		fmt.Println(err)
	}
	if msg, err := utils.ReadMessage(r); err != nil {
		// Remove the connection from subscribers list
		cg.Unsubscribe(conn)
		// Reset the deadline
		if err := (*conn).SetReadDeadline(time.Time{}); err != nil {
			// TODO: Log error at configured logger
			fmt.Println(err)
		}
		// Retry sending the message
		cg.SendMessage(message)
	} else {
		if !bytes.Equal(bytes.TrimSpace(msg), []byte("+ACK")) {
			cg.Unsubscribe(conn)
			if err := (*conn).SetReadDeadline(time.Time{}); err != nil {
				// TODO: Log error at configured logger
				fmt.Println(err)
			}
			cg.SendMessage(message)
		}
	}

	if err := (*conn).SetDeadline(time.Time{}); err != nil {
		// TODO: Log error at configured logger
		fmt.Println(err)
	}
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
					w := io.Writer(*conn)
					r := io.Reader(*conn)

					if _, err := w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n\r\n", len(message), message))); err != nil {
						// TODO: Log error at configured logger
						fmt.Println(err)
					}

					if err := (*conn).SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
						// TODO: Log error at configured logger
						fmt.Println(err)
						ch.Unsubscribe(conn)
					}
					defer func() {
						if err := (*conn).SetReadDeadline(time.Time{}); err != nil {
							// TODO: Log error at configured logger
							fmt.Println(err)
							ch.Unsubscribe(conn)
						}
					}()

					if msg, err := utils.ReadMessage(r); err != nil {
						ch.Unsubscribe(conn)
					} else {
						if !bytes.EqualFold(bytes.TrimSpace(msg), []byte("+ACK")) {
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
	if consumerGroupName == nil && !slices.Contains(ch.subscribers, conn) {
		ch.subscribersRWMut.Lock()
		defer ch.subscribersRWMut.Unlock()
		ch.subscribers = append(ch.subscribers, conn)
		return
	}

	groups := utils.Filter[*ConsumerGroup](ch.consumerGroups, func(group *ConsumerGroup) bool {
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

	ch.subscribers = utils.Filter[*net.Conn](ch.subscribers, func(c *net.Conn) bool {
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

func (ps *PubSub) Subscribe(ctx context.Context, conn *net.Conn, channelName string, consumerGroup interface{}) {
	// Check if channel with given name exists
	// If it does, subscribe the connection to the channel
	// If it does not, create the channel and subscribe to it
	channelIdx := slices.IndexFunc(ps.channels, func(channel *Channel) bool {
		return channel.name == channelName
	})

	if channelIdx == -1 {
		go func() {
			newChan := NewChannel(channelName)
			newChan.Start()
			newChan.Subscribe(conn, consumerGroup)
			ps.channels = append(ps.channels, newChan)
		}()
		return
	}

	go ps.channels[channelIdx].Subscribe(conn, consumerGroup)
}

func (ps *PubSub) Unsubscribe(ctx context.Context, conn *net.Conn, channelName interface{}) {
	if channelName == nil {
		for _, channel := range ps.channels {
			go channel.Unsubscribe(conn)
		}
		return
	}

	channels := utils.Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})

	for _, channel := range channels {
		go channel.Unsubscribe(conn)
	}
}

func (ps *PubSub) Publish(ctx context.Context, message string, channelName string) {
	channels := utils.Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})
	for _, channel := range channels {
		go channel.Publish(message)
	}
}
