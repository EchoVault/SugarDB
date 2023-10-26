package main

import (
	"fmt"
	"net"
	"time"

	"github.com/kelvinmwinuka/memstore/server/utils"
)

// Consumer group allows multiple subscribers to share the consumption load of a channel.
// Only one subscriber in the consumer group will receive messages published to the channel.
// Once a message is consumed, the subscriber will be moved to the back of the queue and the next
// subscriber will receive the next message.
type ConsumerGroup struct {
	name        string
	subscribers *utils.LinkedList[*net.Conn]
	subIterator *chan *utils.Node[*net.Conn]
	messageChan *chan interface{}
}

func NewConsumerGroup(name string) *ConsumerGroup {
	messageChan := make(chan interface{})
	subscribers := utils.NewLinkedList[*net.Conn](true)
	subIterator := subscribers.Iter()

	return &ConsumerGroup{
		name:        name,
		subscribers: subscribers,
		subIterator: subIterator,
		messageChan: &messageChan,
	}
}

func (cg *ConsumerGroup) Start() {
	go func() {
		for {
			message := <-*cg.messageChan
			fmt.Println("MESSAGE FROM CONSUMER GROUP: ", message)
		}
	}()

	// TODO: For debug only
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			cg.subscribers.Print()
			<-ticker.C
		}
	}()
}

func (cg *ConsumerGroup) Subscribe(conn *net.Conn) {
	if !cg.subscribers.Contains(conn) {
		cg.subscribers.Add(conn)
	}
}

func (cg *ConsumerGroup) Unsubscribe(conn *net.Conn) {
	cg.subscribers.Remove(conn)
}

func (cg *ConsumerGroup) Publish(message interface{}) {
	*cg.messageChan <- message
}

// A channel can be subscribed to directly, or via a consumer group.
// All direct subscribers to the channel will receive any message published to the channel.
// Only one subscriber of a channel's consumer group will receive a message posted to the channel.
type Channel struct {
	name           string
	subscribers    []*net.Conn
	consumerGroups []*ConsumerGroup
	messageChan    *chan interface{}
}

func NewChannel(name string) *Channel {
	messageChan := make(chan interface{})

	return &Channel{
		name:           name,
		subscribers:    []*net.Conn{},
		consumerGroups: []*ConsumerGroup{},
		messageChan:    &messageChan,
	}
}

func (ch *Channel) Start() {
	go func() {
		for {
			message := <-*ch.messageChan
			fmt.Println("MESSAGE FROM CHANNEL: ", message)
		}
	}()
}

func (ch *Channel) Subscribe(conn *net.Conn, consumerGroupName interface{}) {
	if consumerGroupName == nil && !utils.Contains[*net.Conn](ch.subscribers, conn) {
		ch.subscribers = append(ch.subscribers, conn)
		return
	}

	groups := utils.Filter[*ConsumerGroup](ch.consumerGroups, func(group *ConsumerGroup) bool {
		return group.name == consumerGroupName.(string)
	})

	if len(groups) == 0 {
		newGroup := NewConsumerGroup(consumerGroupName.(string))
		newGroup.Start()
		newGroup.Subscribe(conn)
		ch.consumerGroups = append(ch.consumerGroups, newGroup)
		return
	}

	for _, group := range groups {
		group.Subscribe(conn)
	}
}

func (ch *Channel) Unsubscribe(conn *net.Conn) {
	ch.subscribers = utils.Filter[*net.Conn](ch.subscribers, func(c *net.Conn) bool {
		return c != conn
	})

	for _, group := range ch.consumerGroups {
		group.Unsubscribe(conn)
	}
}

func (ch *Channel) Publish(message interface{}) {
	*ch.messageChan <- message
}

// Pub/Sub container
type PubSub struct {
	channels []*Channel
}

func NewPubSub() *PubSub {
	return &PubSub{
		channels: []*Channel{},
	}
}

func (ps *PubSub) Subscribe(conn *net.Conn, channelName interface{}, consumerGroup interface{}) {
	// If no channel name is given, subscribe to all channels
	// Check if channel with given name exists
	// If it does, subscribe the connection to the channel
	// If it does not, create the channel and subscribe to it

	if channelName == nil {
		for _, channel := range ps.channels {
			channel.Subscribe(conn, nil)
		}
		return
	}

	channels := utils.Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})

	if len(channels) <= 0 {
		newChan := NewChannel(channelName.(string))
		newChan.Start()
		newChan.Subscribe(conn, consumerGroup)
		ps.channels = append(ps.channels, newChan)
		return
	}

	for _, channel := range channels {
		channel.Subscribe(conn, consumerGroup)
	}
}

func (ps *PubSub) Unsubscribe(conn *net.Conn, channelName interface{}) {
	if channelName == nil {
		for _, channel := range ps.channels {
			channel.Unsubscribe(conn)
		}
		return
	}

	channels := utils.Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})

	for _, channel := range channels {
		channel.Unsubscribe(conn)
	}
}

func (ps *PubSub) Publish(message interface{}, channelName interface{}) {
	if channelName == nil {
		for _, channel := range ps.channels {
			channel.Publish(message)
		}
	}

	channels := utils.Filter[*Channel](ps.channels, func(c *Channel) bool {
		return c.name == channelName
	})

	for _, channel := range channels {
		channel.Publish(message)
	}
}
