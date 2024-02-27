package pubsub

import (
	"context"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"io"
	"log"
	"net"
	"slices"
	"sync"
)

// Channel - A channel can be subscribed to directly, or via a consumer group.
// All direct subscribers to the channel will receive any message published to the channel.
// Only one subscriber of a channel's consumer group will receive a message posted to the channel.
type Channel struct {
	name             string
	subscribersRWMut sync.RWMutex
	subscribers      []*net.Conn
	messageChan      *chan string
}

func NewChannel(name string) *Channel {
	messageChan := make(chan string, 4096)

	return &Channel{
		name:             name,
		subscribersRWMut: sync.RWMutex{},
		subscribers:      []*net.Conn{},
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

					if _, err := w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message))); err != nil {
						log.Println(err)
					}
				}(conn)
			}

			ch.subscribersRWMut.RUnlock()
		}
	}()
}

func (ch *Channel) Subscribe(conn *net.Conn, index int) {
	if !slices.Contains(ch.subscribers, conn) {
		ch.subscribersRWMut.Lock()
		defer ch.subscribersRWMut.Unlock()

		ch.subscribers = append(ch.subscribers, conn)

		// Write array to verify the subscription of this channel
		res := fmt.Sprintf("*3\r\n+subscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(ch.name), ch.name, index+1)
		w := io.Writer(*conn)
		if _, err := w.Write([]byte(res)); err != nil {
			log.Println(err)
		}
	}
}

func (ch *Channel) Unsubscribe(conn *net.Conn, waitGroup *sync.WaitGroup) {
	ch.subscribersRWMut.Lock()
	defer ch.subscribersRWMut.Unlock()

	ch.subscribers = slices.DeleteFunc(ch.subscribers, func(c *net.Conn) bool {
		return c == conn
	})

	if waitGroup != nil {
		waitGroup.Done()
	}
}

func (ch *Channel) Publish(message string) {
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

func (ps *PubSub) Subscribe(ctx context.Context, conn *net.Conn, channelName string, index int) {
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
			newChan.Subscribe(conn, index)
			ps.channels = append(ps.channels, newChan)
		}()
		return
	}

	ps.channels[channelIdx].Subscribe(conn, index)
}

func (ps *PubSub) Unsubscribe(ctx context.Context, conn *net.Conn, channelName string) {
	if channelName == "*" {
		wg := &sync.WaitGroup{}
		for _, channel := range ps.channels {
			wg.Add(1)
			go channel.Unsubscribe(conn, wg)
		}
		wg.Wait()
		return
	}

	channelIdx := slices.IndexFunc(ps.channels, func(channel *Channel) bool {
		return channel.name == channelName
	})

	if channelIdx != -1 {
		ps.channels[channelIdx].Unsubscribe(conn, nil)
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
