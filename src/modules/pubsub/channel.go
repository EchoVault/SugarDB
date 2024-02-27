package pubsub

import (
	"fmt"
	"github.com/gobwas/glob"
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
	pattern          glob.Glob
	subscribersRWMut sync.RWMutex
	subscribers      []*net.Conn
	messageChan      *chan string
}

func WithName(name string) func(channel *Channel) {
	return func(channel *Channel) {
		channel.name = name
	}
}

func WithPattern(pattern string) func(channel *Channel) {
	return func(channel *Channel) {
		channel.name = pattern
		channel.pattern = glob.MustCompile(pattern)
	}
}

func NewChannel(options ...func(channel *Channel)) *Channel {
	messageChan := make(chan string, 4096)

	channel := &Channel{
		name:             "",
		pattern:          nil,
		subscribersRWMut: sync.RWMutex{},
		subscribers:      []*net.Conn{},
		messageChan:      &messageChan,
	}

	for _, option := range options {
		option(channel)
	}

	return channel
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

func (ch *Channel) Subscribe(conn *net.Conn) {
	if !slices.Contains(ch.subscribers, conn) {
		ch.subscribersRWMut.Lock()
		defer ch.subscribersRWMut.Unlock()

		ch.subscribers = append(ch.subscribers, conn)
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
