package pubsub

import (
	"github.com/gobwas/glob"
	"github.com/tidwall/resp"
	"log"
	"net"
	"sync"
)

// Channel - A channel can be subscribed to directly, or via a consumer group.
// All direct subscribers to the channel will receive any message published to the channel.
// Only one subscriber of a channel's consumer group will receive a message posted to the channel.
type Channel struct {
	name             string
	pattern          glob.Glob
	subscribersRWMut sync.RWMutex
	subscribers      map[*net.Conn]*resp.Conn
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
		subscribers:      make(map[*net.Conn]*resp.Conn),
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
				go func(conn *resp.Conn) {
					if err := conn.WriteArray([]resp.Value{
						resp.StringValue("message"),
						resp.StringValue(ch.name),
						resp.StringValue(message),
					}); err != nil {
						log.Println(err)
					}
				}(conn)
			}

			ch.subscribersRWMut.RUnlock()
		}
	}()
}

func (ch *Channel) Subscribe(conn *net.Conn) bool {
	ch.subscribersRWMut.Lock()
	defer ch.subscribersRWMut.Unlock()
	if _, ok := ch.subscribers[conn]; !ok {
		ch.subscribers[conn] = resp.NewConn(*conn)
	}
	_, ok := ch.subscribers[conn]
	return ok
}

func (ch *Channel) Unsubscribe(conn *net.Conn) bool {
	ch.subscribersRWMut.Lock()
	defer ch.subscribersRWMut.Unlock()
	if _, ok := ch.subscribers[conn]; !ok {
		return false
	}
	delete(ch.subscribers, conn)
	return true
}

func (ch *Channel) Publish(message string) {
	*ch.messageChan <- message
}

func (ch *Channel) IsActive() bool {
	return len(ch.subscribers) > 0
}

func (ch *Channel) NumSubs() int {
	return len(ch.subscribers)
}
