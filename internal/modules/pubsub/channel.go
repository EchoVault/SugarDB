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

package pubsub

import (
	"github.com/gobwas/glob"
	"github.com/tidwall/resp"
	"log"
	"net"
	"sync"
)

type Channel struct {
	name             string                   // Channel name. This can be a glob pattern string.
	pattern          glob.Glob                // Compiled glob pattern. This is nil if the channel is not a pattern channel.
	subscribersRWMut sync.RWMutex             // RWMutex to concurrency control when accessing channel subscribers.
	subscribers      map[*net.Conn]*resp.Conn // Map containing the channel subscribers.
	messageChan      *chan string             // Messages published to this channel will be sent to this channel.
}

// WithName option sets the channels name.
func WithName(name string) func(channel *Channel) {
	return func(channel *Channel) {
		channel.name = name
	}
}

// WithPattern option sets the compiled glob pattern for the channel if it's a pattern channel.
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

func (ch *Channel) Name() string {
	return ch.name
}

func (ch *Channel) Pattern() glob.Glob {
	return ch.pattern
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
	ch.subscribersRWMut.RLock()
	defer ch.subscribersRWMut.RUnlock()

	active := len(ch.subscribers) > 0

	return active
}

func (ch *Channel) NumSubs() int {
	ch.subscribersRWMut.RLock()
	defer ch.subscribersRWMut.RUnlock()

	n := len(ch.subscribers)

	return n
}

func (ch *Channel) Subscribers() map[*net.Conn]*resp.Conn {
	ch.subscribersRWMut.RLock()
	defer ch.subscribersRWMut.RUnlock()

	subscribers := make(map[*net.Conn]*resp.Conn, len(ch.subscribers))
	for k, v := range ch.subscribers {
		subscribers[k] = v
	}

	return subscribers
}
