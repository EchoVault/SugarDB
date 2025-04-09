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
	"context"
	"encoding/json"
	"fmt"
	"github.com/gobwas/glob"
	"github.com/tidwall/resp"
	"log"
	"net"
	"slices"
	"sync"
)

type Channel struct {
	name    string    // Channel name. This can be a glob pattern string.
	pattern glob.Glob // Compiled glob pattern. This is nil if the channel is not a pattern channel.

	messages      []string     // Slice that holds messages.
	messagesRWMut sync.RWMutex // RWMutex for accessing channel messages.
	messagesCond  *sync.Cond

	tcpSubs      map[*net.Conn]*resp.Conn // Map containing the channel's TCP subscribers.
	tcpSubsRWMut sync.RWMutex             // RWMutex for accessing TCP channel subscribers.

	embeddedSubs      []*EmbeddedSub // Slice containing embedded subscribers to this channel.
	embeddedSubsRWMut sync.RWMutex   // RWMutex for accessing embedded subscribers.
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

func NewChannel(ctx context.Context, options ...func(channel *Channel)) *Channel {
	channel := &Channel{
		name:    "",
		pattern: nil,

		messages:      make([]string, 0),
		messagesRWMut: sync.RWMutex{},

		tcpSubs:      make(map[*net.Conn]*resp.Conn),
		tcpSubsRWMut: sync.RWMutex{},

		embeddedSubs:      make([]*EmbeddedSub, 0),
		embeddedSubsRWMut: sync.RWMutex{},
	}
	channel.messagesCond = sync.NewCond(&channel.messagesRWMut)

	for _, option := range options {
		option(channel)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("closing channel %s\n", channel.name)
				return
			default:
				channel.messagesRWMut.Lock()
				for len(channel.messages) == 0 {
					channel.messagesCond.Wait()
				}

				message := channel.messages[0]
				channel.messages = channel.messages[1:]
				channel.messagesRWMut.Unlock()

				wg := sync.WaitGroup{}
				wg.Add(2)

				// Send messages to embedded subscribers
				go func() {
					channel.embeddedSubsRWMut.RLock()
					ewg := sync.WaitGroup{}
					b, _ := json.Marshal([]string{"message", channel.name, message})
					msg := append(b, byte('\n'))
					for _, w := range channel.embeddedSubs {
						ewg.Add(1)
						go func(w *EmbeddedSub) {
							_, _ = w.Write(msg)
							ewg.Done()
						}(w)
					}
					ewg.Wait()
					channel.embeddedSubsRWMut.RUnlock()
					wg.Done()
				}()

				// Send messages to TCP subscribers
				go func() {
					channel.tcpSubsRWMut.RLock()
					cwg := sync.WaitGroup{}
					for _, conn := range channel.tcpSubs {
						cwg.Add(1)
						go func(conn *resp.Conn) {
							_ = conn.WriteArray([]resp.Value{
								resp.StringValue("message"),
								resp.StringValue(channel.name),
								resp.StringValue(message),
							})
							cwg.Done()
						}(conn)
					}
					cwg.Wait()
					channel.tcpSubsRWMut.RUnlock()
					wg.Done()
				}()

				wg.Wait()
			}
		}
	}()

	return channel
}

func (ch *Channel) Name() string {
	return ch.name
}

func (ch *Channel) Pattern() glob.Glob {
	return ch.pattern
}

func (ch *Channel) Subscribe(sub any, action string, chanIdx int) {
	switch sub.(type) {
	case *net.Conn:
		ch.tcpSubsRWMut.Lock()
		defer ch.tcpSubsRWMut.Unlock()
		conn := sub.(*net.Conn)
		if _, ok := ch.tcpSubs[conn]; !ok {
			ch.tcpSubs[conn] = resp.NewConn(*conn)
		}
		r, _ := ch.tcpSubs[conn]
		// Send subscription message
		_ = r.WriteArray([]resp.Value{
			resp.StringValue(action),
			resp.StringValue(ch.name),
			resp.IntegerValue(chanIdx + 1),
		})

	case *EmbeddedSub:
		ch.embeddedSubsRWMut.Lock()
		defer ch.embeddedSubsRWMut.Unlock()
		w := sub.(*EmbeddedSub)
		if !slices.ContainsFunc(ch.embeddedSubs, func(writer *EmbeddedSub) bool {
			return writer == w
		}) {
			ch.embeddedSubs = append(ch.embeddedSubs, w)
		}
		// Send subscription message
		b, _ := json.Marshal([]string{action, ch.name, fmt.Sprintf("%d", chanIdx+1)})
		msg := append(b, byte('\n'))
		_, _ = w.Write(msg)
	}
}

func (ch *Channel) Unsubscribe(sub any) bool {
	switch sub.(type) {
	default:
		return false

	case *net.Conn:
		ch.tcpSubsRWMut.Lock()
		defer ch.tcpSubsRWMut.Unlock()
		conn := sub.(*net.Conn)
		if _, ok := ch.tcpSubs[conn]; !ok {
			return false
		}
		delete(ch.tcpSubs, conn)
		return true

	case *EmbeddedSub:
		ch.embeddedSubsRWMut.Lock()
		defer ch.embeddedSubsRWMut.Unlock()
		w := sub.(*EmbeddedSub)
		deleted := false
		ch.embeddedSubs = slices.DeleteFunc(ch.embeddedSubs, func(writer *EmbeddedSub) bool {
			deleted = writer == w
			return deleted
		})
		return deleted
	}
}

func (ch *Channel) Publish(message string) {
	ch.messagesRWMut.Lock()
	defer ch.messagesRWMut.Unlock()
	ch.messages = append(ch.messages, message)
	ch.messagesCond.Signal()
}

func (ch *Channel) IsActive() bool {
	ch.tcpSubsRWMut.RLock()
	defer ch.tcpSubsRWMut.RUnlock()

	ch.embeddedSubsRWMut.RLock()
	defer ch.embeddedSubsRWMut.RUnlock()

	return len(ch.tcpSubs)+len(ch.embeddedSubs) > 0
}

func (ch *Channel) NumSubs() int {
	ch.tcpSubsRWMut.RLock()
	defer ch.tcpSubsRWMut.RUnlock()

	ch.embeddedSubsRWMut.RLock()
	defer ch.embeddedSubsRWMut.RUnlock()

	return len(ch.tcpSubs) + len(ch.embeddedSubs)
}
