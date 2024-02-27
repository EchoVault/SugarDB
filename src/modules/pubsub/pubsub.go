package pubsub

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"sync"
)

// PubSub container
type PubSub struct {
	channels      []*Channel
	channelsRWMut sync.RWMutex
}

func NewPubSub() *PubSub {
	return &PubSub{
		channels:      []*Channel{},
		channelsRWMut: sync.RWMutex{},
	}
}

func (ps *PubSub) Subscribe(ctx context.Context, conn *net.Conn, channels []string, withPattern bool) {

	for i := 0; i < len(channels); i++ {
		// Check if channel with given name exists
		// If it does, subscribe the connection to the channel
		// If it does not, create the channel and subscribe to it
		channelIdx := slices.IndexFunc(ps.channels, func(channel *Channel) bool {
			return channel.name == channels[i]
		})

		if channelIdx == -1 {
			// Create new channel, start it, and subscribe to it
			var newChan *Channel
			if withPattern {
				newChan = NewChannel(WithPattern(channels[i]))
			} else {
				newChan = NewChannel(WithName(channels[i]))
			}
			newChan.Start()
			newChan.Subscribe(conn)
			ps.channels = append(ps.channels, newChan)
		} else {
			// Subscribe to existing channel
			ps.channels[channelIdx].Subscribe(conn)
		}

		var res string
		if len(channels) > 1 {
			// If subscribing to more than one channel, write array to verify the subscription of this channel
			res = fmt.Sprintf("*3\r\n+subscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channels[i]), channels[i], i+1)
		} else {
			// Ony one channel, simply send "subscribe" simple string response
			res = "+subscribe\r\n"
		}

		w := io.Writer(*conn)
		if _, err := w.Write([]byte(res)); err != nil {
			log.Println(err)
		}
	}
}

func (ps *PubSub) Unsubscribe(ctx context.Context, conn *net.Conn, channelName string) {
	ps.channelsRWMut.RLock()
	ps.channelsRWMut.RUnlock()

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
	ps.channelsRWMut.RLock()
	defer ps.channelsRWMut.RUnlock()

	for _, channel := range ps.channels {
		fmt.Println(channel.name, channel.pattern)

		// If it's a regular channel, check if the channel name matches the name given
		if channel.pattern == nil {
			if channel.name == channelName {
				channel.Publish(message)
			}
			continue
		}
		// If it's a glob pattern channel, check if the name matches the pattern
		if channel.pattern.Match(channelName) {
			channel.Publish(message)
		}
	}
}
