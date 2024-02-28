package pubsub

import (
	"context"
	"fmt"
	"github.com/gobwas/glob"
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

func (ps *PubSub) Subscribe(ctx context.Context, conn *net.Conn, channels []string, withPattern bool) []byte {
	res := fmt.Sprintf("*%d\r\n", len(channels))

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

		if len(channels) > 1 {
			// If subscribing to more than one channel, write array to verify the subscription of this channel
			res += fmt.Sprintf("*3\r\n+subscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channels[i]), channels[i], i+1)
		} else {
			// Ony one channel, simply send "subscribe" simple string response
			res = "+subscribe\r\n"
		}
	}

	return []byte(res)
}

func (ps *PubSub) Unsubscribe(ctx context.Context, conn *net.Conn, channels []string, withPattern bool) []byte {
	ps.channelsRWMut.RLock()
	ps.channelsRWMut.RUnlock()

	action := "unsubscribe"
	if withPattern {
		action = "subscribe"
	}

	unsubscribed := make(map[int]string)
	count := 1

	// If the channels slice is empty, unsubscribe from all channels.
	if len(channels) <= 0 {
		for _, channel := range ps.channels {
			if channel.Unsubscribe(conn) {
				unsubscribed[1] = channel.name
				count += 1
			}
		}
	}

	// If withPattern is false, unsubscribe from channels where the name exactly matches channel name.
	if !withPattern {
		for _, channel := range ps.channels { // For each channel in PubSub
			for _, c := range channels { // For each channel name provided
				if channel.name == c && channel.Unsubscribe(conn) {
					unsubscribed[count] = channel.name
					count += 1
				}
			}
		}
	}

	// If withPattern is true, unsubscribe from channels where pattern matches pattern provided,
	// also unsubscribe from channels where the name matches the given pattern.
	if withPattern {
		for _, pattern := range channels {
			g := glob.MustCompile(pattern)
			for _, channel := range ps.channels {
				// If it's a pattern channel, directly compare the patterns
				if channel.pattern != nil && channel.name == pattern {
					unsubscribed[count] = channel.name
					count += 1
					continue
				}
				// If this is a regular channel, check if the channel name matches the pattern given
				if g.Match(channel.name) {
					unsubscribed[count] = channel.name
					count += 1
				}
			}
		}
	}

	res := fmt.Sprintf("*%d\r\n", len(unsubscribed))
	for key, value := range unsubscribed {
		res += fmt.Sprintf("*3\r\n+%s\r\n$%d\r\n%s\r\n:%d\r\n", action, len(value), value, key)
	}

	return []byte(res)
}

func (ps *PubSub) Publish(ctx context.Context, message string, channelName string) {
	ps.channelsRWMut.RLock()
	defer ps.channelsRWMut.RUnlock()

	for _, channel := range ps.channels {
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

func (ps *PubSub) Channels(ctx context.Context, pattern string) []byte {
	var count int
	var res string

	if pattern == "" {
		for _, channel := range ps.channels {
			if channel.IsActive() {
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(channel.name), channel.name)
				count += 1
			}
		}

		res = fmt.Sprintf("*%d\r\n%s", count, res)
		return []byte(res)
	}

	g := glob.MustCompile(pattern)

	for _, channel := range ps.channels {
		// If channel is a pattern channel, then directly compare the channel name to pattern
		if channel.pattern != nil && channel.name == pattern && channel.IsActive() {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(channel.name), channel.name)
			count += 1
			continue
		}
		if g.Match(channel.name) && channel.IsActive() {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(channel.name), channel.name)
			count += 1
		}
	}

	return []byte(res)
}

func (ps *PubSub) NumPat(ctx context.Context) int {
	var count int
	for _, channel := range ps.channels {
		if channel.pattern != nil {
			count += 1
		}
	}
	return count
}

func (ps *PubSub) NumSub(ctx context.Context, channels []string) []byte {
	res := fmt.Sprintf("*%d\r\n", len(channels))
	for _, channel := range channels {
		chanIdx := slices.IndexFunc(ps.channels, func(c *Channel) bool {
			return c.name == channel
		})
		if chanIdx == -1 {
			res += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n:0\r\n", len(channel), channel)
			continue
		}
		res += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, ps.channels[chanIdx].NumSubs())
	}
	return []byte(res)
}
