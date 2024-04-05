package clock

import (
	"os"
	"strings"
	"time"
)

type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

func NewClock() Clock {
	// If we're in a test environment, return the mock clock.
	if strings.Contains(os.Args[0], ".test") {
		return MockClock{}
	}
	return RealClock{}
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

type MockClock struct{}

func (MockClock) Now() time.Time {
	t, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	return t
}

func (MockClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}
