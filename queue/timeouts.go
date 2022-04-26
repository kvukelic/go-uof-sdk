package queue

import (
	"context"
	"time"

	"github.com/minus5/go-uof-sdk"
)

func WithTimeouts(ctx context.Context, conn *Connection, config []TimeoutConfig) func() (<-chan *uof.Message, <-chan error) {
	return func() (<-chan *uof.Message, <-chan error) {
		_out, _errc := WithReconnect(ctx, conn)()
		out := make(chan *uof.Message)
		errc := make(chan error)

		go func() {
			defer close(errc)
			for err := range _errc {
				errc <- err
			}
		}()

		go func() {
			defer close(out)
			state := newTimeoutState(config)
			defer state.close()
			for {
				select {
				case p := <-state.output:
					out <- uof.NewAliveTimeoutMessage(p)
				case msg, ok := <-_out:
					if ok {
						out <- msg
						switch msg.Type {
						case uof.MessageTypeConnection:
							switch msg.Connection.Status {
							case uof.ConnectionStatusUp:
								state.startAll()
							case uof.ConnectionStatusDown:
								state.stopAll()
							}
						case uof.MessageTypeAlive:
							state.start(msg.Alive.Producer)
						}
					} else {
						return
					}
				}
			}
		}()

		return out, errc
	}
}

// CONFIG

type TimeoutConfig struct {
	producer uof.Producer
	duration time.Duration
}

func NewTimeoutConfig(p uof.Producer) TimeoutConfig {
	return TimeoutConfig{
		producer: p,
		duration: -1,
	}
}

func (tc *TimeoutConfig) SetDuration(d time.Duration) {
	tc.duration = d
}

// STATE

type timeoutState struct {
	durations map[uof.Producer]time.Duration
	timeouts  map[uof.Producer]<-chan time.Time
	resets    map[uof.Producer]chan struct{}
	output    chan uof.Producer
	done      chan struct{}
}

func newTimeoutState(config []TimeoutConfig) *timeoutState {
	ts := &timeoutState{
		durations: make(map[uof.Producer]time.Duration),
		timeouts:  make(map[uof.Producer]<-chan time.Time),
		resets:    make(map[uof.Producer]chan struct{}),
		output:    make(chan uof.Producer),
		done:      make(chan struct{}),
	}
	for _, c := range config {
		if c.duration > 0 {
			ts.durations[c.producer] = c.duration
			ts.resets[c.producer] = make(chan struct{})
			go ts.timeoutLoop(c.producer)
		}
	}
	return ts
}

func (ts *timeoutState) timeoutLoop(p uof.Producer) {
	for {
		select {
		case <-ts.done:
			return
		case <-ts.resets[p]:
			continue
		case <-ts.timeouts[p]:
		}
		select {
		case <-ts.resets[p]:
			continue
		case ts.output <- p:
		}
	}
}

func (ts *timeoutState) start(p uof.Producer) {
	if dur, ok := ts.durations[p]; ok {
		ts.timeouts[p] = time.NewTimer(dur).C
		ts.resets[p] <- struct{}{}
	}
}

func (ts *timeoutState) startAll() {
	for p, dur := range ts.durations {
		ts.timeouts[p] = time.NewTimer(dur).C
		ts.resets[p] <- struct{}{}
	}
}

func (ts *timeoutState) stopAll() {
	for p := range ts.timeouts {
		ts.timeouts[p] = nil
		ts.resets[p] <- struct{}{}
	}
}

func (ts *timeoutState) close() {
	ts.stopAll()
	close(ts.done)
}
