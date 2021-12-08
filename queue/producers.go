package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/minus5/go-uof-sdk"
)

// WithProducerHandling is a source stage implementation that wraps around
// WithReconnect and its functionality, and implements producer state handling
// logic on top of it. Producers change messages are inserted into the stream
// of messages in the output channel, allowing any inner stage to consider
// producer states in its behaviour, without having to implement its own logic.
func WithProducerHandling(ctx context.Context, conn *Connection, producers []ProducerConfig) func() (<-chan *uof.Message, <-chan error) {
	return func() (<-chan *uof.Message, <-chan error) {
		out := make(chan *uof.Message)
		errc := make(chan error)

		_msgs, _errc := WithReconnect(ctx, conn)()

		done := make(chan struct{})
		go func() {
			for err := range _errc {
				errc <- err
			}
			close(done)
		}()

		go func() {
			defer close(out)
			defer close(errc)

			statusTsp := 0
			timeouts := make(chan uof.Producer)
			ps := initProducers(producers, timeouts)

			find := func(uofp uof.Producer) *producer {
				for _, p := range ps {
					if p.uofProducer == uofp {
						return p
					}
				}
				return nil
			}

			lastStatusTsp := func() int {
				last := 0
				for _, p := range ps {
					if p.statusTimestamp > last {
						last = p.statusTimestamp
					}
				}
				return last
			}

			for {
				select {

				case m, ok := <-_msgs:
					if !ok {
						<-done
						return
					}

					out <- m

					switch m.Type {
					case uof.MessageTypeConnection:
						for _, p := range ps {
							if m.Connection.Status == uof.ConnectionStatusUp {
								p.connectionUp()
							} else {
								p.connectionDown()
							}
						}
					case uof.MessageTypeAlive:
						p := find(m.Alive.Producer)
						if p != nil {
							p.alive(m.Alive.Timestamp, m.Alive.Subscribed)
						}
					case uof.MessageTypeSnapshotComplete:
						p := find(m.SnapshotComplete.Producer)
						if p != nil {
							err := p.snapshotComplete(m.SnapshotComplete.RequestID)
							if err != nil {
								errc <- uof.E("queue.Snapshot", err)
							}
						}
					}

				case uofp := <-timeouts:
					p := find(uofp)
					if p != nil {
						err := p.timeout()
						if err != nil {
							errc <- uof.E("queue.Timeout", err)
						}
					}
				}

				if lastTsp := lastStatusTsp(); lastTsp > statusTsp {
					statusTsp = lastTsp
					psc := make(uof.ProducersChange, 0)
					for _, p := range ps {
						psc = append(psc, uof.ProducerChange{
							Producer:          p.uofProducer,
							Timestamp:         p.statusTimestamp,
							Status:            p.status,
							RecoveryID:        p.recoveryID,
							RecoveryTimestamp: p.recoveryTimestamp,
							Comment:           p.statusComment,
						})
					}
					out <- uof.NewProducersChangeMessage(psc)
				}
			}
		}()

		return out, errc
	}
}

// PRODUCER CONFIGURATION

type ProducerConfig struct {
	producer    uof.Producer
	timestamp   int
	maxInterval time.Duration
	maxDelay    time.Duration
	timeout     time.Duration
}

func NewProducerConfig(producer uof.Producer) ProducerConfig {
	return ProducerConfig{
		producer:    producer,
		timestamp:   uof.CurrentTimestamp(),
		maxInterval: -1,
		maxDelay:    -1,
		timeout:     -1,
	}
}

func (cfg *ProducerConfig) SetRecoveryTimestamp(timestamp int) {
	cfg.timestamp = timestamp
}

func (cfg *ProducerConfig) SetMaxIntervalDuration(duration time.Duration) {
	cfg.maxInterval = duration
}

func (cfg *ProducerConfig) SetMaxDelayDuration(duration time.Duration) {
	cfg.maxDelay = duration
}

func (cfg *ProducerConfig) SetTimeoutDuration(duration time.Duration) {
	cfg.timeout = duration
}

// PRODUCER STATE

// producer represents a set of data that describes either the current state of
// a producer or conditions for that producer to undergo state trnasitions.
type producer struct {
	uofProducer uof.Producer

	status          uof.ProducerStatus
	statusTimestamp int
	statusComment   string

	aliveTimestamp   int
	aliveMaxInterval time.Duration
	aliveMaxDelay    time.Duration
	aliveTimeout     producerTimeout

	recoveryTimestamp int
	recoveryID        int
}

// initProducers sets up a set of producers per provided configuration. It also
// receives a channel to which alive message timeout signals will be sent.
func initProducers(pcs []ProducerConfig, timeouts chan uof.Producer) []*producer {
	producers := make([]*producer, 0)
	for _, pc := range pcs {
		cb := func() {
			timeouts <- pc.producer
		}
		p := producer{
			uofProducer:       pc.producer,
			status:            uof.ProducerStatusDown,
			statusTimestamp:   uof.CurrentTimestamp(),
			statusComment:     "Initialized.",
			aliveTimestamp:    0,
			aliveMaxInterval:  pc.maxInterval,
			aliveMaxDelay:     pc.maxDelay,
			aliveTimeout:      newTimeout(pc.timeout, cb),
			recoveryTimestamp: pc.timestamp,
			recoveryID:        0,
		}
		producers = append(producers, &p)
	}
	return producers
}

// setStatus saves a new state of a producer, as well as a timestamp of the
// change and a explanatory comment message, if the new state is different
// than the current one.
func (p *producer) setStatus(s uof.ProducerStatus, c string) {
	if p.status != s {
		p.status = s
		p.statusTimestamp = uof.CurrentTimestamp()
		p.statusComment = fmt.Sprintf("%s. Status set to [%s].", c, s.String())
		if s == uof.ProducerStatusInRecovery {
			p.recoveryID++
		}
	}
}

// connectionUp handles producer state when the connection with the source
// server has been established. A producer in the state 'down' is moved to
// state 'inrecovery' and shall attempt recovery.
func (p *producer) connectionUp() {
	if p.status == uof.ProducerStatusDown {
		p.setStatus(uof.ProducerStatusInRecovery, "Connection up")
	}
}

// connectionDown handles producer state when the connection with the source
// server has been lost. The producer is moved to state 'down'.
func (p *producer) connectionDown() {
	p.aliveTimeout.cancel()
	p.aliveTimestamp = 0
	p.setStatus(uof.ProducerStatusDown, "Connection down")
}

// alive handles producer state when an alive message has been received from
// the source server. The effect on the producer state is dependent on its
// current status.
//
// When the producer is in 'active' state:
//  - late or delayed alive message causes it to move to 'down'
//    (connection is deemed unstable or untrustworthy)
//  - otherwise, an alive message with subscribed = 0 moves it to 'inrecovery'
//		(source server signals loss of subscription, enter recovery)
//  - otherwise, update recovery timestamp & restart timeout timer
//
// When the producer is in 'inrecovery' state:
//  - late alive message causes is to move to 'down'
//		(connection is deemed untrustworthy, recovery is invalidated)
//
// When the producer is in 'down' state:
//  - delayed alive message is ignored
//		(connection is deemed unstable, recovery is deferred)
//  - otherwise, move to 'inrecovery' state
//    (connection is deemed stable, attempt recovery)
//
// If the resulting state of the producer is 'active' or 'inrecovery', the
// timestamp of the alive message is saved for checks on subsequent messages;
// otherwise, the saved timestamp value is reset.
func (p *producer) alive(timestamp int, subscribed int) {
	switch p.status {
	case uof.ProducerStatusActive:

		delay, ok := p.aliveDelay(timestamp)
		if !ok {
			p.aliveTimeout.cancel()
			p.aliveTimestamp = 0
			p.setStatus(uof.ProducerStatusDown, fmt.Sprintf("Alive message delayed (%dms)", delay))
			return
		}

		interval, ok := p.aliveInterval(timestamp)
		if !ok {
			p.aliveTimeout.cancel()
			p.aliveTimestamp = 0
			p.setStatus(uof.ProducerStatusDown, fmt.Sprintf("Alive message late (%dms)", interval))
			return
		}

		if subscribed == 0 {
			p.aliveTimeout.cancel()
			p.aliveTimestamp = timestamp
			p.setStatus(uof.ProducerStatusInRecovery, "Producer unsubscribed")
			return
		}

		p.aliveTimeout.restart()
		p.aliveTimestamp = timestamp
		p.recoveryTimestamp = timestamp
		return

	case uof.ProducerStatusInRecovery:

		interval, ok := p.aliveInterval(timestamp)
		if !ok {
			p.aliveTimestamp = 0
			p.setStatus(uof.ProducerStatusDown, fmt.Sprintf("Alive message late (%dms)", interval))
			return
		}

	case uof.ProducerStatusDown:

		_, ok := p.aliveDelay(timestamp)
		if !ok {
			p.aliveTimestamp = 0
			return
		}

		p.aliveTimestamp = timestamp
		p.setStatus(uof.ProducerStatusInRecovery, "Timely alive message")
		return

	}
}

// aliveInterval checks the interval duration between a current timestamp and
// a previously saved timestamp of an alive message for a producer. It returns
// the interval duration in milliseconds and a boolean value that signals
// whether this duration is within the max interval duration configured for
// the producer.
//
// If there is no saved timestamp, or no proper configuration of the max
// interval duration limit, aliveInterval returns values 0 and true.
func (p *producer) aliveInterval(timestamp int) (int, bool) {
	if p.aliveMaxInterval < 0 || p.aliveTimestamp == 0 {
		return 0, true
	}
	interval := timestamp - p.aliveTimestamp
	ok := int64(interval) <= p.aliveMaxInterval.Milliseconds()
	return interval, ok
}

// aliveDelay checks the delay between a timestamp and the current system time.
// It returns the duration of this delay in milliseconds and a boolean value
// that signals whether this duration is within the max delay duration
// configured for the producer.
//
// If there is no proper configuration of the max delay duration limit,
// aliveDelay returns values 0 and true.
func (p *producer) aliveDelay(timestamp int) (int, bool) {
	if p.aliveMaxDelay < 0 {
		return 0, true
	}
	delay := uof.CurrentTimestamp() - timestamp
	ok := int64(delay) <= p.aliveMaxDelay.Milliseconds()
	return delay, ok
}

// snapshotComplete handles producer state when a snapshot_complete message has
// been received from the source server. This message indicates a successfully
// completed recovery. If the producer is in the correct state ('inrecovery')
// and the message contains a matching recovery ID, the producer is moved to
// state 'active'. This is the only scenario that can move a producer from an
// another state to the 'active' state.
func (p *producer) snapshotComplete(id int) error {
	if p.status != uof.ProducerStatusInRecovery {
		return fmt.Errorf("producer %s not in recovery", p.uofProducer.String())
	}

	if p.recoveryID != id {
		return fmt.Errorf("wrong requestID (got %d, expected %d) for producer %s", id, p.recoveryID, p.uofProducer.String())
	}

	p.aliveTimeout.restart()
	p.setStatus(uof.ProducerStatusActive, fmt.Sprintf("Snapshot complete (ID: %d)", id))
	return nil
}

// timeout handles producer state when there were no alive messages received
// for a duration longer than a configured timeout duration. This may indicate
// an issue with either the connection to the source server or the source
// server itself, and, if the producer is active, it is moved to 'down' state.
func (p *producer) timeout() error {
	if p.status != uof.ProducerStatusActive {
		return fmt.Errorf("producer %s not active", p.uofProducer.String())
	}

	p.aliveTimeout.cancel()
	p.aliveTimestamp = 0
	p.setStatus(uof.ProducerStatusDown, "Alive message timeout")
	return nil
}

// PRODUCER TIMEOUT TIMER

// producerTimeout handles timeout logic for timing out alive message awaiting
type producerTimeout struct {
	stop     chan struct{}
	duration time.Duration
	callback func()
}

// newTimeout sets up a new timeout handler with desired timeout duration and
// callback function executed upon reaching timeout
func newTimeout(dur time.Duration, cb func()) producerTimeout {
	return producerTimeout{
		duration: dur,
		callback: cb,
	}
}

// restart stops a currently ongoing timeout timer and starts a new one
func (t *producerTimeout) restart() {
	t.cancel()
	if t.duration > 0 {
		t.stop = make(chan struct{}, 1)
		go func(t producerTimeout) {
			select {
			case <-t.stop:
				return
			case <-time.After(t.duration):
				t.callback()
				return
			}
		}(*t)
	}
}

// cancel stops a currently ongoing timeout timer, if there is one
func (t *producerTimeout) cancel() {
	select {
	case t.stop <- struct{}{}:
	default:
	}
}
