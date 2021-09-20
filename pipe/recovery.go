package pipe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

// on start recover all after timestamp or full
// on reconnect recover all after timestamp
// on alive with subscribed = 0, revocer that producer with last valid ts
// TODO: counting on number of requests per period

// Recovery requests limits: https://docs.betradar.com/display/BD/UOF+-+Access+restrictions+for+odds+recovery
// Recovery sequence explained: https://docs.betradar.com/display/BD/UOF+-+Recovery+using+API

// A client should always store the last successfully received alive message (or
// its timestamp) from each producer. In case of a disconnection, recovery since
// after timestamp should be issued for each affected producer, using the
// timestamp of the last successfully processed alive message before issues
// occurred.

type recoveryProducer struct {
	producer              uof.Producer
	status                uof.ProducerStatus // current status of the producer
	aliveTimestamp        int                // last alive timestamp
	requestID             int                // last recovery requestID
	statusChangedAt       int                // last change of the status
	aliveTimeoutCancel    func()
	recoveryRequestCancel context.CancelFunc
}

func (p *recoveryProducer) setStatus(newStatus uof.ProducerStatus) {
	if p.status != newStatus {
		p.status = newStatus
		ct := uof.CurrentTimestamp()
		if p.statusChangedAt >= ct {
			// ensure monotonic increase (for tests)
			ct = p.statusChangedAt + 1
		}
		p.statusChangedAt = ct
	}
}

func (p *recoveryProducer) alivePulse(dead chan<- uof.Producer, timeout time.Duration) {
	if p.aliveTimeoutCancel != nil {
		p.aliveTimeoutCancel()
		p.aliveTimeoutCancel = nil
	}
	if timeout > 0 {
		alive := make(chan struct{})
		go func() {
			select {
			case <-alive:
				return
			case <-time.After(timeout):
				dead <- p.producer
				return
			}
		}()
		p.aliveTimeoutCancel = func() {
			select {
			case alive <- struct{}{}:
			default:
			}
		}
	}
}

func (p *recoveryProducer) stopPulse() {
	if p.aliveTimeoutCancel != nil {
		p.aliveTimeoutCancel()
		p.aliveTimeoutCancel = nil
	}
}

type recovery struct {
	api       recoveryAPI
	requestID int
	producers []*recoveryProducer
	timeout   time.Duration
	dead      chan uof.Producer
	errc      chan<- error
	subProcs  *sync.WaitGroup
}

type recoveryAPI interface {
	RequestRecovery(producer uof.Producer, timestamp int, requestID int) error
}

func newRecovery(api recoveryAPI, producers uof.ProducersChange, timeout time.Duration) *recovery {
	r := &recovery{
		api:      api,
		timeout:  timeout,
		subProcs: &sync.WaitGroup{},
	}
	ct := uof.CurrentTimestamp()
	for _, p := range producers {
		r.producers = append(r.producers, &recoveryProducer{
			producer:        p.Producer,
			aliveTimestamp:  p.Timestamp,
			status:          uof.ProducerStatusDown,
			statusChangedAt: ct,
		})
	}
	return r
}

func (r *recovery) log(err error) {
	select {
	case r.errc <- uof.E("recovery", err):
	default:
	}
}

func (r *recovery) cancelSubProcs() {
	for _, p := range r.producers {
		if c := p.recoveryRequestCancel; c != nil {
			c()
		}
	}
}

// If producer is back more than recovery window (defined for each producer)
// it has to make full recovery (forced with timestamp = 0).
// Otherwise recovery after timestamp is done.
func recoveryTimestamp(timestamp int, producer uof.Producer) int {
	if uof.CurrentTimestamp()-timestamp >= producer.RecoveryWindow() {
		return 0
	}
	return timestamp
}

func (r *recovery) requestRecovery(p *recoveryProducer) {
	p.setStatus(uof.ProducerStatusInRecovery)
	p.requestID = r.nextRequestID()

	if cancel := p.recoveryRequestCancel; cancel != nil {
		cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.recoveryRequestCancel = cancel

	r.subProcs.Add(1)
	go func(producer uof.Producer, timestamp int, requestID int) {
		defer r.subProcs.Done()
		for {
			recoveryTsp := recoveryTimestamp(timestamp, producer)
			op := fmt.Sprintf("recovery for %s, timestamp: %d, requestID: %d", producer.Code(), recoveryTsp, requestID)
			r.log(fmt.Errorf("starting %s", op))
			err := r.api.RequestRecovery(producer, recoveryTsp, requestID)
			if err == nil {
				return
			}
			r.errc <- uof.Notice(op, err)
			// wait a minute
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}
		}
	}(p.producer, p.aliveTimestamp, p.requestID)
}

func (r *recovery) nextRequestID() int {
	r.requestID++
	return r.requestID
}

func (r *recovery) find(producer uof.Producer) *recoveryProducer {
	for _, rp := range r.producers {
		if rp.producer == producer {
			return rp
		}
	}
	return nil
}

// handles alive message
func (r *recovery) alive(producer uof.Producer, timestamp int, subscribed int) {
	p := r.find(producer)
	if p == nil {
		return // this is expected we are getting alive for all producers in uof (with Subscribed=0)
	}
	p.alivePulse(r.dead, r.timeout)
	switch p.status {
	case uof.ProducerStatusActive:
		if subscribed != 0 {
			p.aliveTimestamp = timestamp
			return
		}
		r.requestRecovery(p)
		return
	case uof.ProducerStatusDown:
		r.requestRecovery(p)
		return
	case uof.ProducerStatusInRecovery:
		return
	}
}

// handles alive timeout
func (r *recovery) aliveTimeout(producer uof.Producer) {
	p := r.find(producer)
	if p == nil {
		return
	}
	p.setStatus(uof.ProducerStatusDown)
}

// handles snapshot complete messages
// set that producer state to active
func (r *recovery) snapshotComplete(producer uof.Producer, requestID int) {
	p := r.find(producer)
	if p == nil {
		r.log(fmt.Errorf("unexpected producer %s", producer))
		return
	}
	if p.status != uof.ProducerStatusInRecovery {
		r.log(fmt.Errorf("producer %s not in recovery", producer))
		return
	}
	if p.requestID != requestID {
		r.log(fmt.Errorf("unexpected requestID %d, expected %d, for producer %s", requestID, p.requestID, producer))
		return
	}
	p.setStatus(uof.ProducerStatusActive)
	p.requestID = 0
}

// start recovery for all producers
func (r *recovery) connectionUp() {
	for _, p := range r.producers {
		p.alivePulse(r.dead, r.timeout)
		if p.status == uof.ProducerStatusDown {
			r.requestRecovery(p)
		}
	}
}

// set status of all producers to down
func (r *recovery) connectionDown() {
	for _, p := range r.producers {
		p.stopPulse()
		p.setStatus(uof.ProducerStatusDown)
	}
}

// most recent status change across all producers
func (r *recovery) statusChangedAt() int {
	var sc int
	for _, r := range r.producers {
		if r.statusChangedAt > sc {
			sc = r.statusChangedAt
		}
	}
	return sc
}

func (r *recovery) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	r.errc = errc
	r.dead = make(chan uof.Producer)
	var statusChangedAt int

	for {
		select {
		case m, ok := <-in:
			if !ok {
				r.cancelSubProcs()
				return r.subProcs
			}
			out <- m
			switch m.Type {
			case uof.MessageTypeAlive:
				r.alive(m.Alive.Producer, m.Alive.Timestamp, m.Alive.Subscribed)
			case uof.MessageTypeSnapshotComplete:
				r.snapshotComplete(m.SnapshotComplete.Producer, m.SnapshotComplete.RequestID)
			case uof.MessageTypeConnection:
				switch m.Connection.Status {
				case uof.ConnectionStatusUp:
					r.connectionUp()
				case uof.ConnectionStatusDown:
					r.connectionDown()
				}
			default:
				continue
			}
		case p := <-r.dead:
			r.aliveTimeout(p)
		}
		if sc := r.statusChangedAt(); sc > statusChangedAt {
			statusChangedAt = sc
			out <- r.producersChangeMessage()
		}
	}
}

func (r *recovery) producersChangeMessage() *uof.Message {
	var psc uof.ProducersChange
	for _, p := range r.producers {
		pc := uof.ProducerChange{
			Producer:  p.producer,
			Status:    p.status,
			Timestamp: p.statusChangedAt,
		}
		if p.status == uof.ProducerStatusInRecovery {
			pc.RecoveryID = p.requestID
		}
		psc = append(psc, pc)
	}
	return uof.NewProducersChangeMessage(psc)
}

func Recovery(api recoveryAPI, producers uof.ProducersChange, timeout time.Duration) InnerStage {
	r := newRecovery(api, producers, timeout)
	return StageWithSubProcesses(r.loop)
}
