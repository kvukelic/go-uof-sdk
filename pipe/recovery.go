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
	producer uof.Producer

	status              uof.ProducerStatus // current status of the producer
	statusChangedAt     int                // timestamp of last status change
	statusChangeComment string             // explanatory message about the last status change

	aliveConfiguration uof.AliveConfiguration // configuration of parameters for alive handling
	aliveTimeoutHandle func()                 // handle alive timeout
	aliveTimeoutCancel func()                 // cancel current alive timeout timer
	aliveTimestamp     int                    // last valid alive timestamp

	recoveryRequestID     int                // last recovery requestID
	recoveryRequestCancel context.CancelFunc // cancel current recovery request
	recoveryTimestamp     int                // last alive timestamp while producer was active (for recovery)

	subProcsAdd  func()
	subProcsDone func()
}

func (p *recoveryProducer) setStatusActive(reason string) {
	p.recoveryRequestID = 0
	if p.recoveryRequestCancel != nil {
		p.recoveryRequestCancel()
	}
	p.setStatus(uof.ProducerStatusActive, reason)
	p.resetTimeout()
}

func (p *recoveryProducer) setStatusDown(reason string) {
	p.recoveryRequestID = 0
	if p.recoveryRequestCancel != nil {
		p.recoveryRequestCancel()
	}
	p.cancelTimeout()
	p.setStatus(uof.ProducerStatusDown, reason)
}

func (p *recoveryProducer) setStatusRecovery(requestID int) {
	p.cancelTimeout()
	reason := fmt.Sprintf("Initiating recovery (ID: %d)", requestID)
	p.setStatus(uof.ProducerStatusInRecovery, reason)
	if p.recoveryRequestCancel != nil {
		p.recoveryRequestCancel()
	}
	p.recoveryRequestID = requestID
}

func (p *recoveryProducer) setStatus(newStatus uof.ProducerStatus, reason string) {
	if p.status != newStatus {
		p.status = newStatus
		p.statusChangeComment = fmt.Sprintf("%s. Status set to [%s].", reason, newStatus.String())
		ct := uof.CurrentTimestamp()
		if p.statusChangedAt >= ct {
			// ensure monotonic increase (for tests)
			ct = p.statusChangedAt + 1
		}
		p.statusChangedAt = ct
	}
}

func (p *recoveryProducer) alive(timestamp int, subscribed int) bool {
	aliveTsp := timestamp
	defer func() {
		p.aliveTimestamp = aliveTsp
	}()

	switch p.status {
	case uof.ProducerStatusActive:
		// irregular alive timestamp interval
		//   - signals problems on source server
		//   - go down and wait for a valid alive message to start recovery
		if val, ok := p.checkAliveInterval(timestamp); !ok {
			p.setStatusDown(fmt.Sprintf("Alive tsp interval limit exceeded (%dms)", val))
			aliveTsp = 0
			return false
		}

		// delay in receiving the alive message
		//   - signals problems in message delivery or processing
		//   - go down and wait for a valid alive message to start recovery
		if val, ok := p.checkAliveDelay(timestamp); !ok {
			p.setStatusDown(fmt.Sprintf("Alive msg delay limit exceeded (%dms)", val))
			aliveTsp = 0
			return false
		}

		// attribute 'subscribed' is set to 0
		//   - signals subscription to feed was interrupted for any reason
		//   - the message is otherwise ok, start recovery now
		if subscribed == 0 {
			return true
		}

		// alive message is ok
		//   - reset alive timeout timer
		//   - update timestamp of last alive message in active state
		p.resetTimeout()
		p.recoveryTimestamp = timestamp
		return false

	case uof.ProducerStatusDown:
		// delay in receiving the alive message
		//   - signals problems in message delivery or processing
		//   - stay down, do not attempt to recover yet
		if _, ok := p.checkAliveDelay(timestamp); !ok {
			aliveTsp = 0
			return false
		}

		// alive message is ok
		//   - start recovery
		return true

	case uof.ProducerStatusInRecovery:
		// irregular alive timestamp interval
		//   - signals problems on source server
		//   - go down and wait for a valid alive message to start recovery
		if val, ok := p.checkAliveInterval(timestamp); !ok {
			p.setStatusDown(fmt.Sprintf("Alive tsp interval limit exceeded (%dms)", val))
			aliveTsp = 0
			return false
		}

		// alive message is ok
		//   - do nothing, wait for snapshot complete...
		return false

	default:
		// unknown state?
		return false
	}
}

func (p *recoveryProducer) timedOut() error {
	if p.status != uof.ProducerStatusActive {
		// wrong state - not active
		return fmt.Errorf("timeout: producer %s not active", p.producer)
	}

	// producer timed out: set to down
	p.setStatusDown(fmt.Sprint("Alive msg timeout"))
	p.aliveTimestamp = 0
	return nil
}

func (p *recoveryProducer) snapshotComplete(requestID int) error {
	if p.status != uof.ProducerStatusInRecovery {
		// wrong state - not in recovery
		return fmt.Errorf("snapshot: producer %s not in recovery", p.producer)
	}

	if p.recoveryRequestID != requestID {
		// snapshot complete for wrong requestID
		return fmt.Errorf("snapshot: unexpected requestID %d, expected %d, for producer %s", requestID, p.recoveryRequestID, p.producer)
	}

	// snapshot complete: set to active
	p.setStatusActive(fmt.Sprintf("Snapshot complete (ID: %d)", requestID))
	return nil
}

func (p *recoveryProducer) checkAliveInterval(timestamp int) (int, bool) {
	if p.aliveConfiguration.MaxInterval <= 0 || p.aliveTimestamp == 0 {
		return 0, true
	}
	interval := timestamp - p.aliveTimestamp
	return interval, int64(interval) <= p.aliveConfiguration.MaxInterval.Milliseconds()
}

func (p *recoveryProducer) checkAliveDelay(timestamp int) (int, bool) {
	if p.aliveConfiguration.MaxDelay < 0 {
		return 0, true
	}
	delay := uof.CurrentTimestamp() - timestamp
	return delay, int64(delay) <= p.aliveConfiguration.MaxDelay.Milliseconds()
}

func (p *recoveryProducer) resetTimeout() {
	if p.aliveTimeoutCancel != nil {
		p.aliveTimeoutCancel()
		p.aliveTimeoutCancel = nil
	}
	if p.aliveConfiguration.Timeout > 0 {
		alive := make(chan struct{}, 1)
		p.subProcsAdd()
		go func() {
			defer p.subProcsDone()
			select {
			case <-alive:
				return
			case <-time.After(p.aliveConfiguration.Timeout):
				p.aliveTimeoutHandle()
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

func (p *recoveryProducer) cancelTimeout() {
	if p.aliveTimeoutCancel != nil {
		p.aliveTimeoutCancel()
		p.aliveTimeoutCancel = nil
	}
}

type recovery struct {
	api       recoveryAPI
	requestID int
	producers []*recoveryProducer
	timedOut  chan uof.Producer
	errc      chan<- error
	subProcs  *sync.WaitGroup
}

type recoveryAPI interface {
	RequestRecovery(producer uof.Producer, timestamp int, requestID int) error
}

func newRecovery(api recoveryAPI, producers uof.ProducersChange, aliveConfig uof.AliveConfiguration) *recovery {
	r := &recovery{
		api:      api,
		subProcs: &sync.WaitGroup{},
	}
	ct := uof.CurrentTimestamp()
	for _, p := range producers {
		rp := recoveryProducer{
			producer:           p.Producer,
			status:             uof.ProducerStatusDown,
			statusChangedAt:    ct,
			aliveConfiguration: aliveConfig,
			aliveTimestamp:     0,
			recoveryTimestamp:  p.Timestamp,
		}
		rp.aliveTimeoutHandle = func() {
			if r.timedOut != nil {
				r.timedOut <- rp.producer
			}
		}
		rp.subProcsAdd = func() { r.subProcs.Add(1) }
		rp.subProcsDone = func() { r.subProcs.Done() }
		r.producers = append(r.producers, &rp)
	}
	return r
}

func (r *recovery) log(err error, notice bool) {
	uofErr := uof.E
	if notice {
		uofErr = uof.Notice
	}
	select {
	case r.errc <- uofErr("recovery", err):
	default:
	}
}

func (r *recovery) cancelSubProcs() {
	for _, p := range r.producers {
		p.cancelTimeout()
		if c := p.recoveryRequestCancel; c != nil {
			c()
		}
	}
}

func (r *recovery) requestRecovery(p *recoveryProducer) {
	p.setStatusRecovery(r.nextRequestID())

	ctx, cancel := context.WithCancel(context.Background())
	p.recoveryRequestCancel = cancel

	r.subProcs.Add(1)
	go func(producer uof.Producer, timestamp int, requestID int) {
		defer r.subProcs.Done()
		for {
			// recalculate recovery timestamp
			recoveryTsp := adjustRecoveryTsp(timestamp, producer)

			// log operation
			op := fmt.Sprintf("recovery for %s, timestamp: %d, requestID: %d", producer.Code(), recoveryTsp, requestID)
			r.log(fmt.Errorf("starting %s", op), false)

			// request recovery
			err := r.api.RequestRecovery(producer, recoveryTsp, requestID)
			if err == nil {
				return
			}

			// log failed request
			r.log(err, true)

			// wait a minute
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}
		}
	}(p.producer, p.recoveryTimestamp, p.recoveryRequestID)
}

func (r *recovery) nextRequestID() int {
	r.requestID++
	return r.requestID
}

// If producer is back more than recovery window (defined for each producer)
// it has to make full recovery (forced with timestamp = 0).
// Otherwise recovery after timestamp is done.
func adjustRecoveryTsp(timestamp int, producer uof.Producer) int {
	if uof.CurrentTimestamp()-timestamp >= producer.RecoveryWindow() {
		return 0
	}
	return timestamp
}

// handles alive message
func (r *recovery) producerAlive(producer uof.Producer, timestamp int, subscribed int) {
	p := r.find(producer)
	if p == nil {
		return // this is expected we are getting alive for all producers in uof (with Subscribed=0)
	}
	recover := p.alive(timestamp, subscribed)
	if recover {
		r.requestRecovery(p)
	}
}

// handles alive timeout
func (r *recovery) producerTimedOut(producer uof.Producer) {
	p := r.find(producer)
	if p == nil {
		return
	}
	err := p.timedOut()
	if err != nil {
		r.log(err, false)
	}
}

// handles snapshot complete messages
func (r *recovery) snapshotComplete(producer uof.Producer, requestID int) {
	p := r.find(producer)
	if p == nil {
		r.log(fmt.Errorf("snapshot: unexpected producer %s", producer), false)
		return
	}
	err := p.snapshotComplete(requestID)
	if err != nil {
		r.log(err, false)
	}
}

func (r *recovery) find(producer uof.Producer) *recoveryProducer {
	for _, rp := range r.producers {
		if rp.producer == producer {
			return rp
		}
	}
	return nil
}

// start recovery for all producers
func (r *recovery) connectionUp() {
	for _, p := range r.producers {
		if p.status == uof.ProducerStatusDown {
			r.requestRecovery(p)
		}
	}
}

// set status of all producers to down
func (r *recovery) connectionDown() {
	for _, p := range r.producers {
		p.setStatusDown(fmt.Sprint("Connection down"))
		p.aliveTimestamp = 0
	}
}

func (r *recovery) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	r.errc = errc
	r.timedOut = make(chan uof.Producer)
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
				r.producerAlive(m.Alive.Producer, m.Alive.Timestamp, m.Alive.Subscribed)
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
		case p := <-r.timedOut:
			r.producerTimedOut(p)
		}
		if sc := r.statusChangedAt(); sc > statusChangedAt {
			statusChangedAt = sc
			out <- r.producersChangeMessage()
		}
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

func (r *recovery) producersChangeMessage() *uof.Message {
	var psc uof.ProducersChange
	for _, p := range r.producers {
		pc := uof.ProducerChange{
			Producer:  p.producer,
			Status:    p.status,
			Timestamp: p.statusChangedAt,
			Comment:   p.statusChangeComment,
		}
		if p.status == uof.ProducerStatusInRecovery {
			pc.RecoveryID = p.recoveryRequestID
		}
		psc = append(psc, pc)
	}
	return uof.NewProducersChangeMessage(psc)
}

func Recovery(api recoveryAPI, producers uof.ProducersChange, aliveConfig uof.AliveConfiguration) InnerStage {
	r := newRecovery(api, producers, aliveConfig)
	return StageWithSubProcesses(r.loop)
}
