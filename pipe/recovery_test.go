package pipe

import (
	"testing"
	"time"

	"github.com/minus5/go-uof-sdk"
	"github.com/stretchr/testify/assert"
)

type requestRecoveryParams struct {
	producer  uof.Producer
	timestamp int
	requestID int
}

type recoveryAPIMock struct {
	calls chan requestRecoveryParams
}

func (m *recoveryAPIMock) RequestRecovery(producer uof.Producer, timestamp int, requestID int) error {
	m.calls <- requestRecoveryParams{
		producer:  producer,
		timestamp: timestamp,
		requestID: requestID,
	}
	return nil
}

func TestRecoveryTimestamp(t *testing.T) {
	cs := uof.CurrentTimestamp()
	rp := &recoveryProducer{
		producer:          uof.ProducerLiveOdds,
		recoveryTimestamp: cs,
	}
	assert.Equal(t, cs, adjustRecoveryTsp(rp.recoveryTimestamp, rp.producer))
	rp.recoveryTimestamp = cs - rp.producer.RecoveryWindow() + 10
	assert.Equal(t, rp.recoveryTimestamp, adjustRecoveryTsp(rp.recoveryTimestamp, rp.producer))
	rp.recoveryTimestamp = cs - rp.producer.RecoveryWindow()
	assert.Equal(t, int(0), adjustRecoveryTsp(rp.recoveryTimestamp, rp.producer))
}

func TestRecoveryStateMachine(t *testing.T) {
	// setup
	timestamp := uof.CurrentTimestamp() - 10*1000
	var ps uof.ProducersChange
	ps.Add(uof.ProducerPrematch, timestamp)
	ps.Add(uof.ProducerLiveOdds, timestamp+1)
	m := &recoveryAPIMock{calls: make(chan requestRecoveryParams, 16)}
	r := newRecovery(m, ps, uof.DefaultAliveConfiguration())

	// 0. initilay all producers are down
	for _, p := range r.producers {
		assert.Equal(t, uof.ProducerStatusDown, p.status)
	}

	// 1. connection up, triggers recovery requests for all producers
	r.connectionUp()
	// all produers status are changed to in recovery
	for _, p := range r.producers {
		assert.Equal(t, uof.ProducerStatusInRecovery, p.status)
	}
	// two recovery requests are sent
	recoveryRequestPrematch := <-m.calls
	recoveryRequestLive := <-m.calls
	if recoveryRequestPrematch.producer == uof.ProducerLiveOdds {
		// reorder because order is not guaranteed (called in goroutines)
		recoveryRequestPrematch, recoveryRequestLive = recoveryRequestLive, recoveryRequestPrematch
	}
	// prematch producer request
	assert.Equal(t, uof.ProducerPrematch, recoveryRequestPrematch.producer)
	assert.Equal(t, timestamp, recoveryRequestPrematch.timestamp)
	prematch := r.find(uof.ProducerPrematch)
	assert.Equal(t, prematch.recoveryRequestID, recoveryRequestPrematch.requestID)
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)
	// live producer request
	assert.Equal(t, uof.ProducerLiveOdds, recoveryRequestLive.producer)
	assert.Equal(t, timestamp+1, recoveryRequestLive.timestamp)
	live := r.find(uof.ProducerLiveOdds)
	assert.Equal(t, live.recoveryRequestID, recoveryRequestLive.requestID)
	assert.Equal(t, uof.ProducerStatusInRecovery, live.status)

	// 2. snapshot complete, changes status to active for that producer
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)
	r.snapshotComplete(uof.ProducerPrematch, recoveryRequestPrematch.requestID)
	assert.Equal(t, uof.ProducerStatusActive, prematch.status)

	// 3. on alive, updates timestamps for that producer
	r.producerAlive(uof.ProducerPrematch, timestamp+2, 1)
	assert.Equal(t, timestamp+2, prematch.aliveTimestamp)
	assert.Equal(t, timestamp+2, prematch.recoveryTimestamp)
	assert.Equal(t, 0, live.aliveTimestamp)
	assert.Equal(t, timestamp+1, live.recoveryTimestamp)

	// 4. on alive with subscribed = 0, forces recovery request for that producer
	// changes status to in recovery until snapshot complete
	assert.Equal(t, uof.ProducerStatusActive, prematch.status)
	r.producerAlive(uof.ProducerPrematch, timestamp+3, 0)
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)
	recoveryRequestPrematch = <-m.calls
	assert.Equal(t, uof.ProducerPrematch, recoveryRequestPrematch.producer)
	assert.Equal(t, timestamp+2, recoveryRequestPrematch.timestamp)
	assert.Equal(t, prematch.recoveryRequestID, recoveryRequestPrematch.requestID)

	// 5. snapshot complete, changes status to active for that producer
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)
	r.snapshotComplete(uof.ProducerPrematch, recoveryRequestPrematch.requestID)
	assert.Equal(t, uof.ProducerStatusActive, prematch.status)

	// 6. connection down, retruns to the start of the cycle
	r.connectionDown()
	for _, p := range r.producers {
		assert.Equal(t, uof.ProducerStatusDown, p.status)
	}
}

func TestRecoveryRequests(t *testing.T) {
	timestamp := uof.CurrentTimestamp() - 10*1000
	var ps uof.ProducersChange
	ps.Add(uof.ProducerPrematch, timestamp)
	ps.Add(uof.ProducerLiveOdds, timestamp+1)

	m := &recoveryAPIMock{calls: make(chan requestRecoveryParams, 16)}
	r := newRecovery(m, ps, uof.DefaultAliveConfiguration())
	in := make(chan *uof.Message)
	out := make(chan *uof.Message, 16)
	errc := make(chan error, 16)
	go r.loop(in, out, errc)

	// 1. connection status triggers recovery requests
	in <- uof.NewConnnectionMessage(uof.ConnectionStatusUp)
	recoveryRequestPrematch := <-m.calls
	recoveryRequestLive := <-m.calls
	if recoveryRequestPrematch.producer == uof.ProducerLiveOdds {
		// order is not guaranteed
		recoveryRequestPrematch, recoveryRequestLive = recoveryRequestLive, recoveryRequestPrematch
	}
	assert.Equal(t, uof.ProducerPrematch, recoveryRequestPrematch.producer)
	assert.Equal(t, timestamp, recoveryRequestPrematch.timestamp)
	prematch := r.find(uof.ProducerPrematch)
	assert.Equal(t, prematch.recoveryRequestID, recoveryRequestPrematch.requestID)
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)

	assert.Equal(t, uof.ProducerLiveOdds, recoveryRequestLive.producer)
	assert.Equal(t, timestamp+1, recoveryRequestLive.timestamp)
	live := r.find(uof.ProducerLiveOdds)
	assert.Equal(t, live.recoveryRequestID, recoveryRequestLive.requestID)
	assert.Equal(t, uof.ProducerStatusInRecovery, live.status)
	<-out // skip connection status message
	producersChangeMessage := <-out
	// check out message
	assert.Equal(t, uof.MessageTypeProducersChange, producersChangeMessage.Type)
	assert.Equal(t, uof.MessageScopeSystem, producersChangeMessage.Scope)
	assert.Equal(t, uof.ProducerPrematch, producersChangeMessage.Producers[0].Producer)
	assert.Equal(t, uof.ProducerStatusInRecovery, producersChangeMessage.Producers[0].Status)
	assert.Equal(t, uof.ProducerLiveOdds, producersChangeMessage.Producers[1].Producer)
	assert.Equal(t, uof.ProducerStatusInRecovery, producersChangeMessage.Producers[1].Status)

	// 2. snapshot complete for the prematch is received
	in <- &uof.Message{
		Header: uof.Header{Type: uof.MessageTypeSnapshotComplete},
		Body: uof.Body{SnapshotComplete: &uof.SnapshotComplete{
			Producer:  uof.ProducerPrematch,
			RequestID: recoveryRequestPrematch.requestID},
		},
	}
	<-out //snapshot complete
	producersChangeMessage = <-out
	// status of the prematch is changed to the active
	assert.Equal(t, prematch.recoveryRequestID, 0)
	assert.Equal(t, uof.ProducerStatusActive, prematch.status)
	assert.Equal(t, live.recoveryRequestID, recoveryRequestLive.requestID)
	assert.Equal(t, uof.ProducerStatusInRecovery, live.status)
	// check out message
	assert.Equal(t, uof.ProducerPrematch, producersChangeMessage.Producers[0].Producer)
	assert.Equal(t, uof.ProducerStatusActive, producersChangeMessage.Producers[0].Status)
	assert.Equal(t, uof.ProducerLiveOdds, producersChangeMessage.Producers[1].Producer)
	assert.Equal(t, uof.ProducerStatusInRecovery, producersChangeMessage.Producers[1].Status)

	// 3. alive message
	in <- &uof.Message{
		Header: uof.Header{Type: uof.MessageTypeAlive},
		Body: uof.Body{Alive: &uof.Alive{
			Producer:   uof.ProducerPrematch,
			Timestamp:  timestamp + 2,
			Subscribed: 1,
		},
		},
	}

	// 4. alive with subscribed=0 triggers recovery request
	in <- &uof.Message{
		Header: uof.Header{Type: uof.MessageTypeAlive},
		Body: uof.Body{Alive: &uof.Alive{
			Producer:   uof.ProducerPrematch,
			Timestamp:  timestamp + 3,
			Subscribed: 0,
		},
		},
	}
	recoveryRequestPrematch = <-m.calls
	assert.Equal(t, uof.ProducerPrematch, recoveryRequestPrematch.producer)
	<-out //alive messages
	<-out
	producersChangeMessage = <-out
	// check out message, both producers are again in recovery
	assert.Equal(t, uof.ProducerPrematch, producersChangeMessage.Producers[0].Producer)
	assert.Equal(t, uof.ProducerStatusInRecovery, producersChangeMessage.Producers[0].Status)
	assert.Equal(t, uof.ProducerLiveOdds, producersChangeMessage.Producers[1].Producer)
	assert.Equal(t, uof.ProducerStatusInRecovery, producersChangeMessage.Producers[1].Status)
}

func TestAliveMaxDelay(t *testing.T) {
	timestamp := uof.CurrentTimestamp() - 10000
	var ps uof.ProducersChange
	ps.Add(uof.ProducerPrematch, timestamp)
	ps.Add(uof.ProducerLiveOdds, timestamp+1)
	m := &recoveryAPIMock{calls: make(chan requestRecoveryParams, 16)}
	ac := uof.DefaultAliveConfiguration()
	ac.MaxDelay = 500 * time.Millisecond
	r := newRecovery(m, ps, ac)

	// producer in status down
	prematch := r.find(uof.ProducerPrematch)
	prematch.setStatus(uof.ProducerStatusDown, "")
	assert.Equal(t, 0, prematch.aliveTimestamp)
	assert.Equal(t, timestamp, prematch.recoveryTimestamp)
	// late alive: stay down, update timestamp
	aliveTs1 := uof.CurrentTimestamp() - 1000
	r.producerAlive(uof.ProducerPrematch, aliveTs1, 1)
	assert.Equal(t, uof.ProducerStatusDown, prematch.status)
	assert.Equal(t, aliveTs1, prematch.aliveTimestamp)
	assert.Equal(t, timestamp, prematch.recoveryTimestamp)
	// timely alive: initiate recovery, update timestamp
	aliveTs2 := uof.CurrentTimestamp() - 100
	r.producerAlive(uof.ProducerPrematch, aliveTs2, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)
	assert.Equal(t, aliveTs2, prematch.aliveTimestamp)
	assert.Equal(t, timestamp, prematch.recoveryTimestamp)

	// producer in status active
	live := r.find(uof.ProducerLiveOdds)
	live.setStatus(uof.ProducerStatusActive, "")
	assert.Equal(t, timestamp+1, live.recoveryTimestamp)
	assert.Equal(t, 0, live.aliveTimestamp)
	// late alive: go down, update timestamp
	aliveTs3 := uof.CurrentTimestamp() - 1000
	r.producerAlive(uof.ProducerLiveOdds, aliveTs3, 1)
	assert.Equal(t, uof.ProducerStatusDown, live.status)
	assert.Equal(t, aliveTs3, live.aliveTimestamp)
	assert.Equal(t, timestamp+1, live.recoveryTimestamp)
	// timely alive: initiate recovery, update timestamp
	aliveTs4 := uof.CurrentTimestamp() - 100
	r.producerAlive(uof.ProducerLiveOdds, aliveTs4, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, live.status)
	assert.Equal(t, aliveTs4, live.aliveTimestamp)
	assert.Equal(t, timestamp+1, live.recoveryTimestamp)
}

func TestAliveMaxInterval(t *testing.T) {
	timestamp := uof.CurrentTimestamp() - 20000
	var ps uof.ProducersChange
	ps.Add(uof.ProducerPrematch, timestamp)
	ps.Add(uof.ProducerLiveOdds, timestamp+1)
	m := &recoveryAPIMock{calls: make(chan requestRecoveryParams, 16)}
	ac := uof.DefaultAliveConfiguration()
	ac.MaxInterval = 5000 * time.Millisecond
	r := newRecovery(m, ps, ac)

	// producer in status down - no interval checks
	prematch := r.find(uof.ProducerPrematch)
	prematch.setStatus(uof.ProducerStatusDown, "")
	assert.Equal(t, 0, prematch.aliveTimestamp)
	assert.Equal(t, timestamp, prematch.recoveryTimestamp)
	// initial alive: initate recovery
	aliveTs1 := uof.CurrentTimestamp() - 12000
	r.producerAlive(uof.ProducerPrematch, aliveTs1, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)
	assert.Equal(t, aliveTs1, prematch.aliveTimestamp)
	assert.Equal(t, timestamp, prematch.recoveryTimestamp)
	// reset to status down
	prematch.setStatus(uof.ProducerStatusDown, "")
	// next alive; initiate recovery
	aliveTs2 := uof.CurrentTimestamp() - 6000
	r.producerAlive(uof.ProducerPrematch, aliveTs2, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, prematch.status)
	assert.Equal(t, aliveTs2, prematch.aliveTimestamp)
	assert.Equal(t, timestamp, prematch.recoveryTimestamp)

	// producer in status active
	live := r.find(uof.ProducerLiveOdds)
	live.setStatus(uof.ProducerStatusActive, "")
	assert.Equal(t, 0, live.aliveTimestamp)
	assert.Equal(t, timestamp+1, live.recoveryTimestamp)
	// initial alive: update timestamp and recovery timestamp
	aliveTs3 := uof.CurrentTimestamp() - 17000
	r.producerAlive(uof.ProducerLiveOdds, aliveTs3, 1)
	assert.Equal(t, uof.ProducerStatusActive, live.status)
	assert.Equal(t, aliveTs3, live.aliveTimestamp)
	assert.Equal(t, aliveTs3, live.recoveryTimestamp)
	// delay exceeded: go down, update timestamp
	aliveTs4 := uof.CurrentTimestamp() - 10000
	r.producerAlive(uof.ProducerLiveOdds, aliveTs4, 1)
	assert.Equal(t, uof.ProducerStatusDown, live.status)
	assert.Equal(t, aliveTs4, live.aliveTimestamp)
	assert.Equal(t, aliveTs3, live.recoveryTimestamp)
	// delay ok: initiate recovery, update timestamp
	aliveTs5 := uof.CurrentTimestamp() - 7000
	r.producerAlive(uof.ProducerLiveOdds, aliveTs5, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, live.status)
	assert.Equal(t, aliveTs5, live.aliveTimestamp)
	assert.Equal(t, aliveTs3, live.recoveryTimestamp)
}

func TestAliveTimeout(t *testing.T) {
	timestamp := uof.CurrentTimestamp() - 10000
	var ps uof.ProducersChange
	ps.Add(uof.ProducerPrematch, timestamp)
	ps.Add(uof.ProducerLiveOdds, timestamp+1)
	m := &recoveryAPIMock{calls: make(chan requestRecoveryParams, 16)}
	ac := uof.DefaultAliveConfiguration()
	ac.Timeout = 150 * time.Millisecond
	r := newRecovery(m, ps, ac)
	r.timedOut = make(chan uof.Producer, 16)

	prematch := r.find(uof.ProducerPrematch)
	prematch.setStatus(uof.ProducerStatusActive, "")
	r.producerAlive(uof.ProducerPrematch, uof.CurrentTimestamp(), 1)
	assert.Equal(t, 0, len(r.timedOut))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, len(r.timedOut))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(r.timedOut))
	p := <-r.timedOut
	assert.Equal(t, uof.ProducerPrematch, p)

	live := r.find(uof.ProducerLiveOdds)
	live.setStatus(uof.ProducerStatusActive, "")
	r.producerAlive(uof.ProducerLiveOdds, uof.CurrentTimestamp(), 1)
	assert.Equal(t, 0, len(r.timedOut))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, len(r.timedOut))
	r.producerAlive(uof.ProducerLiveOdds, uof.CurrentTimestamp(), 1)
	assert.Equal(t, 0, len(r.timedOut))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, len(r.timedOut))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(r.timedOut))
	p = <-r.timedOut
	assert.Equal(t, uof.ProducerLiveOdds, p)
}
