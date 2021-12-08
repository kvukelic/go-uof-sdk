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
	pc := &uof.ProducerChange{
		Producer:          uof.ProducerLiveOdds,
		RecoveryTimestamp: cs,
	}
	assert.Equal(t, pc.RecoveryTimestamp, recoveryTimestamp(pc.RecoveryTimestamp, pc.Producer))
	pc.RecoveryTimestamp = cs - pc.Producer.RecoveryWindow() + 100
	assert.Equal(t, pc.RecoveryTimestamp, recoveryTimestamp(pc.RecoveryTimestamp, pc.Producer))
	pc.RecoveryTimestamp = cs - pc.Producer.RecoveryWindow() - 1
	assert.Equal(t, int(0), recoveryTimestamp(pc.RecoveryTimestamp, pc.Producer))
}

func TestRecovery(t *testing.T) {
	m := &recoveryAPIMock{calls: make(chan requestRecoveryParams, 16)}
	r := newRecovery(m)
	in := make(chan *uof.Message)
	out := make(chan *uof.Message, 16)
	errc := make(chan error, 16)
	go r.loop(in, out, errc)

	tsp := uof.CurrentTimestamp() - 10000
	pc1 := uof.ProducerChange{
		Producer:          uof.ProducerPrematch,
		Status:            uof.ProducerStatusInRecovery,
		RecoveryID:        1,
		RecoveryTimestamp: tsp,
	}
	pc2 := uof.ProducerChange{
		Producer:          uof.ProducerLiveOdds,
		Status:            uof.ProducerStatusInRecovery,
		RecoveryID:        2,
		RecoveryTimestamp: tsp,
	}
	psc := uof.ProducersChange{pc1, pc2}
	in <- uof.NewProducersChangeMessage(psc)
	time.Sleep(10 * time.Millisecond)

	assert.Contains(t, r.producers, uof.ProducerPrematch)
	assert.Contains(t, r.producers, uof.ProducerLiveOdds)
	assert.Equal(t, uof.ProducerStatusInRecovery, r.producers[uof.ProducerPrematch])
	assert.Equal(t, uof.ProducerStatusInRecovery, r.producers[uof.ProducerLiveOdds])
	assert.Equal(t, 2, len(m.calls))
	req1 := <-m.calls
	req2 := <-m.calls
	if req1.producer != uof.ProducerPrematch {
		req1, req2 = req2, req1
	}
	assert.Equal(t, uof.ProducerPrematch, req1.producer)
	assert.Equal(t, tsp, req1.timestamp)
	assert.Equal(t, 1, req1.requestID)
	assert.Equal(t, uof.ProducerLiveOdds, req2.producer)
	assert.Equal(t, tsp, req2.timestamp)
	assert.Equal(t, 2, req2.requestID)

	pc2.Status = uof.ProducerStatusActive
	psc = uof.ProducersChange{pc1, pc2}
	in <- uof.NewProducersChangeMessage(psc)
	time.Sleep(10 * time.Millisecond)

	assert.Contains(t, r.producers, uof.ProducerPrematch)
	assert.Contains(t, r.producers, uof.ProducerLiveOdds)
	assert.Equal(t, uof.ProducerStatusInRecovery, r.producers[uof.ProducerPrematch])
	assert.Equal(t, uof.ProducerStatusActive, r.producers[uof.ProducerLiveOdds])
	assert.Equal(t, 0, len(m.calls))

	pc2.Status = uof.ProducerStatusInRecovery
	pc2.RecoveryTimestamp = tsp + 5000
	pc2.RecoveryID = 3
	psc = uof.ProducersChange{pc1, pc2}
	in <- uof.NewProducersChangeMessage(psc)
	time.Sleep(10 * time.Millisecond)

	assert.Contains(t, r.producers, uof.ProducerPrematch)
	assert.Contains(t, r.producers, uof.ProducerLiveOdds)
	assert.Equal(t, uof.ProducerStatusInRecovery, r.producers[uof.ProducerPrematch])
	assert.Equal(t, uof.ProducerStatusInRecovery, r.producers[uof.ProducerLiveOdds])
	assert.Equal(t, 1, len(m.calls))
	req := <-m.calls
	assert.Equal(t, uof.ProducerLiveOdds, req.producer)
	assert.Equal(t, tsp+5000, req.timestamp)
	assert.Equal(t, 3, req.requestID)
}
