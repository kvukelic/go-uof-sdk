package queue

import (
	"testing"
	"time"

	"github.com/minus5/go-uof-sdk"
	"github.com/stretchr/testify/assert"
)

func TestProducerState(t *testing.T) {
	states := initProducers([]ProducerConfig{{
		producer:    uof.ProducerPrematch,
		timestamp:   0,
		maxInterval: 2000 * time.Millisecond,
		maxDelay:    500 * time.Millisecond,
		timeout:     0,
	}, {
		producer:    uof.ProducerLiveOdds,
		timestamp:   0,
		maxInterval: 2000 * time.Millisecond,
		maxDelay:    500 * time.Millisecond,
		timeout:     0,
	}}, nil)
	assert.Len(t, states, 2)
	assert.Equal(t, uof.ProducerStatusDown, states[0].status)
	assert.Equal(t, uof.ProducerStatusDown, states[1].status)
	assert.Equal(t, 0, states[0].aliveTimestamp)
	assert.Equal(t, 0, states[1].aliveTimestamp)

	// 1. connection up: producers enter recovery
	states[0].connectionUp()
	states[1].connectionUp()
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	assert.Equal(t, 0, states[0].aliveTimestamp)
	assert.Equal(t, 1, states[0].recoveryID)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[1].status)
	assert.Equal(t, 0, states[1].aliveTimestamp)
	assert.Equal(t, 1, states[1].recoveryID)

	// 2. first alive in recovery: set alive timestamps
	ts2 := int(time.Now().UnixNano() / 1e6)
	states[0].alive(ts2, 1)
	states[1].alive(ts2, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	assert.Equal(t, ts2, states[0].aliveTimestamp)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[1].status)
	assert.Equal(t, ts2, states[1].aliveTimestamp)

	// 3. alive message in recovery within interval limit
	ts3 := int(time.Now().Add(1000*time.Millisecond).UnixNano() / 1e6)
	states[0].alive(ts3, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	assert.Equal(t, ts3, states[0].aliveTimestamp)

	// 4. alive message in recovery outside interval limit
	ts4 := int(time.Now().Add(2500*time.Millisecond).UnixNano() / 1e6)
	states[1].alive(ts4, 1)
	assert.Equal(t, uof.ProducerStatusDown, states[1].status)
	assert.Equal(t, 0, states[1].aliveTimestamp)

	// 5. alive message outside delay limit while down
	ts5 := int(time.Now().Add(-1000*time.Millisecond).UnixNano() / 1e6)
	states[1].alive(ts5, 1)
	assert.Equal(t, uof.ProducerStatusDown, states[1].status)
	assert.Equal(t, 0, states[1].aliveTimestamp)

	// 6. alive message within delay limit while down
	ts6 := int(time.Now().Add(-250*time.Millisecond).UnixNano() / 1e6)
	states[1].alive(ts6, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[1].status)
	assert.Equal(t, ts6, states[1].aliveTimestamp)
	assert.Equal(t, 2, states[1].recoveryID)

	// 7. snapshot complete for wrong id
	states[0].snapshotComplete(7)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	assert.Equal(t, ts3, states[0].aliveTimestamp)

	// 8. snapshot complete for correct id
	states[0].snapshotComplete(1)
	assert.Equal(t, uof.ProducerStatusActive, states[0].status)
	assert.Equal(t, ts3, states[0].aliveTimestamp)

	// 9. snapshot complete while not in recovery
	states[0].snapshotComplete(1)
	assert.Equal(t, uof.ProducerStatusActive, states[0].status)
	assert.Equal(t, ts3, states[0].aliveTimestamp)

	// 10. alive message within limits while active
	ts10 := int(time.Now().Add(2000*time.Millisecond).UnixNano() / 1e6)
	states[0].alive(ts10, 1)
	assert.Equal(t, uof.ProducerStatusActive, states[0].status)
	assert.Equal(t, ts10, states[0].aliveTimestamp)

	// 11. alive message with subscribed = 0 while active
	ts11 := int(time.Now().Add(3000*time.Millisecond).UnixNano() / 1e6)
	states[0].alive(ts11, 0)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	assert.Equal(t, ts11, states[0].aliveTimestamp)
	assert.Equal(t, 2, states[0].recoveryID)
	// 11b. snapshot complete
	states[0].snapshotComplete(2)
	assert.Equal(t, uof.ProducerStatusActive, states[0].status)
	assert.Equal(t, ts11, states[0].aliveTimestamp)

	// 12. alive message outside delay limit while active
	ts12 := int(time.Now().Add(-750*time.Millisecond).UnixNano() / 1e6)
	states[0].alive(ts12, 1)
	assert.Equal(t, uof.ProducerStatusDown, states[0].status)
	assert.Equal(t, 0, states[0].aliveTimestamp)
	// 12b. timely alive message, trigger recovery
	ts12b := int(time.Now().UnixNano() / 1e6)
	states[0].alive(ts12b, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	assert.Equal(t, ts12b, states[0].aliveTimestamp)
	assert.Equal(t, 3, states[0].recoveryID)
	// 12c. snapshot complete
	states[0].snapshotComplete(3)
	assert.Equal(t, uof.ProducerStatusActive, states[0].status)
	assert.Equal(t, ts12b, states[0].aliveTimestamp)

	// 13. alive message outside interval limit while active
	ts13 := int(time.Now().Add(2500*time.Millisecond).UnixNano() / 1e6)
	states[0].alive(ts13, 1)
	assert.Equal(t, uof.ProducerStatusDown, states[0].status)
	assert.Equal(t, 0, states[0].aliveTimestamp)
	// 13b. timely alive message, trigger recovery
	ts13b := int(time.Now().UnixNano() / 1e6)
	states[0].alive(ts13b, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	assert.Equal(t, ts13b, states[0].aliveTimestamp)
	assert.Equal(t, 4, states[0].recoveryID)
	// 13c. snapshot complete
	states[0].snapshotComplete(4)
	assert.Equal(t, uof.ProducerStatusActive, states[0].status)
	assert.Equal(t, ts13b, states[0].aliveTimestamp)

	// 14. connection down while active
	states[0].connectionDown()
	assert.Equal(t, uof.ProducerStatusDown, states[0].status)
	assert.Equal(t, 0, states[0].aliveTimestamp)

	// 15. connection down while in recovery
	states[1].connectionDown()
	assert.Equal(t, uof.ProducerStatusDown, states[1].status)
	assert.Equal(t, 0, states[1].aliveTimestamp)
}

func TestProducerTimeout(t *testing.T) {
	timeouts := make(chan uof.Producer, 5)
	states := initProducers([]ProducerConfig{{
		producer:    uof.ProducerPrematch,
		timestamp:   0,
		maxInterval: -1,
		maxDelay:    -1,
		timeout:     100 * time.Millisecond,
	}}, timeouts)

	// turn producer state to active
	assert.Len(t, states, 1)
	assert.Equal(t, uof.ProducerStatusDown, states[0].status)
	states[0].connectionUp()
	assert.Equal(t, uof.ProducerStatusInRecovery, states[0].status)
	states[0].snapshotComplete(1)
	assert.Equal(t, uof.ProducerStatusActive, states[0].status)

	// timeout after producer state turning active
	time.Sleep(125 * time.Millisecond)
	assert.Len(t, timeouts, 1)

	// timeout after previous alive message
	ts := int(time.Now().UnixNano() / 1e6)
	states[0].alive(ts, 1)
	time.Sleep(125 * time.Millisecond)
	assert.Len(t, timeouts, 2)

	// no timeouts
	states[0].alive(ts, 1)
	time.Sleep(75 * time.Millisecond)
	states[0].alive(ts, 1)
	time.Sleep(75 * time.Millisecond)
	states[0].connectionDown()
	time.Sleep(150 * time.Millisecond)
	assert.Len(t, timeouts, 2)

	// check reported timed-out producers
	close(timeouts)
	for p := range timeouts {
		assert.Equal(t, uof.ProducerPrematch, p)
	}
}
