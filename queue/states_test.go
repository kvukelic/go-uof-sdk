package queue

import (
	"testing"
	"time"

	"github.com/minus5/go-uof-sdk"
	"github.com/stretchr/testify/assert"
)

func TestProducerState(t *testing.T) {
	state := newProducersState([]ProducerConfig{{
		producer:      uof.ProducerPrematch,
		startingTsp:   0,
		aliveDiffMax:  2000 * time.Millisecond,
		aliveDelayMax: 1000 * time.Millisecond,
		aliveDelayOK:  500 * time.Millisecond,
		aliveTimeout:  0,
	}, {
		producer:      uof.ProducerLiveOdds,
		startingTsp:   0,
		aliveDiffMax:  2000 * time.Millisecond,
		aliveDelayMax: 1000 * time.Millisecond,
		aliveDelayOK:  500 * time.Millisecond,
		aliveTimeout:  0,
	}})
	assert.Len(t, state, 2)
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerPrematch].status)
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerLiveOdds].status)
	assert.Equal(t, 0, state[uof.ProducerPrematch].aliveTsp)
	assert.Equal(t, 0, state[uof.ProducerLiveOdds].aliveTsp)

	// 1. connection up: producers enter recovery
	state.connectionUp()
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, 0, state[uof.ProducerPrematch].aliveTsp)
	assert.Equal(t, 1, state[uof.ProducerPrematch].recoveryID)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerLiveOdds].status)
	assert.Equal(t, 0, state[uof.ProducerLiveOdds].aliveTsp)
	assert.Equal(t, 1, state[uof.ProducerLiveOdds].recoveryID)

	// 2. first alive in recovery: set alive timestamps
	ts2 := int(time.Now().UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts2, 1)
	state.alive(uof.ProducerLiveOdds, ts2, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts2, state[uof.ProducerPrematch].aliveTsp)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerLiveOdds].status)
	assert.Equal(t, ts2, state[uof.ProducerLiveOdds].aliveTsp)

	// 3. alive message in recovery within timestamp diff limit
	ts3 := int(time.Now().Add(1000*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts3, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts3, state[uof.ProducerPrematch].aliveTsp)

	// 4. alive message in recovery outside timestamp diff limit
	ts4 := int(time.Now().Add(2500*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerLiveOdds, ts4, 1)
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerLiveOdds].status)
	assert.Equal(t, 0, state[uof.ProducerLiveOdds].aliveTsp)

	// 5. alive message outside acceptable delay limit while down
	ts5 := int(time.Now().Add(-750*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerLiveOdds, ts5, 1)
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerLiveOdds].status)
	assert.Equal(t, 0, state[uof.ProducerLiveOdds].aliveTsp)

	// 6. alive message within acceptable delay limit while down
	ts6 := int(time.Now().Add(-250*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerLiveOdds, ts6, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerLiveOdds].status)
	assert.Equal(t, ts6, state[uof.ProducerLiveOdds].aliveTsp)
	assert.Equal(t, 2, state[uof.ProducerLiveOdds].recoveryID)

	// 7. snapshot complete for wrong id
	ts7 := int(time.Now().UnixNano() / 1e6)
	state.snapshotComplete(uof.ProducerPrematch, ts7, 7)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts3, state[uof.ProducerPrematch].aliveTsp)

	// 8. snapshot complete for correct id outside acceptable delay
	ts8 := int(time.Now().Add(-750*time.Millisecond).UnixNano() / 1e6)
	state.snapshotComplete(uof.ProducerPrematch, ts8, 1)
	assert.Equal(t, uof.ProducerStatusPostRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts3, state[uof.ProducerPrematch].aliveTsp)

	// 9. alive message outside acceptable delay limit while in post-recovery
	ts9 := int(time.Now().Add(-750*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts9, 1)
	assert.Equal(t, uof.ProducerStatusPostRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts9, state[uof.ProducerPrematch].aliveTsp)

	// 10. alive message within acceptable delay limit while in post-recovery
	ts10 := int(time.Now().Add(-250*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts10, 1)
	assert.Equal(t, uof.ProducerStatusActive, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts10, state[uof.ProducerPrematch].aliveTsp)

	// 11. snapshot complete while not in recovery
	ts11 := int(time.Now().UnixNano() / 1e6)
	state.snapshotComplete(uof.ProducerPrematch, ts11, 1)
	assert.Equal(t, uof.ProducerStatusActive, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts10, state[uof.ProducerPrematch].aliveTsp)

	// 12. alive message within limits while active
	ts12 := int(time.Now().Add(1500*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts12, 1)
	assert.Equal(t, uof.ProducerStatusActive, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts12, state[uof.ProducerPrematch].aliveTsp)

	// 13. alive message with subscribed = 0 while active
	tsts13 := int(time.Now().Add(3000*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, tsts13, 0)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, tsts13, state[uof.ProducerPrematch].aliveTsp)
	assert.Equal(t, 2, state[uof.ProducerPrematch].recoveryID)

	// 14. snapshot complete for correct id within acceptable delay
	ts14 := int(time.Now().UnixNano() / 1e6)
	state.snapshotComplete(uof.ProducerPrematch, ts14, 2)
	assert.Equal(t, uof.ProducerStatusActive, state[uof.ProducerPrematch].status)
	assert.Equal(t, tsts13, state[uof.ProducerPrematch].aliveTsp)

	// 14. alive message outside maximum delay limit while active
	ts15 := int(time.Now().Add(-1500*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts15, 1)
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerPrematch].status)
	assert.Equal(t, 0, state[uof.ProducerPrematch].aliveTsp)
	// 15b. alive with acceptable delay, trigger recovery
	ts15b := int(time.Now().UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts15b, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts15b, state[uof.ProducerPrematch].aliveTsp)
	assert.Equal(t, 3, state[uof.ProducerPrematch].recoveryID)
	// 15c. snapshot complete for correct id within acceptable delay
	ts15c := int(time.Now().UnixNano() / 1e6)
	state.snapshotComplete(uof.ProducerPrematch, ts15c, 3)
	assert.Equal(t, uof.ProducerStatusActive, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts15b, state[uof.ProducerPrematch].aliveTsp)

	// 16. alive message outside timestamp diff limit while active
	ts16 := int(time.Now().Add(2500*time.Millisecond).UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts16, 1)
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerPrematch].status)
	assert.Equal(t, 0, state[uof.ProducerPrematch].aliveTsp)
	// 16b. alive with acceptable delay, trigger recovery
	ts16b := int(time.Now().UnixNano() / 1e6)
	state.alive(uof.ProducerPrematch, ts16b, 1)
	assert.Equal(t, uof.ProducerStatusInRecovery, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts16b, state[uof.ProducerPrematch].aliveTsp)
	assert.Equal(t, 4, state[uof.ProducerPrematch].recoveryID)
	// 16c. snapshot complete for correct id within acceptable delay
	ts16c := int(time.Now().UnixNano() / 1e6)
	state.snapshotComplete(uof.ProducerPrematch, ts16c, 4)
	assert.Equal(t, uof.ProducerStatusActive, state[uof.ProducerPrematch].status)
	assert.Equal(t, ts16b, state[uof.ProducerPrematch].aliveTsp)

	// 17. connection down
	state.connectionDown()
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerPrematch].status)
	assert.Equal(t, 0, state[uof.ProducerPrematch].aliveTsp)
	assert.Equal(t, uof.ProducerStatusDown, state[uof.ProducerLiveOdds].status)
	assert.Equal(t, 0, state[uof.ProducerLiveOdds].aliveTsp)
}
