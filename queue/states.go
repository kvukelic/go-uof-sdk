package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/minus5/go-uof-sdk"
)

func WithProducerStates(ctx context.Context, conn *Connection, config []ProducerConfig) func() (<-chan *uof.Message, <-chan error) {
	tcs := make([]TimeoutConfig, 0)
	for _, c := range config {
		tc := NewTimeoutConfig(c.producer)
		tc.SetDuration(c.aliveTimeout)
		tcs = append(tcs, tc)
	}
	return func() (<-chan *uof.Message, <-chan error) {
		_out, _errc := WithTimeouts(ctx, conn, tcs)()
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
			state := newProducersState(config)
			lastChange := 0
			for msg := range _out {
				out <- msg
				switch msg.Type {
				case uof.MessageTypeConnection:
					switch msg.Connection.Status {
					case uof.ConnectionStatusUp:
						state.connectionUp()
					case uof.ConnectionStatusDown:
						state.connectionDown()
					}
				case uof.MessageTypeSnapshotComplete:
					state.snapshotComplete(msg.Producer, msg.SnapshotComplete.Timestamp, msg.SnapshotComplete.RequestID)
				case uof.MessageTypeAlive:
					state.alive(msg.Producer, msg.Alive.Timestamp, msg.Alive.Subscribed)
				case uof.MessageTypeAliveTimeout:
					state.aliveTimeout(msg.Producer)
				}
				if newTsp := state.lastUpdate(); newTsp > lastChange {
					lastChange = newTsp
					psc := make(uof.ProducersChange, 0)
					for p, ps := range state {
						psc = append(psc, uof.ProducerChange{
							Producer:          p,
							Status:            ps.status,
							Timestamp:         ps.statusTsp,
							RecoveryID:        ps.recoveryID,
							RecoveryTimestamp: ps.recoveryTsp,
							Comment:           ps.statusMsg,
						})
					}
					out <- uof.NewProducersChangeMessage(psc)
				}
			}
		}()

		return out, errc
	}
}

// CONFIG

type ProducerConfig struct {
	producer      uof.Producer
	startingTsp   int
	aliveDiffMax  time.Duration
	aliveDelayMax time.Duration
	aliveDelayOK  time.Duration
	aliveTimeout  time.Duration
}

func NewProducerConfig(producer uof.Producer) ProducerConfig {
	return ProducerConfig{
		producer:      producer,
		startingTsp:   uof.CurrentTimestamp(),
		aliveDiffMax:  -1,
		aliveDelayMax: -1,
		aliveDelayOK:  -1,
		aliveTimeout:  -1,
	}
}

func (pc *ProducerConfig) SetInitialRecoveryTimestamp(tsp int) {
	pc.startingTsp = tsp
}

func (pc *ProducerConfig) SetAliveTimestampDiffLimit(max time.Duration) {
	pc.aliveDiffMax = max
}

func (pc *ProducerConfig) SetAliveMessageDelayLimits(accept, max time.Duration) error {
	if accept >= 0 && max >= 0 && accept > max {
		return fmt.Errorf("Invalid alive message delay limit configuration")
	}
	pc.aliveDelayOK = accept
	pc.aliveDelayMax = max
	return nil
}

func (pc *ProducerConfig) SetAliveTimeoutDuration(timeout time.Duration) {
	pc.aliveTimeout = timeout
}

// STATE

type producersState map[uof.Producer]*producerState

type producerState struct {
	status        uof.ProducerStatus
	statusTsp     int
	statusMsg     string
	aliveTsp      int
	aliveDiffMax  time.Duration
	aliveDelayMax time.Duration
	aliveDelayOK  time.Duration
	recoveryTsp   int
	recoveryID    int
}

func (ps *producerState) setStatus(st uof.ProducerStatus, msg string) {
	if ps.status != st {
		ps.status = st
		ps.statusTsp = uof.CurrentTimestamp()
		ps.statusMsg = fmt.Sprintf("%s. Status set to [%s].", msg, st.String())
	}
}

func newProducersState(config []ProducerConfig) producersState {
	state := make(producersState)
	for _, c := range config {
		state[c.producer] = &producerState{
			status:        uof.ProducerStatusDown,
			statusTsp:     uof.CurrentTimestamp(),
			statusMsg:     "Initialized.",
			aliveTsp:      0,
			aliveDiffMax:  c.aliveDiffMax,
			aliveDelayMax: c.aliveDelayMax,
			aliveDelayOK:  c.aliveDelayOK,
			recoveryTsp:   c.startingTsp,
			recoveryID:    0,
		}
	}
	return state
}

func (pss producersState) lastUpdate() int {
	lastUpdate := 0
	for _, ps := range pss {
		if ps.statusTsp > lastUpdate {
			lastUpdate = ps.statusTsp
		}
	}
	return lastUpdate
}

func (pss producersState) connectionUp() {
	for _, ps := range pss {
		if ps.status == uof.ProducerStatusDown {
			ps.recoveryID++
			ps.setStatus(uof.ProducerStatusInRecovery, fmt.Sprintf("Connection up [request ID: %d]", ps.recoveryID))
		}
	}
}

func (pss producersState) connectionDown() {
	for _, ps := range pss {
		ps.aliveTsp = 0
		ps.setStatus(uof.ProducerStatusDown, "Connection down")
	}
}

func (pss producersState) aliveTimeout(p uof.Producer) error {
	if ps, ok := pss[p]; ok {
		if ps.status != uof.ProducerStatusActive {
			return fmt.Errorf("Producer %s not active", p.String())
		}
		ps.aliveTsp = 0
		ps.setStatus(uof.ProducerStatusDown, "Alive message timed out")
	}
	return nil
}

func (pss producersState) snapshotComplete(p uof.Producer, tsp, id int) error {
	if ps, ok := pss[p]; ok {
		if ps.status != uof.ProducerStatusInRecovery {
			return fmt.Errorf("Producer %s not in recovery", p.String())
		}
		if ps.recoveryID != id {
			return fmt.Errorf("Wrong requestID (got %d, expected %d) for producer %s", id, ps.recoveryID, p.String())
		}
		if ok := ps.aliveDelayAccept(tsp); ok {
			ps.setStatus(uof.ProducerStatusActive, fmt.Sprintf("Snapshot complete [request ID: %d]", id))
		} else {
			ps.setStatus(uof.ProducerStatusPostRecovery, fmt.Sprintf("Snapshot complete [request ID: %d]", id))
		}
	}
	return nil
}

func (pss producersState) alive(p uof.Producer, tsp int, sub int) {
	if ps, ok := pss[p]; ok {
		switch ps.status {
		case uof.ProducerStatusActive:
			if delay, ok := ps.aliveDelayCheck(tsp); !ok {
				ps.aliveTsp = 0
				ps.setStatus(uof.ProducerStatusDown, fmt.Sprintf("Alive message max delay exceeded (%dms)", delay))
			} else if diff, ok := ps.aliveDiffCheck(tsp); !ok {
				ps.aliveTsp = 0
				ps.setStatus(uof.ProducerStatusDown, fmt.Sprintf("Alive message max diff exceeded (%dms)", diff))
			} else if sub == 0 {
				ps.aliveTsp = tsp
				ps.recoveryID++
				ps.setStatus(uof.ProducerStatusInRecovery, fmt.Sprintf("Producer unsubscribed, starting recovery [request ID: %d]", ps.recoveryID))
			} else {
				ps.aliveTsp = tsp
				ps.recoveryTsp = tsp
			}
			return
		case uof.ProducerStatusPostRecovery:
			if diff, ok := ps.aliveDiffCheck(tsp); !ok {
				ps.aliveTsp = 0
				ps.setStatus(uof.ProducerStatusDown, fmt.Sprintf("Alive message max diff exceeded (%dms)", diff))
			} else if ok := ps.aliveDelayAccept(tsp); ok {
				ps.aliveTsp = tsp
				ps.setStatus(uof.ProducerStatusActive, fmt.Sprintf("Alive message delay OK, recovery complete [request ID: %d]", ps.recoveryID))
			} else {
				ps.aliveTsp = tsp
			}
		case uof.ProducerStatusInRecovery:
			if diff, ok := ps.aliveDiffCheck(tsp); !ok {
				ps.aliveTsp = 0
				ps.setStatus(uof.ProducerStatusDown, fmt.Sprintf("Alive message max diff exceeded (%dms)", diff))
			} else {
				ps.aliveTsp = tsp
			}
			return
		case uof.ProducerStatusDown:
			if ok := ps.aliveDelayAccept(tsp); ok {
				ps.aliveTsp = tsp
				ps.recoveryID++
				ps.setStatus(uof.ProducerStatusInRecovery, fmt.Sprintf("Alive message delay OK, starting recovery [request ID: %d]", ps.recoveryID))
			} else {
				ps.aliveTsp = 0
			}
			return
		}
	}
}

func (ps *producerState) aliveDiffCheck(tsp int) (int, bool) {
	if ps.aliveDiffMax < 0 || ps.aliveTsp == 0 {
		return 0, true
	}
	diff := tsp - ps.aliveTsp
	ok := int64(diff) <= ps.aliveDiffMax.Milliseconds()
	return diff, ok
}

func (ps *producerState) aliveDelayCheck(tsp int) (int, bool) {
	if ps.aliveDelayMax < 0 {
		return 0, true
	}
	delay := uof.CurrentTimestamp() - tsp
	ok := int64(delay) <= ps.aliveDelayMax.Milliseconds()
	return delay, ok
}

func (ps *producerState) aliveDelayAccept(tsp int) bool {
	if ps.aliveDelayOK < 0 {
		return true
	}
	delay := uof.CurrentTimestamp() - tsp
	ok := int64(delay) <= ps.aliveDelayOK.Milliseconds()
	return ok
}
