package pipe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type recovery struct {
	api            recoveryAPI
	producers      map[uof.Producer]uof.ProducerStatus
	recoveryCancel map[uof.Producer]context.CancelFunc
	errc           chan<- error
	subProcs       *sync.WaitGroup
}

type recoveryAPI interface {
	RequestRecovery(producer uof.Producer, timestamp int, requestID int) error
}

func newRecovery(api recoveryAPI) *recovery {
	return &recovery{
		api:            api,
		producers:      make(map[uof.Producer]uof.ProducerStatus),
		recoveryCancel: make(map[uof.Producer]context.CancelFunc),
		subProcs:       &sync.WaitGroup{},
	}
}

func (r *recovery) log(err error) {
	select {
	case r.errc <- uof.E("recovery", err):
	default:
	}
}

func (r *recovery) cancelAllRecoveries() {
	for _, c := range r.recoveryCancel {
		if c != nil {
			c()
		}
	}
}

func (r *recovery) cancelRecovery(producer uof.Producer) {
	c, ok := r.recoveryCancel[producer]
	if ok && c != nil {
		c()
	}
}

func (r *recovery) enterRecovery(producer uof.Producer, timestamp int, requestID int) {
	r.cancelRecovery(producer)
	ctx, cancel := context.WithCancel(context.Background())
	r.recoveryCancel[producer] = cancel
	r.subProcs.Add(1)
	go func() {
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
			select {
			case <-time.After(time.Minute):
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (r *recovery) producersStatusChange(producers uof.ProducersChange) {
	for _, pc := range producers {
		if r.producers[pc.Producer] != pc.Status {
			if pc.Status == uof.ProducerStatusInRecovery {
				r.enterRecovery(pc.Producer, pc.RecoveryTimestamp, pc.RecoveryID)
			} else {
				r.cancelRecovery(pc.Producer)
			}
		}
		r.producers[pc.Producer] = pc.Status
	}
}

func (r *recovery) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	r.errc = errc
	for m := range in {
		out <- m
		if m.IsProducersChange() {
			r.producersStatusChange(m.Producers)
		}
	}
	r.cancelAllRecoveries()
	return r.subProcs
}

func Recovery(api recoveryAPI) InnerStage {
	r := newRecovery(api)
	return StageWithSubProcesses(r.loop)
}

// If producer is back more than recovery window (defined for each producer)
// it has to make full recovery (forced with timestamp = 0).
// Otherwise recovery after timestamp is done.
func recoveryTimestamp(timestamp int, producer uof.Producer) int {
	window := uof.CurrentTimestamp() - timestamp
	if window >= producer.RecoveryWindow() {
		return 0
	}
	return timestamp
}
