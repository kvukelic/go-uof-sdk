package pipe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
	"github.com/pkg/errors"
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

// logError sends an error by the recovery operation to the pipeline
// if the error channel is free to receive the error.
func (r *recovery) logError(err error) {
	select {
	case r.errc <- uof.E("recovery", err):
	default:
	}
}

// noticeError sends a notice by the recovery operation to the pipeline.
func (r *recovery) noticeError(err error) {
	r.errc <- uof.Notice("recovery", err)
}

// cancelAllRecoveries cancels all ongoing recoveries.
func (r *recovery) cancelAllRecoveries() {
	for _, c := range r.recoveryCancel {
		if c != nil {
			c()
		}
	}
}

// cancelRecovery cancels an ongoing recovery for a producer if there is one.
func (r *recovery) cancelRecovery(producer uof.Producer) {
	c, ok := r.recoveryCancel[producer]
	if ok && c != nil {
		c()
	}
}

// enterRecovery handles requesting a recovery for a producer.
//
// Any previous ongoing recovery for the producer is cancelled. If a request
// fails, it will be repeated after a minute, until a request succeeds.
func (r *recovery) enterRecovery(producer uof.Producer, timestamp int, requestID int) {
	r.cancelRecovery(producer)
	ctx, cancel := context.WithCancel(context.Background())
	r.recoveryCancel[producer] = cancel
	r.subProcs.Add(1)
	go func() {
		defer r.subProcs.Done()
		for {
			recoveryTsp := recoveryTimestamp(timestamp, producer)
			r.logError(fmt.Errorf("%s in recovery; timestamp: %d, requestID: %d", producer.Code(), recoveryTsp, requestID))
			err := r.api.RequestRecovery(producer, recoveryTsp, requestID)
			if err == nil {
				return
			}
			r.noticeError(errors.Wrapf(err, "%s in recovery; timestamp: %d, requestID: %d - ERROR", producer.Code(), recoveryTsp, requestID))
			select {
			case <-time.After(time.Minute):
			case <-ctx.Done():
				return
			}
		}
	}()
}

// producersStatusChange processes producers change messages. It updates the
// internally kept states of the producers, and enters or cancels recoveries
// as necessary.
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

// loop of the Recovery stage.
//
// Reacts to changes in producer state, as received via producers change
// messages, to handle recovery requests on the API.
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

// Recovery inner stage for the SDK pipeline
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
