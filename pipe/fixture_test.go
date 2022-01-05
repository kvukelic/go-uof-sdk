package pipe

import (
	"sync"
	"testing"
	"time"

	"github.com/minus5/go-uof-sdk"
	"github.com/stretchr/testify/assert"
)

type fixtureAPIMock struct {
	preloadTo time.Time
	eventURN  uof.URN
	changeURN uof.URN
	sync.Mutex
}

func (m *fixtureAPIMock) Fixture(lang uof.Lang, eventURN uof.URN) (uof.FixtureRsp, error) {
	m.eventURN = eventURN
	return uof.FixtureRsp{
		Fixture: uof.Fixture{
			URN: eventURN,
		},
	}, nil
}

func (m *fixtureAPIMock) FixtureChanges(lang uof.Lang, from time.Time) (uof.ChangesRsp, error) {
	return uof.ChangesRsp{
		Changes: []uof.Change{{
			EventID:  m.changeURN.ID(),
			EventURN: m.changeURN,
		}},
	}, nil
}

func (m *fixtureAPIMock) FixtureSchedule(lang uof.Lang, to time.Time, max int) (<-chan uof.FixtureRsp, <-chan error) {
	m.preloadTo = to
	out := make(chan uof.FixtureRsp)
	errc := make(chan error)
	go func() {
		close(out)
		close(errc)
	}()
	return out, errc
}

func TestFixturePipe(t *testing.T) {
	a := &fixtureAPIMock{changeURN: "sr:match:1235"}
	preloadTo := time.Now().Add(time.Hour)
	langs := []uof.Lang{uof.LangEN, uof.LangDE}
	f := Fixture(a, langs, preloadTo, 0)
	assert.NotNil(t, f)

	// run the stage loop (& start preload)
	in := make(chan *uof.Message)
	defer close(in)
	out, _ := f(in)

	// connection message -> pass through
	m := uof.NewConnnectionMessage(uof.ConnectionStatusUp)
	in <- m
	om := <-out
	assert.Equal(t, m, om)

	// fixture change -> fixture fetch
	m = fixtureChangeMsg(t)
	in <- m
	om = <-out
	assert.Equal(t, m, om)
	// fixture message will be inserted for each language
	for range langs {
		fm := <-out
		assert.Equal(t, m.FixtureChange.EventURN, fm.Fixture.URN)
	}
	assert.Equal(t, m.FixtureChange.EventURN, a.eventURN)
	// fetched fixtures mean preload phase is done at this point
	assert.Equal(t, preloadTo, a.preloadTo)

	// producer in recovery -> internal updates
	tsp := uof.CurrentTimestamp() - 10000
	m = producersChangeMsg(uof.ProducerStatusInRecovery, tsp)
	in <- m
	om = <-out
	assert.Equal(t, m, om)

	// producer active -> recover fixture changes
	tsp = uof.CurrentTimestamp() - 5000
	m = producersChangeMsg(uof.ProducerStatusActive, tsp)
	in <- m
	om = <-out
	assert.Equal(t, m, om)
	// fixture message will be inserted for each language
	for range langs {
		fm := <-out
		assert.Equal(t, a.changeURN, fm.Fixture.URN)
	}
	assert.Equal(t, a.changeURN, a.eventURN)
}

func fixtureChangeMsg(t *testing.T) *uof.Message {
	buf := []byte(`<fixture_change event_id="sr:match:1234" product="3" start_time="1511107200000"/>`)
	m, err := uof.NewQueueMessage("hi.pre.-.fixture_change.1.sr:match.1234.-", buf)
	assert.NoError(t, err)
	return m
}

func producersChangeMsg(status uof.ProducerStatus, tsp int) *uof.Message {
	pc := uof.ProducersChange{{
		Producer:          uof.ProducerLiveOdds,
		Status:            status,
		RecoveryTimestamp: tsp,
	}}
	return uof.NewProducersChangeMessage(pc)
}
