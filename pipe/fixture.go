package pipe

import (
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type fixtureAPI interface {
	Fixture(lang uof.Lang, eventURN uof.URN) (uof.FixtureRsp, error)
	FixtureChanges(lang uof.Lang, from time.Time) (uof.ChangesRsp, error)
	FixtureSchedule(lang uof.Lang, to time.Time, max int) (<-chan uof.FixtureRsp, <-chan error)
}

type fixture struct {
	api         fixtureAPI
	languages   []uof.Lang
	preloadTo   time.Time
	preloadMax  int
	producers   map[uof.Producer]uof.ProducerStatus
	recoveryTsp syncTime
	em          *expireMap
	errc        chan<- error
	out         chan<- *uof.Message
	subProcs    *sync.WaitGroup
	rateLimit   chan struct{}
}

func newFixture(api fixtureAPI, languages []uof.Lang, preloadTo time.Time, preloadMax int) *fixture {
	return &fixture{
		api:        api,
		languages:  languages,
		preloadTo:  preloadTo,
		preloadMax: preloadMax,
		producers:  make(map[uof.Producer]uof.ProducerStatus),
		em:         newExpireMap(time.Minute),
		subProcs:   &sync.WaitGroup{},
		rateLimit:  make(chan struct{}, ConcurentAPICallsLimit),
	}
}

// loop of the Fixture stage. Runs in two phases.
//
// First phase:
//  - Feed processing while preloading fixtures from the API
//  - Incoming fixture changes from feed are cached only
//  - When done, trigger fetch for all cached fixtures before second phase
// Second phase:
//  - Feed processing while reacting to producers in recovery
//  - Incoming fixture changes trigger individual fixture fetch from the API
//  - Producer recovery completion triggers request for possibly missed fixture changes
//  - Each possibly missed change also triggers fixture fetch from the API
func (f *fixture) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	f.out, f.errc = out, errc
	urns := f.loopWhilePreload(in)
	for _, u := range urns {
		f.getFixture(u, uof.CurrentTimestamp(), false)
	}
	f.loopAfterPreload(in)
	return f.subProcs
}

// loopWhilePreload implements the first phase of the Fixture stage loop.
// It does the preload of fixtures from schedule API endpoints while caching
// and returning URNs of any fixture changes received in the meantime.
func (f *fixture) loopWhilePreload(in <-chan *uof.Message) []uof.URN {
	done := make(chan struct{})

	f.subProcs.Add(1)
	go func() {
		defer f.subProcs.Done()
		defer close(done)
		f.preloadFixtures()
	}()

	var urns []uof.URN
	for {
		select {
		case m, ok := <-in:
			if !ok {
				return urns
			}
			f.out <- m
			if m.IsFixtureChange() {
				urns = append(urns, m.FixtureChange.EventURN)
			}
			if m.IsProducersChange() {
				f.producersStatusChange(m.Producers, true)
			}
		case <-done:
			return urns
		}
	}
}

// loopAfterPreload implements the second phase of the Fixture stage loop.
// It triggers fixture fetch for each fixture change received, while also
// reacting to completed producer recoveries by recovering potentially missed
// fixture changes.
//
// One recovery of fixture changes is also triggered upon first entering this
// loop. This is to account for the fact that the API caches schedule endpoint
// responses, and to cover for any changes that might have happened after the
// last caching.
func (f *fixture) loopAfterPreload(in <-chan *uof.Message) {
	recover := func() {
		f.subProcs.Add(1)
		go func() {
			defer f.subProcs.Done()
			f.recoverFixtures()
		}()
	}
	recover()

	for m := range in {
		f.out <- m
		if m.IsFixtureChange() {
			f.getFixture(m.FixtureChange.EventURN, m.ReceivedAt, true)
		}
		if m.IsProducersChange() {
			shouldRecover := f.producersStatusChange(m.Producers, false)
			if shouldRecover {
				recover()
			}
		}
	}
}

// preloadFixtures requests scheduled fixtures and inserts fixture messages
// into the pipeline
func (f *fixture) preloadFixtures() {
	if f.preloadTo.IsZero() {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(f.languages))
	var recoveryTsp syncTime
	for _, lang := range f.languages {
		go func(lang uof.Lang) {
			defer wg.Done()
			rsps, errc := f.api.FixtureSchedule(lang, f.preloadTo, f.preloadMax)
			for rsp := range rsps {
				x, tsp := rsp.Fixture, rsp.GeneratedAt
				recoveryTsp.set(earlierNonZero(recoveryTsp.get(), tsp))
				generatedAt := int(tsp.UnixNano() / 1e6)
				f.out <- uof.NewFixtureMessage(lang, x, uof.CurrentTimestamp(), generatedAt)
			}
			if err := <-errc; err != nil {
				f.errc <- err
			}
		}(lang)
	}
	wg.Wait()
	f.recoveryTsp.set(recoveryTsp.get())
}

// recoverFixtures requests potentially missed fixture changes and triggers
// fixture fetch for each of them
func (f *fixture) recoverFixtures() {
	from := f.recoveryTsp.get()
	if from.IsZero() {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(f.languages))
	changedUrns := make([]uof.URN, 0)
	var recoveryTsp time.Time
	for _, lang := range f.languages {
		go func(lang uof.Lang) {
			defer wg.Done()
			rsp, err := f.api.FixtureChanges(lang, pastLimit(from, time.Hour))
			if err == nil {
				recoveryTsp = earlierNonZero(recoveryTsp, rsp.GeneratedAt)
				changedUrns = addChangeURNs(changedUrns, rsp.Changes)
			} else {
				f.errc <- err
				return
			}
		}(lang)
	}
	wg.Wait()
	f.recoveryTsp.shift(from, recoveryTsp)
	for _, u := range changedUrns {
		f.getFixture(u, uof.CurrentTimestamp(), false)
	}
}

// getFixture requests an individual fixture from the API and inserts fixture
// messages into the pipeline
func (f *fixture) getFixture(eventURN uof.URN, receivedAt int, forceUpdate bool) {
	f.subProcs.Add(len(f.languages))
	for _, lang := range f.languages {
		go func(lang uof.Lang) {
			defer f.subProcs.Done()
			f.rateLimit <- struct{}{}
			defer func() { <-f.rateLimit }()

			key := uof.UIDWithLang(eventURN.EventID(), lang)
			if f.em.fresh(key) && !forceUpdate {
				return
			}
			rsp, err := f.api.Fixture(lang, eventURN)
			if err != nil {
				f.errc <- err
				return
			}
			generatedAt := int(rsp.GeneratedAt.UnixNano() / 1e6)
			f.out <- uof.NewFixtureMessage(lang, rsp.Fixture, receivedAt, generatedAt)
			f.em.insert(key)
		}(lang)
	}
}

// anyRecovering returns true if at least one producer is in recovery state
func (f *fixture) anyRecovering() bool {
	for _, s := range f.producers {
		if s == uof.ProducerStatusInRecovery {
			return true
		}
	}
	return false
}

// updateRecoveryTsp moves the fixture change recovery timestamp due to a
// producer recovery. If no other producer is recovering, the timestamp is set
// to the start of this producer's recovery window. If there are other
// producers in recovery, the timestamp is moved to the start of this
// producer's recovery window only if it is earlier than the current timestamp.
func (f *fixture) updateRecoveryTsp(timestamp int) {
	noRecoveries := !f.anyRecovering()
	tsp := time.Unix(0, int64(timestamp)*1e6)
	f.recoveryTsp.setIf(tsp, func(curr time.Time) bool {
		return noRecoveries || tsp.Before(curr)
	})
}

// producersStatusChange processes producers change messages. It does updates
// on the state of the producers and the recovery timestamp, and returns true
// when a recovery may trigger due to a completed producer recovery.
func (f *fixture) producersStatusChange(producers uof.ProducersChange, inPreload bool) bool {
	shouldRecover := false
	for _, pc := range producers {
		if f.producers[pc.Producer] != pc.Status && !inPreload {
			if pc.Status == uof.ProducerStatusInRecovery {
				f.updateRecoveryTsp(pc.RecoveryTimestamp)
			}
			if pc.Status == uof.ProducerStatusActive {
				shouldRecover = true
			}
		}
		f.producers[pc.Producer] = pc.Status
	}
	return shouldRecover
}

// Fixture inner stage for the SDK pipeline
func Fixture(api fixtureAPI, languages []uof.Lang, preloadTo time.Time, preloadMax int) InnerStage {
	f := newFixture(api, languages, preloadTo, preloadMax)
	return StageWithSubProcesses(f.loop)
}

// AUXILIARIES

// addChangeURNs is an auxiliary function that extracts URNs from a list of
// UOF fixture changes and adds them to a given list of URNs. It also removes
// any duplicate occurences of URNs in the final list.
func addChangeURNs(urns []uof.URN, changes []uof.Change) []uof.URN {
	dedupMap := make(map[uof.URN]bool)
	for _, urn := range urns {
		dedupMap[urn] = true
	}
	for _, ch := range changes {
		dedupMap[ch.EventURN] = true
	}
	merged := make([]uof.URN, 0)
	for urn, val := range dedupMap {
		if val {
			merged = append(merged, urn)
		}
	}
	return merged
}

// earlierNonZero is an auxiliary function that returns the earliest time
// instant from the set of given two time instants, excluding any zero
// value instants. If both given instants are zero values, the function will
// return a zero value time instant.
func earlierNonZero(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

// pastLimit is an auxiliary function that, for a given time instant and
// duration, returns either that given time instant, or a time instant that
// is the given amount of duration in the past from now. The function returns
// whichever of these two time instants is later.
func pastLimit(t time.Time, d time.Duration) time.Time {
	minFrom := time.Now().Add(-d)
	if t.Before(minFrom) {
		return minFrom
	}
	return t
}

// syncTime is an auxiliary structure acting as a concurrent-safe time instant
type syncTime struct {
	time time.Time
	sync.RWMutex
}

// set sets this time instant to a desired instant
func (st *syncTime) set(t time.Time) {
	st.Lock()
	defer st.Unlock()
	st.time = t
}

// setIf sets this time instant only if the provided condition is met
func (st *syncTime) setIf(t time.Time, fn func(time.Time) bool) {
	st.Lock()
	defer st.Unlock()
	if fn(st.time) {
		st.time = t
	}
}

// shift checks if this current instant is within the interval between instants
// 'from' and 'to', in which case it is set to the time instant 'to'
func (st *syncTime) shift(from time.Time, to time.Time) {
	st.Lock()
	defer st.Unlock()
	if st.time.Before(from) || st.time.After(to) {
		return
	}
	st.time = to
}

// get return this current time instant
func (st *syncTime) get() time.Time {
	st.RLock()
	defer st.RUnlock()
	return st.time
}
