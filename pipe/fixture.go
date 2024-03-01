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
	api        fixtureAPI
	languages  []uof.Lang
	preload    bool
	preloadTo  time.Time
	preloadMax int
	preloadTsp time.Time
	producers  map[uof.Producer]uof.ProducerStatus
	em         *expireMap
	errc       chan<- error
	out        chan<- *uof.Message
	subProcs   *sync.WaitGroup
	rateLimit  chan struct{}
}

func newFixture(api fixtureAPI, languages []uof.Lang, preload bool, preloadTo time.Time, preloadMax int) *fixture {
	return &fixture{
		api:        api,
		languages:  languages,
		preload:    preload,
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
// First phase (if preload enabled):
//   - Process (passthrough) feed while preloading fixtures from the API
//   - Incoming fixture changes from feed are cached only
//   - Changes possibly missed in preload are fetched and cached only
//
// Second phase:
//   - Do individual fixture fetch for each unique change cached in first phase
//   - Process feed while reacting to producers in recovery
//   - Incoming fixture changes trigger individual fixture fetch from the API
//   - Producer entering recovery state triggers request for possibly missed changes
//   - Each such possibly missed change also triggers fixture fetch from the API
func (f *fixture) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	f.out, f.errc = out, errc
	toFetch := []uof.URN{}
	if f.preload {
		toFetch = f.loopAndPreload(in)
	}
	f.loopAfterPreload(in, toFetch)
	return f.subProcs
}

// loopAndPreload implements the first phase of the Fixture stage loop.
// It does the preload of fixtures from schedule API endpoints. It will also
// fetch possibly missed fixture changes (due to schedule API endpoint caching).
// Meanwhile, URNs of any fixture changes received through feed are cached.
// When preload is completed, a merged and deduplicated list of both possibly
// missed and cached fixture URNs is returned for fetching in second phase.
func (f *fixture) loopAndPreload(in <-chan *uof.Message) []uof.URN {
	var urns urnSet
	// start preload + collect potentially missed fixture changes to fetch after preload
	done := make(chan struct{})
	f.subProcs.Add(1)
	go func() {
		defer f.subProcs.Done()
		defer close(done)
		missed := f.preloadFixtures()
		urns.add(missed)
	}()
	// passthrough incoming messages + collect incoming fixture changes to fetch after preload
	var onHold []uof.URN
	for {
		select {
		case m, ok := <-in:
			if !ok {
				urns.add(onHold)
				return urns.get()
			}
			f.out <- m
			if m.IsFixtureChange() {
				onHold = append(onHold, m.FixtureChange.EventURN)
			}
			if m.IsProducersChange() {
				f.producersStatusChange(m.Producers, true)
			}
		case <-done:
			urns.add(onHold)
			return urns.get()
		}
	}
}

// loopAfterPreload implements the second phase of the Fixture stage loop.
// It triggers fixture fetch for each fixture change received, while also
// reacting to producers in recovery by recovering potentially missed fixture
// changes. Upon first entering this loop, a fetch for each URN in the given
// list is also performed in parallel.
func (f *fixture) loopAfterPreload(in <-chan *uof.Message, toFetch []uof.URN) {
	// start load of fixtures we need to fetch up to this point
	f.subProcs.Add(1)
	go func() {
		defer f.subProcs.Done()
		for _, u := range toFetch {
			f.getFixture(u, uof.CurrentTimestamp(), false)
		}
	}()
	// consume incoming messages
	for m := range in {
		f.out <- m
		if m.IsFixtureChange() {
			f.getFixture(m.FixtureChange.EventURN, m.ReceivedAt, true)
		}
		if m.IsProducersChange() {
			recover, tsp := f.producersStatusChange(m.Producers, false)
			if recover {
				// recover potentially missed fixtures
				f.subProcs.Add(1)
				go func() {
					defer f.subProcs.Done()
					missed := f.getChangesSince(tsp)
					for _, u := range missed {
						f.getFixture(u, uof.CurrentTimestamp(), true)
					}
				}()
			}
		}
	}
}

// preloadFixtures requests scheduled fixtures and inserts fixture messages
// into the pipeline. It will also fetch possibly missed fixture changes
// according to fetched schedule timestamps, and return their URNs.
func (f *fixture) preloadFixtures() []uof.URN {
	var wg sync.WaitGroup
	wg.Add(len(f.languages))
	var earliestTsp syncTime
	for _, lang := range f.languages {
		go func(lang uof.Lang) {
			defer wg.Done()
			rsps, errc := f.api.FixtureSchedule(lang, f.preloadTo, f.preloadMax)
			var errWg sync.WaitGroup
			errWg.Add(1)
			go func() {
				defer errWg.Done()
				for err := range errc {
					f.errc <- err
				}
			}()
			for rsp := range rsps {
				earliestTsp.set(earlierNonZero(earliestTsp.get(), rsp.GeneratedAt))
				generatedAt := int(rsp.GeneratedAt.UnixNano() / 1e6)
				f.out <- uof.NewFixtureMessage(lang, rsp.Fixture, rsp.Raw, uof.CurrentTimestamp(), generatedAt)
			}
			errWg.Wait()
		}(lang)
	}
	wg.Wait()
	f.preloadTsp = earliestTsp.get()
	return f.getChangesSince(earliestTsp.get())
}

// getChangesSince requests fixture changes that have happened since the given
// point in time and returns their URNs.
func (f *fixture) getChangesSince(from time.Time) []uof.URN {
	var urns urnSet
	if !from.IsZero() {
		var wg sync.WaitGroup
		wg.Add(len(f.languages))
		for _, lang := range f.languages {
			go func(lang uof.Lang) {
				defer wg.Done()
				rsp, err := f.api.FixtureChanges(lang, pastLimit(from, time.Hour))
				if err == nil {
					urns.addFromChanges(rsp.Changes)
				} else {
					f.errc <- err
				}
			}(lang)
		}
		wg.Wait()
	}
	return urns.get()
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
			f.out <- uof.NewFixtureMessage(lang, rsp.Fixture, rsp.Raw, receivedAt, generatedAt)
			f.em.insert(key)
		}(lang)
	}
}

// producersStatusChange processes producers change messages. It updates the
// states of the producers, and signals when a recovery should be triggered
// by returning true as the first returned value, and time of the start of
// the producer's recovery window as the second value.
func (f *fixture) producersStatusChange(changes uof.ProducersChange, inPreload bool) (bool, time.Time) {
	shouldRecover := false
	recoveryTsp := time.Time{}
	for _, pc := range changes {
		if pc.Status == uof.ProducerStatusInRecovery && f.producers[pc.Producer] != uof.ProducerStatusInRecovery && !inPreload {
			shouldRecover = true
			tsp := time.Unix(0, int64(pc.RecoveryTimestamp)*1e6)
			if !tsp.IsZero() && (recoveryTsp.IsZero() || tsp.Before(recoveryTsp)) {
				recoveryTsp = tsp
				if recoveryTsp.Before(f.preloadTsp) {
					recoveryTsp = f.preloadTsp
				}
			}
		}
		f.producers[pc.Producer] = pc.Status
	}
	return shouldRecover, recoveryTsp
}

// Fixture inner stage for the SDK pipeline
func Fixture(api fixtureAPI, languages []uof.Lang, preload bool, preloadTo time.Time, preloadMax int) InnerStage {
	f := newFixture(api, languages, preload, preloadTo, preloadMax)
	return StageWithSubProcessesSync(f.loop)
}

// AUXILIARIES

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

// get return this current time instant
func (st *syncTime) get() time.Time {
	st.RLock()
	defer st.RUnlock()
	return st.time
}

// urnSet is an auxiliary structure acting as a concurrent-safe set of uof.URNs
type urnSet struct {
	urns []uof.URN
	sync.RWMutex
}

// add URNs from a given list to the set (with dedup)
func (us *urnSet) add(urns []uof.URN) {
	dedupMap := us.toMap()
	for _, urn := range urns {
		dedupMap[urn] = true
	}
	merged := make([]uof.URN, 0)
	for urn := range dedupMap {
		merged = append(merged, urn)
	}
	us.Lock()
	defer us.Unlock()
	us.urns = merged
}

// add URNs from a list of uof.Changes to the set (with dedup)
func (us *urnSet) addFromChanges(changes []uof.Change) {
	dedupMap := us.toMap()
	for _, ch := range changes {
		dedupMap[ch.EventURN] = true
	}
	merged := make([]uof.URN, 0)
	for urn := range dedupMap {
		merged = append(merged, urn)
	}
	us.Lock()
	defer us.Unlock()
	us.urns = merged
}

// convert set to map[uof.URN]bool with value=true for each URN in set
func (us *urnSet) toMap() map[uof.URN]bool {
	us.RLock()
	defer us.RUnlock()
	ret := make(map[uof.URN]bool)
	for _, urn := range us.urns {
		ret[urn] = true
	}
	return ret
}

// get set as a slice of uof.URNs
func (us *urnSet) get() []uof.URN {
	us.RLock()
	defer us.RUnlock()
	ret := make([]uof.URN, 0, len(us.urns))
	ret = append(ret, us.urns...)
	return ret
}
