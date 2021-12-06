package pipe

import (
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type fixtureAPI interface {
	Fixture(lang uof.Lang, eventURN uof.URN) (*uof.Fixture, error)
	FixtureChanges(lang uof.Lang, from time.Time) ([]uof.Change, time.Time, error)
	FixtureSchedule(lang uof.Lang, to time.Time, max int) (<-chan uof.Fixture, <-chan time.Time, <-chan error)
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

func (f *fixture) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	f.out, f.errc = out, errc
	urns := f.loopWithPreload(in)
	for _, u := range urns {
		f.getFixture(u, uof.CurrentTimestamp(), false)
	}
	f.loopWithRecovery(in)
	return f.subProcs
}

func (f *fixture) loopWithPreload(in <-chan *uof.Message) []uof.URN {
	done := make(chan struct{})

	f.subProcs.Add(1)
	go func() {
		defer f.subProcs.Done()
		f.preloadFixtures()
		close(done)
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

func (f *fixture) loopWithRecovery(in <-chan *uof.Message) {
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

func (f *fixture) preloadFixtures() {
	if f.preloadTo.IsZero() {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(f.languages))
	tsps := make(chan time.Time, len(f.languages))
	for _, lang := range f.languages {
		go func(lang uof.Lang) {
			defer wg.Done()
			in, tspc, errc := f.api.FixtureSchedule(lang, f.preloadTo, f.preloadMax)
			for x := range in {
				f.out <- uof.NewFixtureMessage(lang, x, uof.CurrentTimestamp())
			}
			if tsp := <-tspc; !tsp.IsZero() {
				tsps <- tsp
			}
			if err := <-errc; err != nil {
				f.errc <- err
			}
		}(lang)
	}
	wg.Wait()
	close(tsps)
	f.recoveryTsp.set(earliest(tsps))
}

func (f *fixture) recoverFixtures() {
	from := f.recoveryTsp.get()
	if from.IsZero() {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(f.languages))
	rsps := make(chan []uof.Change, len(f.languages))
	tsps := make(chan time.Time, len(f.languages))
	for _, lang := range f.languages {
		go func(lang uof.Lang) {
			defer wg.Done()
			chs, tsp, err := f.api.FixtureChanges(lang, pastLimit(from, time.Hour))
			if err == nil {
				rsps <- chs
			}
			if err == nil && !tsp.IsZero() {
				tsps <- tsp
			}
			if err != nil {
				f.errc <- err
			}
		}(lang)
	}
	wg.Wait()
	close(rsps)
	close(tsps)
	f.recoveryTsp.shift(from, earliest(tsps))
	for _, u := range collectURNs(rsps) {
		f.getFixture(u, uof.CurrentTimestamp(), false)
	}
}

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
			fixture, err := f.api.Fixture(lang, eventURN)
			if err != nil {
				f.errc <- err
				return
			}
			if fixture != nil {
				f.out <- uof.NewFixtureMessage(lang, *fixture, receivedAt)
				f.em.insert(key)
			}
		}(lang)
	}
}

func (f *fixture) noneRecovering() bool {
	for _, s := range f.producers {
		if s == uof.ProducerStatusInRecovery {
			return false
		}
	}
	return true
}

func (f *fixture) updateRecoveryTsp(timestamp int) {
	noRec := f.noneRecovering()
	tsp := time.Unix(0, int64(timestamp)*1e6)
	f.recoveryTsp.setIf(tsp, func(curr time.Time) bool {
		return tsp.Before(curr) || noRec
	})
}

func (f *fixture) producersStatusChange(producers uof.ProducersChange, preload bool) bool {
	shouldRecover := false
	for _, pc := range producers {
		if f.producers[pc.Producer] != pc.Status && !preload {
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

func Fixture(api fixtureAPI, languages []uof.Lang, preloadTo time.Time, preloadMax int) InnerStage {
	f := newFixture(api, languages, preloadTo, preloadMax)
	return StageWithSubProcesses(f.loop)
}

// collectURNs is an auxiliary function that returns all unique URNs appearing
// in a channel of fixture change lists
func collectURNs(rsps <-chan []uof.Change) []uof.URN {
	set := make(map[uof.URN]struct{})
	for rsp := range rsps {
		for _, ch := range rsp {
			set[ch.EventURN] = struct{}{}
		}
	}
	urns := make([]uof.URN, 0)
	for urn := range set {
		if urn != uof.NoURN {
			urns = append(urns, urn)
		}
	}
	return urns
}

// earliest is an auxiliary function that returns the earliest non-zero time
// instant appearing in a channel
func earliest(tsps <-chan time.Time) time.Time {
	best := time.Time{}
	for tsp := range tsps {
		if tsp.IsZero() {
			continue
		}
		if best.IsZero() || tsp.Before(best) {
			best = tsp
		}
	}
	return best
}

// pastLimit is an auxiliary function that, for a given time instant and
// duration, returns either the given time instant or a time instant that is
// the given amount of duration in the past from now.
//
// The function returns whichever of these two time instants is later.
func pastLimit(t time.Time, d time.Duration) time.Time {
	minFrom := time.Now().Add(-d)
	if t.Before(minFrom) {
		return minFrom
	}
	return t
}

type syncTime struct {
	time time.Time
	sync.RWMutex
}

func (st *syncTime) set(t time.Time) {
	st.Lock()
	defer st.Unlock()
	st.time = t
}

func (st *syncTime) setIf(t time.Time, fn func(time.Time) bool) {
	st.Lock()
	defer st.Unlock()
	if fn(st.time) {
		st.time = t
	}
}

func (st *syncTime) shift(from time.Time, to time.Time) {
	st.Lock()
	defer st.Unlock()
	if st.time.Before(from) || st.time.After(to) {
		return
	}
	st.time = to
}

func (st *syncTime) get() time.Time {
	st.RLock()
	defer st.RUnlock()
	return st.time
}
