package pipe

import (
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type extraFixtureAPI interface {
	Fixture(lang uof.Lang, eventURN uof.URN) (uof.FixtureRsp, error)
}

type extraFixtures struct {
	api       extraFixtureAPI
	prefixes  []uof.PrefixType
	evTypes   []uof.EventType
	languages []uof.Lang
	em        *expireMap
	out       chan<- *uof.Message
	errc      chan<- error
	subProcs  *sync.WaitGroup
	rateLimit chan struct{}
}

func ExtraFixtures(api extraFixtureAPI, languages []uof.Lang, prefixes []uof.PrefixType, evTypes []uof.EventType) InnerStage {
	x := extraFixtures{
		api:       api,
		languages: languages,
		prefixes:  prefixes,
		evTypes:   evTypes,
		em:        newExpireMap(time.Hour),
		subProcs:  &sync.WaitGroup{},
		rateLimit: make(chan struct{}, ConcurentAPICallsLimit),
	}
	return StageWithSubProcesses(x.loop)
}

func (x *extraFixtures) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	x.out, x.errc = out, errc
	for m := range in {
		out <- m
		if x.isExtraEventMessage(m) {
			x.getExtraFixture(m.EventURN, m.ReceivedAt)
		}
	}
	return x.subProcs
}

func (x *extraFixtures) isExtraEventMessage(m *uof.Message) bool {
	if m.Type.Kind() == uof.MessageKindEvent && !m.Is(uof.MessageTypeFixtureChange) {
		for _, p := range x.prefixes {
			if m.EventURN.PrefixType() == p {
				for _, t := range x.evTypes {
					if m.EventURN.EventType() == t {
						return true
					}
				}
			}
		}
	}
	return false
}

func (x *extraFixtures) getExtraFixture(fixtureURN uof.URN, receivedAt int) {
	x.subProcs.Add(len(x.languages))
	for _, lang := range x.languages {
		go func(lang uof.Lang) {
			defer x.subProcs.Done()
			x.rateLimit <- struct{}{}
			defer func() { <-x.rateLimit }()

			key := uof.UIDWithLang(fixtureURN.EventID(), lang)
			if x.em.fresh(key) {
				return
			}
			rsp, err := x.api.Fixture(lang, fixtureURN)
			if err != nil {
				x.errc <- err
				return
			}
			generatedAt := int(rsp.GeneratedAt.UnixNano() / 1e6)
			x.out <- uof.NewFixtureMessage(lang, rsp.Fixture, rsp.Raw, receivedAt, generatedAt)
			x.em.insert(key)
		}(lang)
	}
}
