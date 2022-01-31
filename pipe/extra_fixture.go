package pipe

import (
	"strings"
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type extraFixtureAPI interface {
	Fixture(lang uof.Lang, eventURN uof.URN) (uof.FixtureRsp, error)
}

type extraFixtures struct {
	api        extraFixtureAPI
	languages  []uof.Lang
	namespaces []string
	em         *expireMap
	out        chan<- *uof.Message
	errc       chan<- error
	subProcs   *sync.WaitGroup
	rateLimit  chan struct{}
}

func ExtraFixtures(api extraFixtureAPI, languages []uof.Lang, namespaces []string) InnerStage {
	x := extraFixtures{
		api:        api,
		languages:  languages,
		namespaces: namespaces,
		em:         newExpireMap(time.Hour),
		subProcs:   &sync.WaitGroup{},
		rateLimit:  make(chan struct{}, ConcurentAPICallsLimit),
	}
	return StageWithSubProcesses(x.loop)
}

func (x *extraFixtures) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	x.out, x.errc = out, errc
	for m := range in {
		out <- m
		if x.isExtraEventMessage(m.Type, m.EventURN) {
			x.getExtraFixture(m.EventURN, m.ReceivedAt)
		}
	}
	return x.subProcs
}

func (x *extraFixtures) isExtraEventMessage(typ uof.MessageType, urn uof.URN) bool {
	if typ.Kind() == uof.MessageKindEvent && typ != uof.MessageTypeFixtureChange {
		for _, n := range x.namespaces {
			if strings.HasPrefix(urn.String(), n) {
				return true
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
			x.out <- uof.NewFixtureMessage(lang, rsp.Fixture, receivedAt, generatedAt)
			x.em.insert(key)
		}(lang)
	}
}
