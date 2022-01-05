package pipe

import (
	"strings"
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type outrights struct {
	api        fixtureAPI
	em         *expireMap
	namespaces []string
	languages  []uof.Lang
	out        chan<- *uof.Message
	errc       chan<- error
	subProcs   *sync.WaitGroup
	rateLimit  chan struct{}
}

func Outrights(api fixtureAPI, languages []uof.Lang, namespaces []string) InnerStage {
	o := outrights{
		api:        api,
		em:         newExpireMap(time.Hour),
		namespaces: namespaces,
		languages:  languages,
		subProcs:   &sync.WaitGroup{},
		rateLimit:  make(chan struct{}, ConcurentAPICallsLimit),
	}
	return StageWithSubProcesses(o.loop)
}

func (o *outrights) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	o.out, o.errc = out, errc
	for m := range in {
		out <- m
		if m.Type != uof.MessageTypeFixtureChange && o.isOutright(m.EventURN) {
			o.getOutrightFixture(m.EventURN, m.ReceivedAt)
		}
	}
	return o.subProcs
}

func (o *outrights) isOutright(urn uof.URN) bool {
	for _, n := range o.namespaces {
		if strings.HasPrefix(urn.String(), n) {
			return true
		}
	}
	return false
}

func (o *outrights) getOutrightFixture(outrightURN uof.URN, receivedAt int) {
	o.subProcs.Add(len(o.languages))
	for _, lang := range o.languages {
		go func(lang uof.Lang) {
			defer o.subProcs.Done()
			o.rateLimit <- struct{}{}
			defer func() { <-o.rateLimit }()

			key := uof.UIDWithLang(outrightURN.EventID(), lang)
			if o.em.fresh(key) {
				return
			}
			rsp, err := o.api.Fixture(lang, outrightURN)
			if err != nil {
				o.errc <- err
				return
			}
			generatedAt := int(rsp.GeneratedAt.UnixNano() / 1e6)
			o.out <- uof.NewFixtureMessage(lang, rsp.Fixture, receivedAt, generatedAt)
			o.em.insert(key)
		}(lang)
	}
}
