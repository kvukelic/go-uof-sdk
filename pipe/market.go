package pipe

import (
	"strings"
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type marketsAPI interface {
	Markets(lang uof.Lang) (uof.MarketDescriptions, error)
	MarketVariant(lang uof.Lang, marketID int, variant string) (uof.MarketDescriptions, error)
}

type markets struct {
	api       marketsAPI
	languages []uof.Lang
	variants  bool
	outrights bool
	confirm   bool
	em        *expireMap
	errc      chan<- error
	out       chan<- *uof.Message
	rateLimit chan struct{}
	subProcs  *sync.WaitGroup
}

// getting all markets on the start
func Markets(api marketsAPI, languages []uof.Lang, variants, outrights, confirm bool) InnerStage {
	var wg sync.WaitGroup
	m := &markets{
		api:       api,
		languages: languages,
		variants:  variants,
		outrights: outrights,
		confirm:   confirm,
		em:        newExpireMap(24 * time.Hour),
		subProcs:  &wg,
		rateLimit: make(chan struct{}, ConcurentAPICallsLimit),
	}
	return StageWithSubProcessesSync(m.loop)
}

func (s *markets) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	s.out, s.errc = out, errc

	s.getAll()
	for m := range in {
		out <- m
		if m.Is(uof.MessageTypeOddsChange) && (s.variants || s.outrights) {
			var wg sync.WaitGroup
			var anyVariants bool = false
			m.OddsChange.EachVariantMarket(func(marketID int, variant string) {
				if strings.HasPrefix(variant, "pre:playerprops") {
					// TODO: it is not working for this type of variant markets
					return
				}
				if s.outrights && strings.HasPrefix(variant, "pre:markettext") {
					wg.Add(1)
					anyVariants = true
					s.variantMarket(marketID, variant, m.ReceivedAt, func() { wg.Done() })
					return
				}
				if s.variants && !strings.HasPrefix(variant, "pre:markettext") {
					wg.Add(1)
					anyVariants = true
					s.variantMarket(marketID, variant, m.ReceivedAt, func() { wg.Done() })
					return
				}
			})
			if s.confirm && anyVariants {
				s.subProcs.Add(1)
				go func(msg *uof.Message) {
					defer s.subProcs.Done()
					wg.Wait()
					out <- &uof.Message{Header: msg.Header, Raw: []byte{}, Body: uof.Body{}}
				}(m)
			}
		}
	}
	return s.subProcs
}

func (s *markets) getAll() {
	s.subProcs.Add(len(s.languages))
	requestedAt := uof.CurrentTimestamp()

	for _, lang := range s.languages {
		go func(lang uof.Lang) {
			defer s.subProcs.Done()

			ms, err := s.api.Markets(lang)
			if err != nil {
				s.errc <- err
				return
			}
			s.out <- uof.NewMarketsMessage(lang, ms, requestedAt)
		}(lang)
	}
}

func (s *markets) variantMarket(marketID int, variant string, requestedAt int, callback func()) {
	var wg sync.WaitGroup
	wg.Add(len(s.languages))
	s.subProcs.Add(len(s.languages))
	for _, lang := range s.languages {
		go func(lang uof.Lang) {
			defer s.subProcs.Done()
			defer wg.Done()
			s.rateLimit <- struct{}{}
			defer func() { <-s.rateLimit }()

			key := uof.UIDWithLang(uof.Hash(variant)<<32|marketID, lang)
			if s.em.fresh(key) {
				return
			}

			ms, err := s.api.MarketVariant(lang, marketID, variant)
			if err != nil {
				s.errc <- err
				return
			}
			s.out <- uof.NewMarketsMessage(lang, ms, requestedAt)
			s.em.insert(key)
		}(lang)
	}
	s.subProcs.Add(1)
	go func() {
		defer s.subProcs.Done()
		wg.Wait()
		callback()
	}()
}
