package pipe

import (
	"sync"
	"time"

	"github.com/minus5/go-uof-sdk"
)

type playerAPI interface {
	Player(lang uof.Lang, playerID int) (uof.PlayerProfile, error)
}

type player struct {
	api       playerAPI
	languages []uof.Lang // suported languages
	confirm   bool
	em        *expireMap
	errc      chan<- error
	out       chan<- *uof.Message
	rateLimit chan struct{}
	subProcs  *sync.WaitGroup
}

func Player(api playerAPI, languages []uof.Lang, confirm bool) InnerStage {
	p := &player{
		api:       api,
		languages: languages,
		confirm:   confirm,
		em:        newExpireMap(time.Hour),
		subProcs:  &sync.WaitGroup{},
		rateLimit: make(chan struct{}, ConcurentAPICallsLimit),
	}
	return StageWithSubProcessesSync(p.loop)
}

func (p *player) loop(in <-chan *uof.Message, out chan<- *uof.Message, errc chan<- error) *sync.WaitGroup {
	p.errc, p.out = errc, out

	for m := range in {
		out <- m
		if m.Is(uof.MessageTypeOddsChange) {
			var wg sync.WaitGroup
			var anyPlayers bool = false
			m.OddsChange.EachPlayer(func(playerID int) {
				wg.Add(1)
				anyPlayers = true
				p.get(playerID, m.ReceivedAt, func() { wg.Done() })
			})
			if p.confirm && anyPlayers {
				p.subProcs.Add(1)
				go func(msg *uof.Message) {
					defer p.subProcs.Done()
					wg.Wait()
					out <- &uof.Message{Header: msg.Header, Raw: []byte{}, Body: uof.Body{}}
				}(m)
			}
		}
	}
	return p.subProcs
}

func (p *player) get(playerID, requestedAt int, callback func()) {
	var wg sync.WaitGroup
	wg.Add(len(p.languages))
	p.subProcs.Add(len(p.languages))
	for _, lang := range p.languages {
		go func(lang uof.Lang) {
			defer p.subProcs.Done()
			defer wg.Done()
			p.rateLimit <- struct{}{}
			defer func() { <-p.rateLimit }()

			key := uof.UIDWithLang(playerID, lang)
			if p.em.fresh(key) {
				return
			}
			p.em.insert(key)
			pp, err := p.api.Player(lang, playerID)
			if err != nil {
				p.em.remove(key)
				p.errc <- err
				return
			}
			generatedAt := int(pp.GeneratedAt.UnixNano() / 1e6)
			p.out <- uof.NewPlayerMessage(lang, &pp.Player, requestedAt, generatedAt)
		}(lang)
	}
	p.subProcs.Add(1)
	go func() {
		defer p.subProcs.Done()
		wg.Wait()
		callback()
	}()
}
