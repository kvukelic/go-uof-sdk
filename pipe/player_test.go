package pipe

import (
	"io/ioutil"
	"sync"
	"testing"

	"github.com/minus5/go-uof-sdk"
	"github.com/stretchr/testify/assert"
)

type playerAPIMock struct {
	requests map[int]struct{}
	sync.Mutex
}

func (m *playerAPIMock) Player(lang uof.Lang, playerID int) (uof.PlayerProfile, error) {
	m.Lock()
	defer m.Unlock()
	m.requests[uof.UIDWithLang(playerID, lang)] = struct{}{}
	return uof.PlayerProfile{
		Player: uof.Player{
			ID: playerID,
		},
	}, nil
}

func TestPlayerPipe(t *testing.T) {
	a := &playerAPIMock{requests: make(map[int]struct{})}
	p := Player(a, []uof.Lang{uof.LangEN, uof.LangDE}, false)
	assert.NotNil(t, p)

	in := make(chan *uof.Message)
	out, _ := p(in)

	// this type of message is passing through
	m := uof.NewConnnectionMessage(uof.ConnectionStatusUp)
	in <- m
	om := <-out
	assert.Equal(t, m, om)

	m = oddsChangeMessage(t)
	in <- m
	om = <-out
	assert.Equal(t, m, om)

	close(in)
	cnt := 0
	for range out {
		cnt++
	}
	assert.Equal(t, 82, cnt)
	assert.Equal(t, 82, len(a.requests))
}

func TestPlayerPipeWithPlayerConfirm(t *testing.T) {
	a := &playerAPIMock{requests: make(map[int]struct{})}
	p := Player(a, []uof.Lang{uof.LangEN, uof.LangDE}, true)
	assert.NotNil(t, p)

	in := make(chan *uof.Message)
	out, _ := p(in)

	// this type of message is passing through
	m := uof.NewConnnectionMessage(uof.ConnectionStatusUp)
	in <- m
	om := <-out
	assert.Equal(t, m, om)

	oc := oddsChangeMessage(t)
	in <- oc
	om = <-out
	assert.Equal(t, oc, om)

	close(in)
	cnt := 0
	var last *uof.Message
	for outm := range out {
		cnt++
		last = outm
	}

	// 82 player messages + 1 extra oddschange
	assert.Equal(t, 83, cnt)
	assert.Equal(t, 82, len(a.requests))
	// the extra oddschange only has a header, copied from the original oddschange
	assert.Equal(t, oc.Header, last.Header)
	assert.Equal(t, uof.Body{}, last.Body)
	assert.Equal(t, []byte{}, last.Raw)
}

func oddsChangeMessage(t *testing.T) *uof.Message {
	buf, err := ioutil.ReadFile("../testdata/odds_change-0.xml")
	assert.NoError(t, err)
	m, err := uof.NewQueueMessage("hi.pre.-.odds_change.1.sr:match.1234.-", buf)
	assert.NoError(t, err)
	return m
}
