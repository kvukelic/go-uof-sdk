package pipe

import (
	"fmt"
	"sync"
	"testing"

	"github.com/minus5/go-uof-sdk"
	"github.com/stretchr/testify/assert"
)

type marketsAPIMock struct {
	requests map[string]struct{}
	sync.Mutex
}

func (m *marketsAPIMock) Markets(lang uof.Lang) (uof.MarketDescriptions, error) {
	return nil, nil
}

func (m *marketsAPIMock) MarketVariant(lang uof.Lang, marketID int, variant string) (uof.MarketDescriptions, error) {
	m.Lock()
	defer m.Unlock()
	m.requests[fmt.Sprintf("%s %d %s", lang, marketID, variant)] = struct{}{}
	return nil, nil
}

func TestMarketsPipe(t *testing.T) {
	a := &marketsAPIMock{requests: make(map[string]struct{})}
	ms := Markets(a, []uof.Lang{uof.LangEN, uof.LangDE}, true, false, false)
	assert.NotNil(t, ms)

	in := make(chan *uof.Message)
	out, _ := ms(in)

	// all markets for EN & DE
	m1 := <-out
	m2 := <-out
	assert.Equal(t, uof.MessageTypeMarkets, m1.Type)
	assert.Equal(t, uof.MessageTypeMarkets, m2.Type)

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
	assert.Equal(t, 2, cnt)

	_, found := a.requests["en 145 sr:point_range:76+"]
	assert.True(t, found)

	_, found = a.requests["de 145 sr:point_range:76+"]
	assert.True(t, found)

}

func TestMarketsPipeNoVariants(t *testing.T) {
	a := &marketsAPIMock{requests: make(map[string]struct{})}
	ms := Markets(a, []uof.Lang{uof.LangEN, uof.LangDE}, false, false, false)
	assert.NotNil(t, ms)

	in := make(chan *uof.Message)
	out, _ := ms(in)

	// all markets for EN & DE
	m1 := <-out
	m2 := <-out
	assert.Equal(t, uof.MessageTypeMarkets, m1.Type)
	assert.Equal(t, uof.MessageTypeMarkets, m2.Type)

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
	assert.Equal(t, 0, cnt)

	_, found := a.requests["en 145 sr:point_range:76+"]
	assert.False(t, found)

	_, found = a.requests["de 145 sr:point_range:76+"]
	assert.False(t, found)
}

func TestMarketsPipeWithConfirm(t *testing.T) {
	a := &marketsAPIMock{requests: make(map[string]struct{})}
	ms := Markets(a, []uof.Lang{uof.LangEN, uof.LangDE}, true, false, true)
	assert.NotNil(t, ms)

	in := make(chan *uof.Message)
	out, _ := ms(in)

	// all markets for EN & DE
	m1 := <-out
	m2 := <-out
	assert.Equal(t, uof.MessageTypeMarkets, m1.Type)
	assert.Equal(t, uof.MessageTypeMarkets, m2.Type)

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
	var last *uof.Message
	for outm := range out {
		cnt++
		last = outm
	}

	// 2 variant market messages + 1 extra oddschange
	assert.Equal(t, 3, cnt)

	_, found := a.requests["en 145 sr:point_range:76+"]
	assert.True(t, found)

	_, found = a.requests["de 145 sr:point_range:76+"]
	assert.True(t, found)

	// the extra oddschange only has a header, copied from the original oddschange
	assert.Equal(t, m.Header, last.Header)
	assert.Equal(t, uof.Body{}, last.Body)
	assert.Equal(t, []byte{}, last.Raw)
}
