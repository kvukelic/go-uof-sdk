package uof

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Header struct {
	Type        MessageType     `json:"type,omitempty"`
	Scope       MessageScope    `json:"scope,omitempty"`
	Priority    MessagePriority `json:"priority,omitempty"`
	Lang        Lang            `json:"lang,omitempty"`
	SportID     int             `json:"sportID,omitempty"`
	EventID     int             `json:"eventID,omitempty"`
	EventURN    URN             `json:"eventURN,omitempty"`
	ReceivedAt  int             `json:"receivedAt,omitempty"`
	RequestedAt int             `json:"requestedAt,omitempty"`
	GeneratedAt int             `json:"generatedAt,omitempty"`
	Producer    Producer        `json:"producer,omitempty"`
	Timestamp   int             `json:"timestamp,omitempty"`
}

type Body struct {
	// queue message types
	// Ref: https://docs.betradar.com/display/BD/UOF+-+Messages
	Alive                 *Alive                 `json:"alive,omitempty"`
	BetCancel             *BetCancel             `json:"betCancel,omitempty"`
	RollbackBetSettlement *RollbackBetSettlement `json:"rollbackBetSettlement,omitempty"`
	RollbackBetCancel     *RollbackBetCancel     `json:"rollbackBetCancel,omitempty"`
	SnapshotComplete      *SnapshotComplete      `json:"snapshotComplete,omitempty"`
	OddsChange            *OddsChange            `json:"oddsChange,omitempty"`
	FixtureChange         *FixtureChange         `json:"fixtureChange,omitempty"`
	BetSettlement         *BetSettlement         `json:"betSettlement,omitempty"`
	BetStop               *BetStop               `json:"betStop,omitempty"`
	// api response message types
	Fixture *Fixture           `json:"fixture,omitempty"`
	Markets MarketDescriptions `json:"markets,omitempty"`
	Player  *Player            `json:"player,omitempty"`
	// sdk status message types
	Connection *Connection     `json:"connection,omitempty"`
	Producers  ProducersChange `json:"producerChange,omitempty"`
}

type Message struct {
	Header `json:",inline"`
	Raw    []byte `json:"-"`
	Body   `json:",inline"`
}

var uniqTimestamp func() int // ensures unique timestamp value

func init() {
	// init makes clousure for lastTs and mu
	lastTs := CurrentTimestamp()
	var mu sync.Mutex

	uniqTimestamp = func() int {
		mu.Lock()
		defer mu.Unlock()
		ts := CurrentTimestamp()
		if ts <= lastTs {
			ts++
		}
		lastTs = ts
		return ts
	}
}

// CurrentTimestamp in milliseconds
func CurrentTimestamp() int {
	return timeToTimestamp(time.Now())
}
func timeToTimestamp(t time.Time) int {
	return int(t.UnixNano()) / 1e6
}

func NewQueueMessage(routingKey string, body []byte) (*Message, error) {
	r := &Message{
		Header: Header{ReceivedAt: uniqTimestamp()},
		Raw:    body,
	}
	if err := r.parseRoutingKey(routingKey); err != nil {
		return nil, err
	}
	return r, r.unpack()
}

func (m *Message) parseRoutingKey(routingKey string) error {
	p := strings.Split(routingKey, ".")
	if len(p) < 7 {
		err := fmt.Errorf("unknown routing key: %s", routingKey)
		return E("message.RoutingKey", err)
	}
	part := func(i int) string {
		if len(p) > i && p[i] != "-" {
			return p[i]
		}
		return ""
	}
	priority := part(0)
	prematchInterest := part(1)
	liveInterest := part(2)
	messageType := part(3)
	sportID := part(4)
	eventURN := part(5)
	eventID := part(6)
	//nodeID := part(7)  // currently unused

	m.Priority.Parse(priority)
	m.Type.Parse(messageType)
	m.Scope.Parse(prematchInterest, liveInterest)

	if m.Type == MessageTypeUnknown {
		err := fmt.Errorf("unknown message type for routing key: %s", routingKey)
		return E("message.RoutingKey", err)
	}

	// if eventID != "" {
	// 	m.EventID, _ = strconv.Atoi(eventID)
	// }
	if sportID != "" {
		m.SportID, _ = strconv.Atoi(sportID)
	}
	if eventURN != "" && eventID != "" {
		m.EventURN = URN(eventURN + ":" + eventID)
		id := m.EventURN.EventID()
		if id == 0 {
			err := fmt.Errorf("unknown eventID for URN: %s", m.EventURN)
			return E("message.RoutingKey", err)
		}
		m.EventID = id
	}

	return nil
}

// after unmarshall copy producer and timestamp to Header
func (m *Message) unpack() error {
	if m.Raw == nil {
		return nil
	}
	var err error

	unmarshal := func(i interface{}) {
		err = xml.Unmarshal(m.Raw, i)
	}

	switch m.Type {
	case MessageTypeAlive:
		m.Alive = &Alive{}
		unmarshal(m.Alive)
		m.Timestamp = m.Alive.Timestamp
		m.Producer = m.Alive.Producer
	case MessageTypeBetCancel:
		m.BetCancel = &BetCancel{}
		unmarshal(m.BetCancel)
		m.Timestamp = m.BetCancel.Timestamp
		m.Producer = m.BetCancel.Producer
	case MessageTypeBetSettlement:
		m.BetSettlement = &BetSettlement{}
		unmarshal(m.BetSettlement)
		m.Timestamp = m.BetSettlement.Timestamp
		m.Producer = m.BetSettlement.Producer
	case MessageTypeBetStop:
		m.BetStop = &BetStop{}
		unmarshal(m.BetStop)
		m.Timestamp = m.BetStop.Timestamp
		m.Producer = m.BetStop.Producer
	case MessageTypeFixtureChange:
		m.FixtureChange = &FixtureChange{}
		unmarshal(m.FixtureChange)
		m.Timestamp = m.FixtureChange.Timestamp
		m.Producer = m.FixtureChange.Producer
	case MessageTypeOddsChange:
		m.OddsChange = &OddsChange{}
		unmarshal(m.OddsChange)
		m.Timestamp = m.OddsChange.Timestamp
		m.Producer = m.OddsChange.Producer
	case MessageTypeRollbackBetSettlement:
		m.RollbackBetSettlement = &RollbackBetSettlement{}
		unmarshal(m.RollbackBetSettlement)
		m.Timestamp = m.RollbackBetSettlement.Timestamp
		m.Producer = m.RollbackBetSettlement.Producer
	case MessageTypeRollbackBetCancel:
		m.RollbackBetCancel = &RollbackBetCancel{}
		unmarshal(m.RollbackBetCancel)
		m.Timestamp = m.RollbackBetCancel.Timestamp
		m.Producer = m.RollbackBetCancel.Producer
	case MessageTypeSnapshotComplete:
		m.SnapshotComplete = &SnapshotComplete{}
		unmarshal(m.SnapshotComplete)
		m.Timestamp = m.SnapshotComplete.Timestamp
		m.Producer = m.SnapshotComplete.Producer
	case MessageTypeFixture:
		fr := FixtureRsp{}
		unmarshal(&fr)
		m.GeneratedAt = int(fr.GeneratedAt.UnixNano() / 1e6)
		m.Fixture = &fr.Fixture
	case MessageTypeMarkets:
		md := &MarketsRsp{}
		unmarshal(md)
		m.Markets = md.Markets
	case MessageTypePlayer:
		pp := PlayerProfile{}
		unmarshal(&pp)
		m.GeneratedAt = int(pp.GeneratedAt.UnixNano() / 1e6)
		m.Player = &pp.Player
	default:
		err := fmt.Errorf("unknown message type %d", m.Type)
		return Notice("message.Unpack", err)
	}
	if err != nil {
		return Notice("message.Unpack", err)
	}
	return nil
}

func NewAPIMessage(lang Lang, typ MessageType, body []byte) (*Message, error) {
	m := &Message{
		Header: Header{
			Type:       typ,
			Lang:       lang,
			ReceivedAt: uniqTimestamp(),
		},
		Raw: body,
	}
	if err := m.unpack(); err != nil {
		return nil, err
	}
	return m, nil
}

func NewMarketsMessage(lang Lang, ms MarketDescriptions, requestedAt int) *Message {
	m := &Message{
		Header: Header{
			Type:        MessageTypeMarkets,
			Lang:        lang,
			ReceivedAt:  uniqTimestamp(),
			RequestedAt: requestedAt,
		},
		Body: Body{Markets: ms},
	}
	return m
}

func NewPlayerMessage(lang Lang, player *Player, requestedAt, generatedAt int) *Message {
	return &Message{
		Header: Header{
			Type:        MessageTypePlayer,
			Lang:        lang,
			ReceivedAt:  uniqTimestamp(),
			RequestedAt: requestedAt,
			GeneratedAt: generatedAt,
		},
		Body: Body{Player: player},
	}
}

func NewConnnectionMessage(status ConnectionStatus) *Message {
	ts := uniqTimestamp()
	return &Message{
		Header: Header{
			Type:       MessageTypeConnection,
			Scope:      MessageScopeSystem,
			ReceivedAt: ts,
		},
		Body: Body{
			Connection: &Connection{
				Status:    status,
				Timestamp: ts,
			},
		},
	}
}

func NewProducersChangeMessage(pc ProducersChange) *Message {
	return &Message{
		Header: Header{
			Type:       MessageTypeProducersChange,
			Scope:      MessageScopeSystem,
			ReceivedAt: uniqTimestamp(),
		},
		Body: Body{Producers: pc},
	}
}

func NewAliveTimeoutMessage(p Producer) *Message {
	return &Message{
		Header: Header{
			Type:       MessageTypeAliveTimeout,
			Scope:      MessageScopeSystem,
			ReceivedAt: uniqTimestamp(),
			Producer:   p,
		},
		Body: Body{},
	}
}

func NewFixtureMessage(lang Lang, x Fixture, raw []byte, requestedAt, generatedAt int) *Message {
	return &Message{
		Header: Header{
			Type:        MessageTypeFixture,
			EventURN:    x.URN,
			EventID:     x.ID,
			Lang:        lang,
			ReceivedAt:  uniqTimestamp(),
			RequestedAt: requestedAt,
			GeneratedAt: generatedAt,
		},
		Body: Body{Fixture: &x},
		Raw:  raw,
	}
}

const separator = byte(10)

func (m Message) Marshal() []byte {
	if m.Raw == nil {
		buf, _ := json.Marshal(m)
		return buf
	}
	buf, _ := json.Marshal(m.Header)
	buf = append(buf, separator)
	return append(buf, m.Raw...)
}

func (m *Message) Unmarshal(buf []byte) error {
	parts := bytes.SplitN(buf, []byte{separator}, 2)
	if err := json.Unmarshal(parts[0], m); err != nil {
		return Notice("message.Unmarshal", err)
	}
	if len(parts) < 2 {
		return nil
	}
	m.Raw = parts[1]
	return m.unpack()
}

// UID unique id for statefull messages
// Combines id of the content and language.
func (m *Message) UID() int {
	switch m.Type {
	case MessageTypePlayer:
		if m.Player != nil {
			return UIDWithLang(m.Player.ID, m.Lang)
		}
	case MessageTypeFixture:
		if m.Fixture != nil {
			return UIDWithLang(m.Fixture.ID, m.Lang)
		}
	}
	return 0
}

func UIDWithLang(id int, lang Lang) int {
	if id >= 0 {
		return (id << 8) | int(lang)
	}
	return -((-id << 8) | int(lang))
}

func (m *Message) Is(mt MessageType) bool {
	return m.Type == mt
}

func (m *Message) IsFixtureChange() bool {
	return m.Is(MessageTypeFixtureChange) && m.FixtureChange != nil
}

func (m *Message) IsProducersChange() bool {
	return m.Is(MessageTypeProducersChange) && m.Producers != nil
}
