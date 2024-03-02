package api

import (
	"encoding/xml"
	"time"

	"github.com/minus5/go-uof-sdk"
)

const (
	pathMarkets       = "/v1/descriptions/{{.Lang}}/markets.xml?include_mappings={{.IncludeMappings}}"
	pathMarketVariant = "/v1/descriptions/{{.Lang}}/markets/{{.MarketID}}/variants/{{.Variant}}?include_mappings={{.IncludeMappings}}"
	pathFixture       = "/v1/sports/{{.Lang}}/sport_events/{{.EventURN}}/fixture.xml"
	pathPlayer        = "/v1/sports/{{.Lang}}/players/sr:player:{{.PlayerID}}/profile.xml"
	events            = "/v1/sports/{{.Lang}}/schedules/pre/schedule.xml?start={{.Start}}&limit={{.Limit}}"
	liveEvents        = "/v1/sports/{{.Lang}}/schedules/live/schedule.xml"
	fixtureChanges    = "/v1/sports/{{.Lang}}/fixtures/changes.xml?afterDateTime={{.DateTime}}"
)

// Markets all currently available markets for a language
func (a *API) Markets(lang uof.Lang) (uof.MarketDescriptions, error) {
	var mr marketsRsp
	err := a.getAs(&mr, pathMarkets, &params{Lang: lang})
	return mr.Markets, err
}

func (a *API) MarketVariant(lang uof.Lang, marketID int, variant string) (uof.MarketDescriptions, error) {
	var mr marketsRsp
	err := a.getAs(&mr, pathMarketVariant, &params{Lang: lang, MarketID: marketID, Variant: variant})
	return mr.Markets, err
}

func (a *API) Player(lang uof.Lang, playerID int) (uof.PlayerProfile, error) {
	var pr playerRsp
	err := a.getAs(&pr, pathPlayer, &params{Lang: lang, PlayerID: playerID})
	return uof.PlayerProfile{Player: pr.Player, GeneratedAt: pr.GeneratedAt}, err
}

// Fixture lists the fixture for a specified sport event
func (a *API) Fixture(lang uof.Lang, eventURN uof.URN) (uof.FixtureRsp, error) {
	buf, err := a.get(pathFixture, &params{Lang: lang, EventURN: eventURN})
	if err != nil {
		return uof.FixtureRsp{}, err
	}
	var fr fixtureRsp
	if err := xml.Unmarshal(buf, &fr); err != nil {
		return uof.FixtureRsp{}, uof.Notice("unmarshal", err)
	}
	fr.Fixture.ID = eventURN.EventID()
	fr.Fixture.URN = eventURN
	return uof.FixtureRsp{Fixture: fr.Fixture, GeneratedAt: fr.GeneratedAt, Raw: buf}, err
}

// FixtureChanges retrieves a list of fixture changes starting from the time
// instant 'from'. It also returns the timestamp of the API response.
func (a *API) FixtureChanges(lang uof.Lang, from time.Time) (uof.ChangesRsp, error) {
	var fcr fixtureChangesRsp
	dateTime := from.UTC().Format("2006-01-02T15:04:05")
	err := a.getAs(&fcr, fixtureChanges, &params{Lang: lang, DateTime: dateTime})
	return uof.ChangesRsp{Changes: fcr.Changes, GeneratedAt: fcr.GeneratedAt}, err
}

// FixtureSchedule gets fixtures from schedule endpoints.
//
// First, it fetches all currently live events from the live/schedule endpoint.
// Then, it begins fetching upcoming events with prematch offer from the
// paginated pre/schedule enpoint. It keeps requesting batches of fixtures for
// these events until it reaches events whose scheduled start time exceeds the
// time instant 'to', or until the maximum fixture count 'max' has been
// reached. It will always, at minimum, attempt to get the schedule of
// currently live events and the first page of upcoming prematch events.
//
// Due to the pagination of the latter endpoint, the fixtures are returned
// asynchronously via a channel. A separate channel, that receives and buffers
// only the earliest timestamp of all received responses, is also returned.
func (a *API) FixtureSchedule(lang uof.Lang, to time.Time, max int) (<-chan uof.FixtureRsp, <-chan error) {
	errc := make(chan error)
	out := make(chan uof.FixtureRsp)
	go func() {
		defer close(out)
		defer close(errc)

		fixtureCount := 0
		latestSchedule := time.Time{}

		pushFixtures := func(rsp scheduleRsp) {
			fixtureCount += len(rsp.Fixtures)
			for _, f := range rsp.Fixtures {
				latestSchedule = laterNonZero(latestSchedule, f.Fixture.Scheduled)
				out <- uof.FixtureRsp{Fixture: f.Fixture, GeneratedAt: rsp.GeneratedAt, Raw: f.Raw}
			}
		}

		// step 1: get schedule of currently live events
		var liveRsp scheduleRsp
		err := a.getAs(&liveRsp, liveEvents, &params{Lang: lang})
		if err != nil {
			errc <- err
			return
		}
		pushFixtures(liveRsp)

		// step 2: get schedule of active prematch events (until 'to')
		limit := 1000
		for start := 0; ; start += limit {
			var preRsp scheduleRsp
			err = a.getAs(&preRsp, events, &params{Lang: lang, Start: start, Limit: limit})
			if err != nil {
				errc <- err
				return
			}
			if len(preRsp.Fixtures) < 1 {
				break // no more schedule pages
			}
			pushFixtures(preRsp)
			if max >= 0 && fixtureCount >= max {
				break // reached configured maximum count of fixtures
			}
			if !to.IsZero() && latestSchedule.After(to) {
				break // reached configured fixture scheduled time limit
			}
		}
	}()

	return out, errc
}

func laterNonZero(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.After(a)) {
		return b
	}
	return a
}

type marketsRsp struct {
	Markets uof.MarketDescriptions `xml:"market,omitempty" json:"markets,omitempty"`
	// unused
	// ResponseCode string   `xml:"response_code,attr,omitempty" json:"responseCode,omitempty"`
	// Location     string   `xml:"location,attr,omitempty" json:"location,omitempty"`
}

type playerRsp struct {
	Player      uof.Player `xml:"player" json:"player"`
	GeneratedAt time.Time  `xml:"generated_at,attr,omitempty" json:"generatedAt,omitempty"`
}

type fixtureRsp struct {
	Fixture     uof.Fixture `xml:"fixture" json:"fixture"`
	GeneratedAt time.Time   `xml:"generated_at,attr,omitempty" json:"generatedAt,omitempty"`
}

func (fr *fixtureRsp) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var err error
	if start.Name.Local == "tournament_info" {
		err = d.DecodeElement(&fr.Fixture, &start)
		if err == nil {
			for _, attr := range start.Attr {
				if attr.Name.Local == "generated_at" {
					fr.GeneratedAt, err = time.Parse(time.RFC3339Nano, attr.Value)
					break
				}
			}
		}
	} else {
		type T fixtureRsp
		var overlay T
		err = d.DecodeElement(&overlay, &start)
		fr.Fixture = overlay.Fixture
		fr.GeneratedAt = overlay.GeneratedAt
	}
	return err
}

type fixtureChangesRsp struct {
	Changes     []uof.Change `xml:"fixture_change,omitempty" json:"fixtureChange,omitempty"`
	GeneratedAt time.Time    `xml:"generated_at,attr,omitempty" json:"generatedAt,omitempty"`
}

type scheduleRsp struct {
	Fixtures    []scheduleFixture `json:"sportEvents,omitempty"`
	GeneratedAt time.Time         `json:"generatedAt,omitempty"`
}

type scheduleFixture struct {
	Fixture uof.Fixture `json:"sportEvent,omitempty"`
	Raw     []byte      `json:"-"`
}

func (sr *scheduleRsp) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T struct {
		Raw         []byte    `xml:",innerxml"`
		GeneratedAt time.Time `xml:"generated_at,attr,omitempty"`
	}
	var t T
	if err := d.DecodeElement(&t, &start); err != nil {
		return err
	}
	var fs []scheduleFixture
	for len(t.Raw) > 0 {
		var w scheduleFixtureWrapper
		if err := xml.Unmarshal(t.Raw, &w); err != nil {
			return err
		}
		fs = append(fs, scheduleFixture{Fixture: w.Fixture, Raw: t.Raw[:w.rawEnd]})
		t.Raw = t.Raw[w.rawEnd:]
	}
	sr.Fixtures = fs
	sr.GeneratedAt = t.GeneratedAt
	return nil
}

type scheduleFixtureWrapper struct {
	Fixture uof.Fixture
	rawEnd  int64
}

func (t *scheduleFixtureWrapper) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	if err := d.DecodeElement(&t.Fixture, &start); err != nil {
		return err
	}
	t.rawEnd = d.InputOffset()
	return nil
}
