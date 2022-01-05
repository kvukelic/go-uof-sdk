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

// Fixture lists the fixture for a specified sport event
func (a *API) Fixture(lang uof.Lang, eventURN uof.URN) (uof.FixtureRsp, error) {
	var fr fixtureRsp
	err := a.getAs(&fr, pathFixture, &params{Lang: lang, EventURN: eventURN})
	return uof.FixtureRsp{Fixture: fr.Fixture, GeneratedAt: fr.GeneratedAt}, err
}

func (a *API) Player(lang uof.Lang, playerID int) (uof.PlayerProfile, error) {
	var pr playerRsp
	err := a.getAs(&pr, pathPlayer, &params{Lang: lang, PlayerID: playerID})
	return uof.PlayerProfile{Player: pr.Player, GeneratedAt: pr.GeneratedAt}, err
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
	if start.Name.Local == "tournament_info" {
		for _, attr := range start.Attr {
			if attr.Name.Local == "generated_at" {
				tsp, err := time.Parse(time.RFC3339, attr.Value)
				if err != nil {
					return err
				}
				fr.GeneratedAt = tsp
				break
			}
		}
		return d.DecodeElement(&fr.Fixture, &start)
	} else {
		var overlay struct {
			Fixture     uof.Fixture `xml:"fixture"`
			GeneratedAt time.Time   `xml:"generated_at,attr,omitempty"`
		}
		defer func() {
			fr.Fixture = overlay.Fixture
			fr.GeneratedAt = overlay.GeneratedAt
		}()
		return d.DecodeElement(&overlay, &start)
	}
}

type fixtureChangesRsp struct {
	Changes     []uof.Change `xml:"fixture_change,omitempty" json:"fixtureChange,omitempty"`
	GeneratedAt time.Time    `xml:"generated_at,attr,omitempty" json:"generatedAt,omitempty"`
}

type scheduleRsp struct {
	Fixtures    []uof.Fixture `xml:"sport_event,omitempty" json:"sportEvent,omitempty"`
	GeneratedAt time.Time     `xml:"generated_at,attr,omitempty" json:"generatedAt,omitempty"`
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
	errc := make(chan error, 1)
	out := make(chan uof.FixtureRsp)
	go func() {
		defer close(out)
		defer close(errc)

		fixtureCount := 0
		lastSchedule := time.Time{}

		sendFixtures := func(rsp scheduleRsp) {
			fixtureCount += len(rsp.Fixtures)
			for _, f := range rsp.Fixtures {
				lastSchedule = laterNonZero(lastSchedule, f.Scheduled)
				out <- uof.FixtureRsp{Fixture: f, GeneratedAt: rsp.GeneratedAt}
			}
		}

		// step 1: get schedule of currently live events
		var liveRsp scheduleRsp
		err := a.getAs(&liveRsp, liveEvents, &params{Lang: lang})
		if err != nil {
			errc <- err
			return
		}
		sendFixtures(liveRsp)

		// step 2: get schedule of active prematch events (until 'to')
		limit := 1000
		for start, done := 0, false; !done; start += limit {
			var preRsp scheduleRsp
			err = a.getAs(&preRsp, events, &params{Lang: lang, Start: start, Limit: limit})
			if err != nil {
				errc <- err
				return
			}
			sendFixtures(preRsp)
			done = fixtureCount >= max || lastSchedule.After(to)
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
