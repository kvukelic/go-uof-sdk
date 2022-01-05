package uof

import (
	"encoding/xml"
	"time"
)

type ChangesRsp struct {
	Changes     []Change  `xml:"changes" json:"changes"`
	GeneratedAt time.Time `xml:"generated_at,attr,omitempty" json:"generatedAt,omitempty"`
}

// Struct for changes on Fixture_change API endpoint
//
// The End Point will return a list of xml elements with the following information:
//  - Id of the Sport Event
//  - Time of the change
// Once the customer is aware that a fixture change happened, Timeline and Summary API
// End Points can be used in order to retrieve detailed information about the change.
// Reference: https://docs.betradar.com/display/BD/UOF+-+Fixture_change+end+point
type Change struct {
	EventID    int    `json:"eventID"`
	EventURN   URN    `xml:"sport_event_id,attr" json:"eventURN"`
	UpdateTime string `xml:"update_time,attr" json:"updateTime"`
}

func (c *Change) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T Change
	var overlay struct {
		*T
	}
	overlay.T = (*T)(c)
	if err := d.DecodeElement(&overlay, &start); err != nil {
		return err
	}
	c.EventID = c.EventURN.EventID()
	return nil
}

//You will receive a fixture change when you book a match, and also when/if the
// match is added to the live odds program.
// A fixture_change message is sent when a Betradar system has made a fixture
// change it deems is important. These are typically changes that affect events
// in the near-term (e.g. a match was added that starts in the next few minutes,
// a match was delayed and starts in a couple of minutes, etc.). The message is
// short and includes a bare minimum of relevant details about the
// addition/change. The recommended practice is to always to a follow-up API
// call to lookup the updated fixture information.
// Reference: https://docs.betradar.com/display/BD/UOF+-+Fixture+change
type FixtureChange struct {
	EventID      int                `json:"eventID"`
	EventURN     URN                `xml:"event_id,attr" json:"eventURN"`
	Producer     Producer           `xml:"product,attr" json:"producer"`
	Timestamp    int                `xml:"timestamp,attr" json:"timestamp"`
	RequestID    *int               `xml:"request_id,attr,omitempty" json:"requestID,omitempty"`
	ChangeType   *FixtureChangeType `xml:"change_type,attr,omitempty" json:"changeType,omitempty"`
	StartTime    *int               `xml:"start_time,attr" json:"startTime"`
	NextLiveTime *int               `xml:"next_live_time,attr,omitempty" json:"nextLiveTime,omitempty"`
}

func (fc *FixtureChange) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T FixtureChange
	var overlay struct {
		*T
	}
	overlay.T = (*T)(fc)
	if err := d.DecodeElement(&overlay, &start); err != nil {
		return err
	}
	fc.EventID = fc.EventURN.EventID()
	return nil
}

func (fc *FixtureChange) Schedule() *time.Time {
	if fc.StartTime == nil {
		return nil
	}
	ts := time.Unix(0, int64(*fc.StartTime*int(time.Millisecond)))
	return &ts
}
