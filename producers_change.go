package uof

type ProducersChange []ProducerChange

type ProducerChange struct {
	Producer          Producer       `json:"producer,omitempty"`
	Status            ProducerStatus `json:"status,omitempty"`
	Timestamp         int            `json:"timestamp,omitempty"`
	RecoveryID        int            `json:"recoveryID,omitempty"`
	RecoveryTimestamp int            `json:"recoveryTimestamp,omitempty"`
	Comment           string         `json:"comment,omitempty"`
}

func (p *ProducersChange) Add(producer Producer, timestamp int) {
	*p = append(*p, ProducerChange{Producer: producer, Timestamp: timestamp})
}

type ProducerStatus int8

const (
	ProducerStatusDown         ProducerStatus = -1
	ProducerStatusActive       ProducerStatus = 1
	ProducerStatusInRecovery   ProducerStatus = 2
	ProducerStatusPostRecovery ProducerStatus = 3
)

func (p ProducerStatus) String() string {
	switch p {
	case ProducerStatusDown:
		return "down"
	case ProducerStatusActive:
		return "active"
	case ProducerStatusInRecovery:
		return "recovery"
	case ProducerStatusPostRecovery:
		return "postrecovery"
	}
	return ""
}
