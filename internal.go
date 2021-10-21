package uof

import "time"

type Connection struct {
	Status    ConnectionStatus `json:"status"`
	Timestamp int              `json:"timestamp,omitempty"`
}

type ConnectionStatus int8

const (
	ConnectionStatusUp ConnectionStatus = iota
	ConnectionStatusDown
)

func (cs ConnectionStatus) String() string {
	switch cs {
	case ConnectionStatusDown:
		return "down"
	case ConnectionStatusUp:
		return "up"
	default:
		return "?"
	}
}

type AliveConfiguration struct {
	Timeout     time.Duration
	MaxDelay    time.Duration
	MaxInterval time.Duration
}

func DefaultAliveConfiguration() AliveConfiguration {
	return AliveConfiguration{
		Timeout:     0,
		MaxDelay:    -1,
		MaxInterval: 0,
	}
}
