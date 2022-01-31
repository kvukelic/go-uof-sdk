package sdk

import (
	"context"
	"time"

	"github.com/minus5/go-uof-sdk"
	"github.com/minus5/go-uof-sdk/api"
	"github.com/minus5/go-uof-sdk/pipe"
	"github.com/minus5/go-uof-sdk/queue"
)

var defaultLanguages = uof.Languages("en,de")

// ErrorListenerFunc listens all SDK errors
type ErrorListenerFunc func(err error)

// Config is active SDK configuration
type Config struct {
	BookmakerID     string
	Token           string
	Producers       []queue.ProducerConfig
	FixturesTo      time.Time
	FixturesMax     int
	Variants        bool
	ExtraNamespaces []string
	Stages          []pipe.InnerStage
	Replay          func(*api.ReplayAPI) error
	Env             uof.Environment
	Staging         bool
	Languages       []uof.Lang
	ErrorListener   ErrorListenerFunc
}

// Option sets attributes on the Config.
type Option func(*Config)

// Run starts uof connector.
//
// Call to Run blocks until stopped by context, or error occurred.
// Order in which options are set is not important.
// Credentials and one of Callback or Pipe are functional minimum.
func Run(ctx context.Context, options ...Option) error {
	c := config(options...)
	qc, apiConn, err := connect(ctx, c)
	if err != nil {
		return err
	}
	if c.Replay != nil {
		rpl, err := api.Replay(ctx, c.Token)
		if err != nil {
			return err
		}
		if err := c.Replay(rpl); err != nil {
			return err
		}
	}

	stages := []pipe.InnerStage{
		pipe.Markets(apiConn, c.Languages, c.Variants),
		pipe.Fixture(apiConn, c.Languages, c.FixturesTo, c.FixturesMax),
		pipe.Player(apiConn, c.Languages),
		pipe.BetStop(),
		pipe.Recovery(apiConn),
	}

	if len(c.ExtraNamespaces) > 0 {
		stages = append(stages, pipe.ExtraFixtures(apiConn, c.Languages, c.ExtraNamespaces))
	}

	stages = append(stages, c.Stages...)

	errc := pipe.Build(
		queue.WithProducerHandling(ctx, qc, c.Producers),
		stages...,
	)
	return firstErr(errc, c.ErrorListener)
}

// listen errors
func firstErr(errc <-chan error, errorListener ErrorListenerFunc) error {
	var err error
	for e := range errc {
		if err == nil {
			err = e
		}
		if errorListener != nil {
			errorListener(e)
		}
	}
	return err
}

// apply configuration options
func config(options ...Option) Config {
	// defaults
	c := &Config{
		Producers:       make([]queue.ProducerConfig, 0),
		Variants:        true,
		ExtraNamespaces: make([]string, 0),
		Languages:       defaultLanguages,
		Env:             uof.Production,
	}
	for _, o := range options {
		o(c)
	}
	return *c
}

// connect to the queue and api
func connect(ctx context.Context, c Config) (*queue.Connection, *api.API, error) {
	conn, err := queue.Dial(ctx, c.Env, c.BookmakerID, c.Token)
	if err != nil {
		return nil, nil, err
	}
	stg, err := api.Dial(ctx, c.Env, c.Token)
	if err != nil {
		return nil, nil, err
	}
	return conn, stg, nil
}

// PRODUCER SUBSCRIPTION OPTIONS

// Subscribe registers a producer for tracking and handling of its state and
// state changes. When a producer is registered, it will appear in any
// ProducersChange messages inserted into the stream of messages in the
// pipeline, which will also enable any inner stages with state-dependent
// behaviour (e.g. recovery stage) to fully function in regards to that
// producer.
//
// The function may also receive configuration options that define various
// parameters in the producer's state handling.
func Subscribe(producer uof.Producer, opts ...ProducerOption) Option {
	prod := queue.NewProducerConfig(producer)
	for _, o := range opts {
		o(&prod)
	}
	return func(c *Config) {
		c.Producers = append(c.Producers, prod)
	}
}

type ProducerOption func(*queue.ProducerConfig)

// RecoverFrom sets the beginning of the recovery window for the initial
// recovery of the producer. As with any recovery, if the set window
// exceeds recovery window duration limit for the producer, full recovery
// of all events with active offer will be requested.
//
// If not used, the beginning of the recovery window will be set to the
// moment of registering the producer via Subscribe.
func RecoverFrom(timestamp int) ProducerOption {
	return func(pc *queue.ProducerConfig) {
		pc.SetRecoveryTimestamp(timestamp)
	}
}

// MaxInterval sets the maximum allowed amount of time between timestamps of
// two subsequent alive messages for the producer. Exceeding this threshold
// while the producer is active or in recovery will result in it being
// considered down.
//
// If not used, or if the parameter is set to a negative value, no checks on
// the interval between subsequent alive message timestamps will be made.
func MaxInterval(d time.Duration) ProducerOption {
	return func(pc *queue.ProducerConfig) {
		pc.SetMaxIntervalDuration(d)
	}
}

// MaxDelay sets the maximum allowed amount of delay between a timestamp of an
// alive message, as reported from the source server, and the time at which SDK
// received the message. Exceeding this threshold while the producer is active
// will result in it being considered down.
//
// If not used, or if the parameter is set to a negative value, no checks on
// the delay of alive messages will be made.
func MaxDelay(d time.Duration) ProducerOption {
	return func(pc *queue.ProducerConfig) {
		pc.SetMaxDelayDuration(d)
	}
}

// Timeout sets the maximum allowed amount of time between receiving two alive
// messages for the producer. Exceeding this threshold while the producer is
// active will result in it being considered down.
//
// If not used, or if the parameter is set to a non-positive value, no checks
// on the duration between receiving alive messages will be made.
func Timeout(d time.Duration) ProducerOption {
	return func(pc *queue.ProducerConfig) {
		pc.SetTimeoutDuration(d)
	}
}

// OTHER SDK OPTIONS

// Credentials for establishing connection to the uof queue and api.
func Credentials(bookmakerID, token string) Option {
	return func(c *Config) {
		c.BookmakerID = bookmakerID
		c.Token = token
	}
}

// Languages for api calls.
//
// Statefull messages (markets, players, fixtures) will be served in all this
// languages. Each language requires separate call to api. If not specified
// `defaultLanguages` will be used.
func Languages(langs []uof.Lang) Option {
	return func(c *Config) {
		c.Languages = langs
	}
}

// Staging forces use of staging environment instead of production.
func Staging() Option {
	return func(c *Config) {
		c.Env = uof.Staging
		c.Staging = true
	}
}

// Replay forces use of replay environment.
// Callback will be called to start replay after establishing connection.
func Replay(cb func(*api.ReplayAPI) error) Option {
	return func(c *Config) {
		c.Env = uof.Replay
		c.Replay = cb
	}
}

// Consumer sets chan consumer of the SDK messages stream.
//
// Consumer should range over `in` chan and handle all messages.
// In chan will be closed on SDK tear down.
// If the consumer returns an error it is handled as fatal. Immediately closes SDK connection.
// Can be called multiple times.
func Consumer(consumer pipe.ConsumerStage) Option {
	return func(c *Config) {
		c.Stages = append(c.Stages, pipe.Consumer(consumer))
	}
}

// BufferedConsumer same as consumer but with buffered `in` chan of size `buffer`.
func BufferedConsumer(consumer pipe.ConsumerStage, buffer int) Option {
	return func(c *Config) {
		c.Stages = append(c.Stages, pipe.BufferedConsumer(consumer, buffer))
	}
}

// Callback sets handler for all messages.
//
// If returns error will break the pipe and force exit from sdk.Run.
// Can be called multiple times.
func Callback(cb func(m *uof.Message) error) Option {
	return func(c *Config) {
		c.Stages = append(c.Stages, pipe.Simple(cb))
	}
}

// FixturePreload configures retrieval of live and prematch fixtures at start-up.
//
// Fixtures for all events which start before `to` time are fetched. The `max`
// value sets a rough maximum of fixtures to be preloaded. A minimum set of
// fixtures for currently running events and the first page of active scheduled
// prematch events is always fetched.
//
// There is a special endpoint to get almost all fixtures before initiating
// recovery. This endpoint is designed to significantly reduce the number of API
// calls required during recovery.
//
// Ref: https://docs.betradar.com/display/BD/UOF+-+Fixtures+in+the+API
func FixturePreload(to time.Time, max int) Option {
	return func(c *Config) {
		c.FixturesTo = to
		c.FixturesMax = max
	}
}

// NoVariants disables variant market description messages.
func NoVariants() Option {
	return func(c *Config) {
		c.Variants = false
	}
}

// ListenErrors sets ErrorListener for all SDK errors
func ListenErrors(listener ErrorListenerFunc) Option {
	return func(c *Config) {
		c.ErrorListener = listener
	}
}

// ExtraFixtureTypes allows for definition of event types for which the event fixture
// will be requested from the API upon each received feed message. This is to circumvent
// the lack of preload options on Betradar's side for various event types.
//
// The event types are defined as a combination of two lists: list of Betradar URN prefix
// types and list of Betradar URN event types. Such a definition means that any events that
// combine any listed prefix type and any listed event type in their URN will be handled.
// URN type constants are provided by the top-level SDK package.
func ExtraFixtureTypes(evPrefixes []uof.URNPrefixType, evTypes []uof.URNEventType) Option {
	return func(c *Config) {
		for _, p := range evPrefixes {
			for _, t := range evTypes {
				c.ExtraNamespaces = append(c.ExtraNamespaces, uof.EventNamespace(p, t))
			}
		}
	}
}
