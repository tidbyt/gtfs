package storage

import (
	"time"
)

// TODO:
// - Remove UpdatedAt from FeedMetadata
// - s/sha256/hash/
// - s/FeedMetadata/Feed/
// - Remove FeedStartDate/FeedEndDate

type Storage interface {
	// Retrieves all feed metadata records matching the given
	// filter.
	ListFeeds(filter ListFeedsFilter) ([]*FeedMetadata, error)

	// Writes a FeedMetadata record. If a record with the same URL
	// and hash exists, it is updated.
	WriteFeedMetadata(metadata *FeedMetadata) error

	// Retrieves all feed requests matching the given URL. If the
	// URL is blank, all requests are returned.
	ListFeedRequests(url string) ([]FeedRequest, error)

	// Writes a FeedRequest record. If a record with the same URL
	// exists, it is updated. All consumers included in the
	// request will be created/updated. Missing consumers will
	// _not_ be removed.
	WriteFeedRequest(req FeedRequest) error

	DeleteFeedMetadata(url string, sha256 string) error // TODO: delete this

	// Gets a reader for the feed with the given hash.
	GetReader(feed string) (FeedReader, error)

	// Gets a writer for the feed with the given hash.
	GetWriter(feed string) (FeedWriter, error)
}

type ListFeedsFilter struct {
	// If set, only include feeds with the given URL.
	URL string

	// If set, only include feeds with the given hash.
	SHA256 string
}

// A request to download a static GTFS feed at the given URL. The same
// URL can be requested by multiple consumers of the data, possibly
// with different HTTP headers holding API keys.
type FeedRequest struct {
	URL         string
	RefreshedAt time.Time
	Consumers   []FeedConsumer
}

type FeedConsumer struct {
	Name      string
	Headers   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Metadata for a downloaded static GTFS feed. The parsed data can be
// accessed via FeedReader.
type FeedMetadata struct {
	URL               string
	SHA256            string
	RetrievedAt       time.Time
	Timezone          string
	CalendarStartDate string
	CalendarEndDate   string
	FeedStartDate     string
	FeedEndDate       string
	MaxArrival        string
	MaxDeparture      string
}

// Writes GTFS records for a single feed.
//
// As stop_times.txt tends to be very large, BeginStopTimes() and
// EndStopTimes() are called before and after all calls to
// WriteStopTime(), allowing transactions/batching/whathaveyou.
type FeedWriter interface {
	WriteAgency(agency *Agency) error
	WriteStop(stop *Stop) error
	WriteRoute(route *Route) error
	WriteTrip(trip *Trip) error
	BeginTrips() error
	EndTrips() error
	WriteCalendar(cal *Calendar) error
	WriteCalendarDate(caldate *CalendarDate) error
	WriteStopTime(stopTime *StopTime) error
	BeginStopTimes() error
	EndStopTimes() error
	Close() error
}

type FeedReader interface {
	Agencies() ([]*Agency, error)
	Stops() ([]*Stop, error)
	Routes() ([]*Route, error)
	Trips() ([]*Trip, error)
	StopTimes() ([]*StopTime, error)
	Calendars() ([]*Calendar, error)
	CalendarDates() ([]*CalendarDate, error)

	// Services IDs for all services active on the given
	// date. Date is given as YYYYMMDD.
	ActiveServices(date string) ([]string, error)

	// Map from trip_id to [min, max] stop_sequence for that trip,
	// as per stop_times. This is useful for filtering out first
	// or last stops of a trip.
	MinMaxStopSeq() (map[string][2]uint32, error)

	// List of stop_times and associated data matching the
	// provided filter.
	StopTimeEvents(filter StopTimeEventFilter) ([]*StopTimeEvent, error)

	// List of all distinct routes with direction data passing
	// through a stop, with all distinct headsigns.
	RouteDirections(stopID string) ([]*RouteDirection, error)

	// List of stops near given lat/lng, ordered by distance. At
	// most limit results (pass 0 for no limit.) Optionally
	// filtered to only include stops with routes of the given
	// type passing through.
	//
	// Currently, stations are returned when available. Stops that
	// lack a parent_station are also included, to accommodate
	// feeds without stations. This behavior should probable be
	// configurable/optional.
	NearbyStops(lat float64, lng float64, limit int, routeTypes []RouteType) ([]Stop, error)
}

// Holds all Headsigns for trips passing through a stop, for a given
// route and direction.
type RouteDirection struct {
	StopID      string
	RouteID     string
	DirectionID int8
	Headsigns   []string
}

// Filter for StopTimeEvents()
type StopTimeEventFilter struct {
	// Limit results to events for the given stop ID. This can
	// reference a parent station, in which case all sub-stops are
	// included.
	StopID string

	// Limit results to a set of services, a specific route,
	// a set of route types and/or a set of trips.
	ServiceIDs []string
	RouteID    string
	RouteTypes []RouteType
	TripIDs    []string

	// Limit results to a direction. Pass -1 to include all
	// directions.
	DirectionID int

	// Limit results to stop_times with arrival/departure within a
	// certain range (inclusive.) Times given as "HHMMSS".
	ArrivalStart   string
	ArrivalEnd     string
	DepartureStart string
	DepartureEnd   string
}

// Holds informaion about a stop_time record. Includes information
// about the associated trip, route and stop, as well as parent
// station of the stop (if any.)
type StopTimeEvent struct {
	StopTime      *StopTime
	Trip          *Trip
	Route         *Route
	Stop          *Stop
	ParentStation *Stop
}
