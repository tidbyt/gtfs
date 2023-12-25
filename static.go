package gtfs

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"tidbyt.dev/gtfs/storage"
)

type Static struct {
	Metadata *storage.FeedMetadata
	Reader   storage.FeedReader

	minMaxStopSeqByTripID map[string][2]uint32
	location              *time.Location
	maxDeparture          time.Duration
}

func NewStatic(reader storage.FeedReader, metadata *storage.FeedMetadata) (*Static, error) {
	location, err := time.LoadLocation(metadata.Timezone)
	if err != nil {
		return nil, fmt.Errorf("loading timezone: %w", err)
	}

	// TODO: get rid of this. annoying.
	minMaxStopSeqByTripID, err := reader.MinMaxStopSeq()
	if err != nil {
		return nil, fmt.Errorf("getting min/max stop seq by trip: %w", err)
	}

	mH, errH := strconv.Atoi(metadata.MaxDeparture[0:2])
	mM, errM := strconv.Atoi(metadata.MaxDeparture[2:4])
	mS, errS := strconv.Atoi(metadata.MaxDeparture[4:6])
	if errH != nil || errM != nil || errS != nil {
		return nil, fmt.Errorf("parsing max departure")
	}
	maxDeparture := time.Duration(mH)*time.Hour + time.Duration(mM)*time.Minute + time.Duration(mS)*time.Second

	return &Static{
		Metadata:              metadata,
		Reader:                reader,
		minMaxStopSeqByTripID: minMaxStopSeqByTripID,
		location:              location,
		maxDeparture:          maxDeparture,
	}, nil
}

// Returns stops ordered by distance from lat,lon.
//
// If limit is >0, at most limit stops are returned.
//
// If types is provided, then only stops along routes of at least one
// of the types is returned. E.g., pass []storage.RouteType{storage.RouteTypeBus} to
// only receive bus stops.
//
// Only stations (location_type=1) and stops (location_type=0)
// _without_ parent station are returned.
func (s Static) NearbyStops(lat float64, lon float64, limit int, types []storage.RouteType) ([]storage.Stop, error) {
	stops, err := s.Reader.NearbyStops(lat, lon, limit, types)
	if err != nil {
		return nil, fmt.Errorf("getting nearby stops: %w", err)
	}
	return stops, nil
}

// Returns all routes and direction for a stop
//
// In GTFS, direction and headsign are properties of a trip, and all
// trips belong to some route. To be able to let a user select
// e.g. "Stop 5, Route L, to Canarsie", we need this.
//
// NOTE: Headsign can also be set on stop_time, which messes this up
// quite a bit.
func (s Static) RouteDirections(stopID string) ([]*storage.RouteDirection, error) {
	rds, err := s.Reader.RouteDirections(stopID)
	if err != nil {
		return nil, err
	}
	return rds, nil
}

type Departure struct {
	StopID       string
	RouteID      string
	TripID       string
	StopSequence uint32
	DirectionID  int8
	Time         time.Time
	Headsign     string
	Delay        time.Duration
}

// Translates a time offset into a GTFS style HHMMSS string.
func gtfsDate(offset time.Duration) string {
	h := int(offset.Hours())
	m := int(offset.Minutes()) - h*60
	s := int(offset.Seconds()) - h*3600 - m*60
	return fmt.Sprintf("%02d%02d%02d", h, m, s)
}

// This is a helper to translate a time window into a GTFS friendly
// list of time range per date.
type span struct {
	Date  string
	Start string
	End   string
}

// Computes list of all time ranges that must be inspected for a GTFS
// stop time lookup.
func rangePerDate(start time.Time, window time.Duration, maxTrip time.Duration) []span {
	end := start.Add(window)

	spans := []span{}

	date := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())

	// XXX: One issue here is (I think) that the day after the
	// time window can possibly push back a departure into
	// previous day on DST change. E.g., if next day has departure
	// at 00:01, then that's noon-12+00:01, and if DST begins on
	// that day then the departure time will be 23:0 on previous
	// day. Gonna ignore this for now.

	for today := date.AddDate(0, 0, -1); today.Before(end); today = today.AddDate(0, 0, 1) {
		noon := time.Date(today.Year(), today.Month(), today.Day(), 12, 0, 0, 0, today.Location())
		tomorrow := today.AddDate(0, 0, 1)

		span := span{Date: today.Format("20060102")}

		if start.Before(today) {
			// window starts before this day
		} else if start.Before(tomorrow) {
			// window starts on this day
			span.Start = gtfsDate(start.Sub(noon) + 12*time.Hour)
		} else {
			// window starts after this day
			x := start.Sub(noon) + 12*time.Hour
			if x <= maxTrip {
				// potentially during today's overflow trips
				span.Start = gtfsDate(x)
			} else {
				// definitely not during today's overflow trips
				continue
			}
		}

		if end.Before(tomorrow) {
			// window ends on this day
			span.End = gtfsDate(end.Sub(noon) + 12*time.Hour)
		} else {
			// window ends in the future, possibly during
			// today's overflow trips
			x := end.Sub(noon) + 12*time.Hour
			if x <= maxTrip {
				span.End = gtfsDate(x)
			}
		}

		spans = append(spans, span)
	}

	return spans
}

// Returns departures from a particular stop in a time window.
//
// - numDepartures (if >= 0) limits the number of results
// - routeID (if != "") limits results to a route
// - directionID (if >= 0) limits results to a directionID
func (s Static) Departures(
	stopID string,
	windowStart time.Time,
	windowLength time.Duration,
	numDepartures int,
	routeID string,
	directionID int8,
	routeTypes []storage.RouteType,
) ([]Departure, error) {

	departures := []Departure{}

	if numDepartures == 0 {
		return departures, nil
	}

	// All computations are done in the GTFS timezone, but
	// Departure.Time will be returned in the timezone used by
	// caller.
	origTz := windowStart.Location()
	startTime := windowStart.In(s.location)
	endTime := startTime.Add(windowLength)

	// Query for departures for each day in the window
	for _, span := range rangePerDate(startTime, windowLength, s.maxDeparture) {

		// Get active services for this day
		serviceIDs, err := s.Reader.ActiveServices(span.Date)
		if err != nil {
			return nil, err
		}
		if len(serviceIDs) == 0 {
			continue
		}

		// stop time events for the day's span
		events, err := s.Reader.StopTimeEvents(storage.StopTimeEventFilter{
			StopID:         stopID,
			DirectionID:    int(directionID),
			ServiceIDs:     serviceIDs,
			RouteID:        routeID,
			RouteTypes:     routeTypes,
			DepartureStart: span.Start,
			DepartureEnd:   span.End,
		})
		if err != nil {
			return nil, err
		}

		sort.SliceStable(events, func(i, j int) bool {
			return events[i].StopTime.DepartureTime() < events[j].StopTime.DepartureTime()
		})

		for _, event := range events {

			// Compute the departure time in original timezone
			date, _ := time.ParseInLocation("20060102", span.Date, s.location)
			dateNoon := time.Date(date.Year(), date.Month(), date.Day(), 12, 0, 0, 0, s.location)
			departureTime := dateNoon.Add(-12 * time.Hour).Add(event.StopTime.DepartureTime()).In(origTz)
			if departureTime.After(endTime) {
				// TODO: this shouldn't be possible
				break
			}

			// Ignore the last stop on a trip, since it's not a
			// boardable departure.
			minMaxSeq := s.minMaxStopSeqByTripID[event.Trip.ID]
			if event.StopTime.StopSequence >= uint32(minMaxSeq[1]) {
				continue
			}

			headsign := event.StopTime.Headsign
			if headsign == "" {
				headsign = event.Trip.Headsign
			}

			if !startTime.After(departureTime) {
				departures = append(departures, Departure{
					StopID:       event.Stop.ID,
					RouteID:      event.Trip.RouteID,
					TripID:       event.Trip.ID,
					StopSequence: event.StopTime.StopSequence,
					DirectionID:  event.Trip.DirectionID,
					Time:         departureTime,
					Headsign:     headsign,
				})
			}
		}
	}

	// Sort by departure time
	sort.SliceStable(departures, func(i, j int) bool {
		return departures[i].Time.Before(departures[j].Time)
	})

	if numDepartures >= 0 && len(departures) > numDepartures {
		departures = departures[:numDepartures]
	}

	return departures, nil
}
