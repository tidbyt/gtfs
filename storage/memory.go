package storage

import (
	"fmt"
	"sort"
	"time"
)

// In memory implementation of Storage below

type memoryMetadataKey struct {
	URL    string
	SHA256 string
}

type memoryRequestKey struct {
	URL      string
	Consumer string
}

type MemoryStorage struct {
	Feeds    map[string]*MemoryStorageFeed
	Metadata map[memoryMetadataKey]*FeedMetadata
	Requests map[memoryRequestKey]FeedRequest
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		Feeds:    map[string]*MemoryStorageFeed{},
		Metadata: map[memoryMetadataKey]*FeedMetadata{},
		Requests: map[memoryRequestKey]FeedRequest{},
	}
}

func (s *MemoryStorage) ListFeeds(filter ListFeedsFilter) ([]*FeedMetadata, error) {
	feeds := []*FeedMetadata{}
	for _, metadata := range s.Metadata {
		if filter.URL != "" && metadata.URL != filter.URL {
			continue
		}
		if filter.SHA256 != "" && metadata.SHA256 != filter.SHA256 {
			continue
		}
		feeds = append(feeds, metadata)
	}
	sort.Slice(feeds, func(i, j int) bool {
		return feeds[i].RetrievedAt.After(feeds[j].RetrievedAt)
	})
	return feeds, nil
}

func (s *MemoryStorage) ListFeedRequests(url string) ([]FeedRequest, error) {
	reqs := []FeedRequest{}

	for _, req := range s.Requests {
		if url != "" && req.URL != url {
			continue
		}
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (s *MemoryStorage) WriteFeedMetadata(feed *FeedMetadata) error {
	s.Metadata[memoryMetadataKey{feed.URL, feed.SHA256}] = feed
	return nil
}

func (s *MemoryStorage) WriteFeedRequest(req FeedRequest) error {
	return nil
}

func (s *MemoryStorage) DeleteFeedMetadata(url string, sha256 string) error {
	key := memoryMetadataKey{url, sha256}
	if _, found := s.Metadata[key]; !found {
		return fmt.Errorf("Feed not found")
	}
	delete(s.Metadata, key)
	return nil
}

func (s *MemoryStorage) GetReader(feedID string) (FeedReader, error) {
	f, ok := s.Feeds[feedID]
	if !ok {
		return nil, fmt.Errorf("Feed not found")
	}
	return f, nil
}

func (s *MemoryStorage) GetWriter(feed string) (FeedWriter, error) {
	f := &MemoryStorageFeed{
		metadata: &FeedMetadata{
			SHA256: feed,
		},
		calendar:        map[string]*Calendar{},
		calendarDate:    map[string][]*CalendarDate{},
		routes:          map[string]*Route{},
		agency:          map[string]*Agency{},
		stops:           map[string]*Stop{},
		stopsByParent:   map[string][]*Stop{},
		trips:           map[string]*Trip{},
		stopTimesByTrip: map[string][]*StopTime{},
		stopTimesByStop: map[string][]*StopTime{},
		minMaxStopSeq:   map[string][2]uint32{},
	}

	s.Feeds[feed] = f

	return f, nil
}

type MemoryStorageFeed struct {
	metadata         *FeedMetadata
	calendar         map[string]*Calendar
	calendarDate     map[string][]*CalendarDate
	routes           map[string]*Route
	agency           map[string]*Agency
	stops            map[string]*Stop
	stopsByParent    map[string][]*Stop
	trips            map[string]*Trip
	stopTimesByTrip  map[string][]*StopTime
	stopTimesByStop  map[string][]*StopTime
	minMaxStopSeq    map[string][2]uint32
	routeTypesByStop map[string][]RouteType
}

func (f *MemoryStorageFeed) WriteAgency(agency *Agency) error {
	f.agency[agency.ID] = agency
	return nil
}

func (f *MemoryStorageFeed) WriteStop(stop *Stop) error {
	f.stops[stop.ID] = stop
	if stop.ParentStation != "" {
		f.stopsByParent[stop.ParentStation] = append(f.stopsByParent[stop.ParentStation], stop)
	}
	return nil
}

func (f *MemoryStorageFeed) WriteRoute(route *Route) error {
	f.routes[route.ID] = route
	return nil
}

func (f *MemoryStorageFeed) BeginTrips() error {
	return nil
}

func (f *MemoryStorageFeed) WriteTrip(trip *Trip) error {
	f.trips[trip.ID] = trip
	return nil
}

func (f *MemoryStorageFeed) EndTrips() error {
	return nil
}

func (f *MemoryStorageFeed) BeginStopTimes() error {
	return nil
}

func (f *MemoryStorageFeed) WriteStopTime(stopTime *StopTime) error {
	sts, found := f.stopTimesByTrip[stopTime.TripID]
	if !found {
		sts = []*StopTime{}
		f.stopTimesByTrip[stopTime.TripID] = sts
	}

	f.stopTimesByTrip[stopTime.TripID] = append(sts, stopTime)

	sts, found = f.stopTimesByStop[stopTime.StopID]
	if !found {
		sts = []*StopTime{}
		f.stopTimesByStop[stopTime.StopID] = sts
	}

	f.stopTimesByStop[stopTime.StopID] = append(sts, stopTime)

	mms, found := f.minMaxStopSeq[stopTime.TripID]
	if !found {
		f.minMaxStopSeq[stopTime.TripID] = [2]uint32{stopTime.StopSequence, stopTime.StopSequence}
	} else {
		if stopTime.StopSequence < mms[0] {
			mms[0] = stopTime.StopSequence
		}
		if stopTime.StopSequence > mms[1] {
			mms[1] = stopTime.StopSequence
		}
		f.minMaxStopSeq[stopTime.TripID] = mms
	}

	return nil
}

func (f *MemoryStorageFeed) EndStopTimes() error {
	f.routeTypesByStop = map[string][]RouteType{}

	for _, stop := range f.stops {
		rts := map[RouteType]bool{}
		for _, st := range f.stopTimesByStop[stop.ID] {
			rts[f.routes[f.trips[st.TripID].RouteID].Type] = true
		}
		for rt := range rts {
			f.routeTypesByStop[stop.ID] = append(f.routeTypesByStop[stop.ID], rt)
		}
	}

	return nil
}

func (f *MemoryStorageFeed) WriteCalendar(row *Calendar) error {
	f.calendar[row.ServiceID] = row
	return nil
}

func (f *MemoryStorageFeed) WriteCalendarDate(row *CalendarDate) error {
	cds, found := f.calendarDate[row.ServiceID]
	if !found {
		cds = []*CalendarDate{}
		f.calendarDate[row.ServiceID] = cds
	}

	f.calendarDate[row.ServiceID] = append(cds, row)
	return nil
}

func (f *MemoryStorageFeed) Close() error {
	return nil
}

func (f *MemoryStorageFeed) Agencies() ([]*Agency, error) {
	agencies := []*Agency{}
	for _, v := range f.agency {
		agencies = append(agencies, v)
	}
	return agencies, nil
}

func (f *MemoryStorageFeed) Stops() ([]*Stop, error) {
	stops := []*Stop{}
	for _, v := range f.stops {
		stops = append(stops, v)
	}
	return stops, nil
}

func (f *MemoryStorageFeed) Routes() ([]*Route, error) {
	routes := []*Route{}
	for _, v := range f.routes {
		routes = append(routes, v)
	}
	return routes, nil
}

func (f *MemoryStorageFeed) Trips() ([]*Trip, error) {
	trips := []*Trip{}
	for _, v := range f.trips {
		trips = append(trips, v)
	}
	return trips, nil
}

func (f *MemoryStorageFeed) StopTimes() ([]*StopTime, error) {
	stoptimes := []*StopTime{}
	for _, v := range f.stopTimesByTrip {
		stoptimes = append(stoptimes, v...)
	}
	return stoptimes, nil
}

func (f *MemoryStorageFeed) Calendars() ([]*Calendar, error) {
	cals := []*Calendar{}
	for _, v := range f.calendar {
		cals = append(cals, v)
	}
	return cals, nil
}

func (f *MemoryStorageFeed) CalendarDates() ([]*CalendarDate, error) {
	cds := []*CalendarDate{}
	for _, v := range f.calendarDate {
		cds = append(cds, v...)
	}
	return cds, nil
}

func (f *MemoryStorageFeed) ActiveServices(date string) ([]string, error) {
	services := map[string]bool{}

	parsedDate, err := time.Parse("20060102", date)
	if err != nil {
		return nil, fmt.Errorf("invalid date: %s", date)
	}

	for _, calendar := range f.calendar {
		if calendar.Weekday&(1<<parsedDate.Weekday()) == 0 {
			continue
		}
		if calendar.StartDate > date {
			continue
		}
		if calendar.EndDate < date {
			continue
		}
		services[calendar.ServiceID] = true
	}

	for _, cds := range f.calendarDate {
		for _, cd := range cds {
			if cd.Date == date {
				if cd.ExceptionType == 1 {
					services[cd.ServiceID] = true
				} else if cd.ExceptionType == 2 {
					services[cd.ServiceID] = false
				}
			}
		}
	}

	activeServices := []string{}
	for serviceID, active := range services {
		if active {
			activeServices = append(activeServices, serviceID)
		}
	}

	return activeServices, nil
}

func (f *MemoryStorageFeed) MinMaxStopSeq() (map[string][2]uint32, error) {
	return f.minMaxStopSeq, nil
}

func (f *MemoryStorageFeed) StopTimeEvents(filter StopTimeEventFilter) ([]*StopTimeEvent, error) {
	var stopTimes []*StopTime

	if filter.StopID != "" {
		// The StopID filter must also apply to parent
		// stations, in case caller is referring to a Station
		// holding (potentially) multiple Stops
		stop, found := f.stops[filter.StopID]
		if !found {
			return []*StopTimeEvent{}, nil
		}

		if stop.LocationType == LocationTypeStation {
			for _, s := range f.stopsByParent[filter.StopID] {
				stopTimes = append(stopTimes, f.stopTimesByStop[s.ID]...)
			}
		} else {
			stopTimes = f.stopTimesByStop[filter.StopID]
		}
	} else {
		// Without StopID, we need to iterate over all
		// StopTimes.
		stopTimes = []*StopTime{}
		for _, v := range f.stopTimesByTrip {
			stopTimes = append(stopTimes, v...)
		}
	}

	routeTypes := map[RouteType]bool{}
	if len(filter.RouteTypes) > 0 {
		for _, rt := range filter.RouteTypes {
			routeTypes[rt] = true
		}
	}

	serviceIDs := map[string]bool{}
	if len(filter.ServiceIDs) > 0 {
		for _, sid := range filter.ServiceIDs {
			serviceIDs[sid] = true
		}
	}

	events := []*StopTimeEvent{}

	for _, st := range stopTimes {
		// Filters on StopTime
		if filter.ArrivalStart != "" && st.Arrival < filter.ArrivalStart {
			continue
		}
		if filter.ArrivalEnd != "" && st.Arrival > filter.ArrivalEnd {
			continue
		}
		if filter.DepartureStart != "" && st.Departure < filter.DepartureStart {
			continue
		}
		if filter.DepartureEnd != "" && st.Departure > filter.DepartureEnd {
			continue
		}

		// Filters on Trip
		trip := f.trips[st.TripID]
		if filter.RouteID != "" && trip.RouteID != filter.RouteID {
			continue
		}
		if filter.DirectionID != -1 && int(trip.DirectionID) != filter.DirectionID {
			continue
		}
		if len(serviceIDs) > 0 && !serviceIDs[trip.ServiceID] {
			continue
		}

		// Filters on Route
		route := f.routes[trip.RouteID]
		if len(routeTypes) > 0 && !routeTypes[route.Type] {
			continue
		}

		var parentStation *Stop
		stop := f.stops[st.StopID]
		if stop.ParentStation != "" {
			parentStation = f.stops[stop.ParentStation]
		}

		events = append(events, &StopTimeEvent{
			StopTime:      st,
			Trip:          trip,
			Route:         route,
			Stop:          stop,
			ParentStation: parentStation,
		})
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].StopTime.Arrival < events[j].StopTime.Arrival
	})

	return events, nil
}

func (f *MemoryStorageFeed) RouteDirections(stopID string) ([]*RouteDirection, error) {
	type routeDirectionKey struct {
		RouteID     string
		DirectionID int8
	}

	headsignSet := map[routeDirectionKey]map[string]bool{}
	for _, st := range f.stopTimesByStop[stopID] {
		if st.StopSequence == f.minMaxStopSeq[st.TripID][1] {
			// Ignore last stop in trip, its headsign is
			// not relevant to passengers departing from
			// this stop
			continue
		}

		trip := f.trips[st.TripID]
		headsign := trip.Headsign

		// If stop_time itself has a headsign, that overrides
		// the trip headsign
		if st.Headsign != "" {
			headsign = st.Headsign
		}

		k := routeDirectionKey{trip.RouteID, trip.DirectionID}
		if _, found := headsignSet[k]; !found {
			headsignSet[k] = map[string]bool{}
		}
		headsignSet[k][headsign] = true
	}

	rds := []*RouteDirection{}
	for rdKey, hs := range headsignSet {
		rs := RouteDirection{
			StopID:      stopID,
			RouteID:     rdKey.RouteID,
			DirectionID: rdKey.DirectionID,
			Headsigns:   []string{},
		}
		for h := range hs {
			rs.Headsigns = append(rs.Headsigns, h)
		}
		rds = append(rds, &rs)
	}

	return rds, nil
}

func (f *MemoryStorageFeed) NearbyStops(lat float64, lng float64, limit int, routeTypes []RouteType) ([]Stop, error) {
	stops := []*Stop{}

	if len(routeTypes) == 0 {
		for _, s := range f.stops {
			if !(s.LocationType == LocationTypeStation || s.LocationType == LocationTypeStop && s.ParentStation == "") {
				continue
			}
			stops = append(stops, s)
		}
	} else {
		typeSet := map[RouteType]bool{}
		for _, rt := range routeTypes {
			typeSet[rt] = true
		}
		for _, s := range f.stops {
			if !(s.LocationType == LocationTypeStation || s.LocationType == LocationTypeStop && s.ParentStation == "") {
				continue
			}
			for _, rt := range f.routeTypesByStop[s.ID] {
				if typeSet[rt] {
					stops = append(stops, s)
					break
				}
			}
		}
	}

	sort.Slice(stops, func(i, j int) bool {
		di := HaversineDistance(lat, lng, stops[i].Lat, stops[i].Lon)
		dj := HaversineDistance(lat, lng, stops[j].Lat, stops[j].Lon)
		return di < dj
	})

	if limit > 0 && len(stops) > limit {
		stops = stops[:limit]
	}

	res := []Stop{}
	for _, s := range stops {
		res = append(res, *s)
	}

	return res, nil
}
