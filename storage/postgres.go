package storage

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

const (
	PSQLTripBatchSize     = 10000
	PSQLStopTimeBatchSize = 5000
)

type PSQLStorage struct {
	db *sql.DB
}

type PSQLFeedWriter struct {
	id          string
	db          *sql.DB
	tripBuf     []*Trip
	stopTimeBuf []*StopTime
}

type PSQLFeedReader struct {
	id string
	db *sql.DB
}

// Creates a new Postgres Storage using the provided connection string.
//
// If clearDB is true, the database will be cleared on startup. You
// probably only want this for testing.
func NewPSQLStorage(connStr string, clearDB bool) (*PSQLStorage, error) {

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	if db.Ping() != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	if clearDB {
		_, err = db.Exec(`
DROP TABLE IF EXISTS feed;
DROP TABLE IF EXISTS agency;
DROP TABLE IF EXISTS calendar;
DROP TABLE IF EXISTS calendar_dates;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS stop_times;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS trips;
`)
		if err != nil {
			return nil, fmt.Errorf("clearing db: %w", err)
		}
	}

	// Create feed table if needed
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS feed (
    sha256 TEXT,
    url TEXT NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    calendar_start TEXT NOT NULL,
    calendar_end TEXT NOT NULL,
    feed_start TEXT NOT NULL,
    feed_end TEXT NOT NULL,
    timezone TEXT NOT NULL,
    max_arrival TEXT NOT NULL,
    max_departure TEXT NOT NULL,
    PRIMARY KEY (sha256, url)
);`)
	if err != nil {
		return nil, fmt.Errorf("creating feed table: %w", err)
	}

	return &PSQLStorage{
		db: db,
	}, nil
}

func (s *PSQLStorage) Close() error {
	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close db: %w", err)
	}
	return nil
}

func (s *PSQLStorage) ListFeeds(filter ListFeedsFilter) ([]*FeedMetadata, error) {
	query := `
SELECT
    sha256,
    url,
    retrieved_at,
    calendar_start,
    calendar_end,
    feed_start,
    feed_end,
    timezone,
    max_arrival,
    max_departure
FROM feed`

	conditions := []string{}
	params := []interface{}{}
	paramCount := 1

	if filter.URL != "" {
		conditions = append(conditions, fmt.Sprintf("url = $%d", paramCount))
		params = append(params, filter.URL)
		paramCount++
	}
	if filter.SHA256 != "" {
		conditions = append(conditions, fmt.Sprintf("sha256 = $%d", paramCount))
		params = append(params, filter.SHA256)
		paramCount++
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY retrieved_at DESC"

	rows, err := s.db.Query(query, params...)
	if err != nil {
		return nil, fmt.Errorf("listing feeds: %w", err)
	}
	defer rows.Close()

	var feeds []*FeedMetadata
	for rows.Next() {
		var feed FeedMetadata
		err := rows.Scan(
			&feed.SHA256,
			&feed.URL,
			&feed.RetrievedAt,
			&feed.CalendarStartDate,
			&feed.CalendarEndDate,
			&feed.FeedStartDate,
			&feed.FeedEndDate,
			&feed.Timezone,
			&feed.MaxArrival,
			&feed.MaxDeparture,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning feed: %w", err)
		}
		feeds = append(feeds, &feed)
	}

	return feeds, nil
}

func (s *PSQLStorage) ListFeedRequests(url string) ([]FeedRequest, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *PSQLStorage) WriteFeedMetadata(feed *FeedMetadata) error {
	_, err := s.db.Exec(`
INSERT INTO feed (
    sha256,
    url,
    retrieved_at,
    calendar_start,
    calendar_end,
    feed_start,
    feed_end,
    timezone,
    max_arrival,
    max_departure
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (sha256, url) DO UPDATE SET
    retrieved_at = excluded.retrieved_at,
    calendar_start = excluded.calendar_start,
    calendar_end = excluded.calendar_end,
    feed_start = excluded.feed_start,
    feed_end = excluded.feed_end,
    timezone = excluded.timezone,
    max_arrival = excluded.max_arrival,
    max_departure = excluded.max_departure
`,
		feed.SHA256,
		feed.URL,
		feed.RetrievedAt,
		feed.CalendarStartDate,
		feed.CalendarEndDate,
		feed.FeedStartDate,
		feed.FeedEndDate,
		feed.Timezone,
		feed.MaxArrival,
		feed.MaxDeparture,
	)
	if err != nil {
		return fmt.Errorf("writing feed metadata: %w", err)
	}
	return nil
}

func (s *PSQLStorage) WriteFeedRequest(req FeedRequest) error {
	return fmt.Errorf("not implemented")
}

func (s *PSQLStorage) DeleteFeedMetadata(url string, sha256 string) error {
	_, err := s.db.Exec(`
DELETE FROM feed
WHERE url = $1 AND sha256 = $2
`, url, sha256)
	return err
}

func (s *PSQLStorage) GetReader(feedID string) (FeedReader, error) {
	return &PSQLFeedReader{
		id: feedID,
		db: s.db,
	}, nil
}

func (s *PSQLStorage) GetWriter(feedID string) (FeedWriter, error) {
	tables := map[string]string{
		"agency": `
CREATE TABLE IF NOT EXISTS agency (
    feed TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    timezone TEXT NOT NULL,
    PRIMARY KEY(feed, id)
);`,
		"stops": `
CREATE TABLE IF NOT EXISTS stops (
    feed TEXT NOT NULL,
    id TEXT NOT NULL,
    code TEXT,
    name TEXT NOT NULL,
    description TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    url TEXT,
    location_type INTEGER NOT NULL,
    parent_station TEXT,
    platform_code TEXT,
    PRIMARY KEY(feed, id)
);
CREATE INDEX IF NOT EXISTS stops_parent_station ON stops (parent_station);
`,
		"routes": `
CREATE TABLE IF NOT EXISTS routes (
    feed TEXT NOT NULL,
    id TEXT NOT NULL,
    agency_id TEXT,
    short_name TEXT,
    long_name TEXT NOT NULL,
    description TEXT,
    type INTEGER NOT NULL,
    url TEXT,
    color TEXT,
    text_color TEXT,
    PRIMARY KEY(feed, id)
);`,
		"trips": `
CREATE TABLE IF NOT EXISTS trips (
    feed TEXT NOT NULL,
    id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    headsign TEXT,
    short_name TEXT,
    direction_id INTEGER,
    PRIMARY KEY(feed, id)
);
CREATE INDEX IF NOT EXISTS trips_route_id ON trips (route_id);
CREATE INDEX IF NOT EXISTS trips_service_id ON trips (service_id);
`,
		"stop_times": `
CREATE TABLE IF NOT EXISTS stop_times (
    feed TEXT NOT NULL,
    trip_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    arrival_time TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    headsign TEXT,
    PRIMARY KEY(feed, trip_id, stop_id, stop_sequence)
);
CREATE INDEX IF NOT EXISTS stop_times_trip_id ON stop_times (trip_id);
CREATE INDEX IF NOT EXISTS stop_times_stop_id ON stop_times (stop_id);
CREATE INDEX IF NOT EXISTS stop_times_arrival_time ON stop_times (arrival_time);
CREATE INDEX IF NOT EXISTS stop_times_departure_time ON stop_times (departure_time);
`,
		"calendar": `
CREATE TABLE IF NOT EXISTS calendar (
    feed TEXT NOT NULL,
    service_id TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    sunday INTEGER NOT NULL,
    PRIMARY KEY(feed, service_id)
);`,
		"calendar_dates": `
CREATE TABLE IF NOT EXISTS calendar_dates (
    feed TEXT NOT NULL,
    service_id TEXT NOT NULL,
    date TEXT NOT NULL,
    exception_type INTEGER NOT NULL,
    PRIMARY KEY(feed, service_id, date)
);`,
	}

	// Create tables if they don't exist
	for name, query := range tables {
		_, err := s.db.Exec(query)
		if err != nil {
			s.db.Close()
			return nil, fmt.Errorf("creating %s table: %s", name, err)
		}
	}

	// In case feed already exists, delete all records
	for name := range tables {
		_, err := s.db.Exec(`DELETE FROM `+name+` WHERE feed = $1`, feedID)
		if err != nil {
			s.db.Close()
			return nil, fmt.Errorf("deleting %s records: %s", name, err)
		}
	}

	return &PSQLFeedWriter{
		id: feedID,
		db: s.db,
	}, nil
}

func (w *PSQLFeedWriter) WriteAgency(a *Agency) error {
	_, err := w.db.Exec(`
INSERT INTO agency (feed, id, name, url, timezone)
VALUES ($1, $2, $3, $4, $5)`,
		w.id,
		a.ID,
		a.Name,
		a.URL,
		a.Timezone,
	)
	if err != nil {
		return fmt.Errorf("inserting agency: %w", err)
	}
	return nil
}

func (w *PSQLFeedWriter) WriteStop(stop *Stop) error {
	var parentStation sql.NullString
	if stop.ParentStation != "" {
		parentStation = sql.NullString{
			String: stop.ParentStation,
			Valid:  true,
		}
	}
	_, err := w.db.Exec(`
INSERT INTO stops (feed, id, code, name, description, lat, lon, url, location_type, parent_station, platform_code)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		w.id,
		stop.ID,
		stop.Code,
		stop.Name,
		stop.Desc,
		stop.Lat,
		stop.Lon,
		stop.URL,
		stop.LocationType,
		parentStation,
		stop.PlatformCode,
	)
	if err != nil {
		return fmt.Errorf("inserting stop: %w", err)
	}
	return nil
}

func (w *PSQLFeedWriter) WriteRoute(route *Route) error {
	_, err := w.db.Exec(`
INSERT INTO routes (feed, id, agency_id, short_name, long_name, description, type, url, color, text_color)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		w.id,
		route.ID,
		route.AgencyID,
		route.ShortName,
		route.LongName,
		route.Desc,
		route.Type,
		route.URL,
		route.Color,
		route.TextColor,
	)
	if err != nil {
		return fmt.Errorf("inserting route: %w", err)
	}
	return nil
}

func (w *PSQLFeedWriter) BeginTrips() error {
	return nil
}

func (w *PSQLFeedWriter) WriteTrip(trip *Trip) error {
	w.tripBuf = append(w.tripBuf, trip)

	if len(w.tripBuf) >= PSQLTripBatchSize {
		err := w.flushTrips()
		if err != nil {
			return fmt.Errorf("flushing trips: %w", err)
		}
	}

	return nil
}

func (w *PSQLFeedWriter) EndTrips() error {
	if len(w.tripBuf) > 0 {
		err := w.flushTrips()
		if err != nil {
			return fmt.Errorf("flushing trips: %w", err)
		}
	}
	return nil
}

func (w *PSQLFeedWriter) flushTrips() error {
	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(pq.CopyIn(
		"trips", "feed", "id", "route_id", "service_id", "headsign", "short_name", "direction_id",
	))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, trip := range w.tripBuf {
		_, err = stmt.Exec(
			w.id, trip.ID, trip.RouteID, trip.ServiceID, trip.Headsign, trip.ShortName, trip.DirectionID,
		)
		if err != nil {
			return fmt.Errorf("COPY trip: %w", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("executing statement: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("committing: %w", err)
	}

	w.tripBuf = nil

	return nil
}

func (w *PSQLFeedWriter) WriteCalendar(cal *Calendar) error {
	mon, tue, wed, thu, fri, sat, sun := 0, 0, 0, 0, 0, 0, 0
	if cal.Weekday&(1<<time.Monday) != 0 {
		mon = 1
	}
	if cal.Weekday&(1<<time.Tuesday) != 0 {
		tue = 1
	}
	if cal.Weekday&(1<<time.Wednesday) != 0 {
		wed = 1
	}
	if cal.Weekday&(1<<time.Thursday) != 0 {
		thu = 1
	}
	if cal.Weekday&(1<<time.Friday) != 0 {
		fri = 1
	}
	if cal.Weekday&(1<<time.Saturday) != 0 {
		sat = 1
	}
	if cal.Weekday&(1<<time.Sunday) != 0 {
		sun = 1
	}

	_, err := w.db.Exec(`
INSERT INTO calendar (feed, service_id, start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		w.id,
		cal.ServiceID,
		cal.StartDate,
		cal.EndDate,
		mon, tue, wed, thu, fri, sat, sun,
	)
	if err != nil {
		return fmt.Errorf("inserting calendar: %w", err)
	}

	return nil
}

func (w *PSQLFeedWriter) WriteCalendarDate(cd *CalendarDate) error {
	_, err := w.db.Exec(`
INSERT INTO calendar_dates (feed, service_id, date, exception_type)
VALUES ($1, $2, $3, $4)`,
		w.id,
		cd.ServiceID,
		cd.Date,
		cd.ExceptionType,
	)
	if err != nil {
		return fmt.Errorf("inserting calendar date: %w", err)
	}

	return nil

}

func (w *PSQLFeedWriter) BeginStopTimes() error {
	return nil
}

func (w *PSQLFeedWriter) WriteStopTime(stopTime *StopTime) error {
	w.stopTimeBuf = append(w.stopTimeBuf, stopTime)

	if len(w.stopTimeBuf) >= PSQLStopTimeBatchSize {
		err := w.flushStopTimes()
		if err != nil {
			return fmt.Errorf("flushing stop_times: %w", err)
		}
	}

	return nil
}

func (w *PSQLFeedWriter) EndStopTimes() error {
	if len(w.stopTimeBuf) > 0 {
		err := w.flushStopTimes()
		if err != nil {
			return fmt.Errorf("flushing stop_times: %w", err)
		}
	}
	return nil
}

func (w *PSQLFeedWriter) flushStopTimes() error {
	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(pq.CopyIn(
		"stop_times", "feed", "trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "headsign",
	))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, stopTime := range w.stopTimeBuf {
		_, err = stmt.Exec(
			w.id,
			stopTime.TripID,
			stopTime.StopID,
			stopTime.StopSequence,
			stopTime.Arrival,
			stopTime.Departure,
			stopTime.Headsign,
		)
		if err != nil {
			return fmt.Errorf("COPY stop_time: %w", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("executing statement: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("committing: %w", err)
	}

	w.stopTimeBuf = nil

	return nil
}

func (s *PSQLFeedWriter) Close() error {
	return nil
}

func (r *PSQLFeedReader) Agencies() ([]*Agency, error) {
	rows, err := r.db.Query(`
SELECT id, name, url, timezone
FROM agency
WHERE feed = $1`, r.id)
	if err != nil {
		return nil, fmt.Errorf("querying agencies: %w", err)
	}
	defer rows.Close()

	agencies := []*Agency{}
	for rows.Next() {
		a := &Agency{}
		err := rows.Scan(&a.ID, &a.Name, &a.URL, &a.Timezone)
		if err != nil {
			return nil, fmt.Errorf("scanning agency: %w", err)
		}
		agencies = append(agencies, a)
	}

	return agencies, nil
}

func (r *PSQLFeedReader) Stops() ([]*Stop, error) {
	rows, err := r.db.Query(`
SELECT id, code, name, description, lat, lon, url, location_type, parent_station, platform_code
FROM stops
WHERE feed = $1`, r.id)
	if err != nil {
		return nil, fmt.Errorf("querying stops: %w", err)
	}
	defer rows.Close()

	stops := []*Stop{}
	for rows.Next() {
		s := &Stop{}
		parentStation := sql.NullString{}
		err := rows.Scan(
			&s.ID,
			&s.Code,
			&s.Name,
			&s.Desc,
			&s.Lat,
			&s.Lon,
			&s.URL,
			&s.LocationType,
			&parentStation,
			&s.PlatformCode,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning stop: %w", err)
		}

		if parentStation.Valid {
			s.ParentStation = parentStation.String
		}

		stops = append(stops, s)
	}

	return stops, nil
}

func (r *PSQLFeedReader) Routes() ([]*Route, error) {
	rows, err := r.db.Query(`
SELECT id, agency_id, short_name, long_name, description, type, url, color, text_color
FROM routes
WHERE feed = $1`, r.id)
	if err != nil {
		return nil, fmt.Errorf("querying routes: %w", err)
	}
	defer rows.Close()

	routes := []*Route{}
	for rows.Next() {
		route := &Route{}
		err := rows.Scan(
			&route.ID,
			&route.AgencyID,
			&route.ShortName,
			&route.LongName,
			&route.Desc,
			&route.Type,
			&route.URL,
			&route.Color,
			&route.TextColor,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning route: %w", err)
		}
		routes = append(routes, route)
	}

	return routes, nil
}

func (r *PSQLFeedReader) Trips() ([]*Trip, error) {
	rows, err := r.db.Query(`
SELECT id, route_id, service_id, headsign, short_name, direction_id
FROM trips
WHERE feed = $1`, r.id)
	if err != nil {
		return nil, fmt.Errorf("querying trips: %w", err)
	}
	defer rows.Close()

	trips := []*Trip{}
	for rows.Next() {
		t := &Trip{}
		err := rows.Scan(
			&t.ID,
			&t.RouteID,
			&t.ServiceID,
			&t.Headsign,
			&t.ShortName,
			&t.DirectionID,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning trip: %w", err)
		}
		trips = append(trips, t)
	}

	return trips, nil
}

func (r *PSQLFeedReader) StopTimes() ([]*StopTime, error) {
	rows, err := r.db.Query(`
SELECT trip_id, stop_id, headsign, stop_sequence, arrival_time, departure_time
FROM stop_times
WHERE feed = $1`, r.id)
	if err != nil {
		return nil, fmt.Errorf("querying stop times: %w", err)
	}
	defer rows.Close()

	stopTimes := []*StopTime{}
	for rows.Next() {
		st := &StopTime{}
		err := rows.Scan(
			&st.TripID,
			&st.StopID,
			&st.Headsign,
			&st.StopSequence,
			&st.Arrival,
			&st.Departure,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning stop time: %w", err)
		}
		stopTimes = append(stopTimes, st)
	}

	return stopTimes, nil
}

func (r *PSQLFeedReader) Calendars() ([]*Calendar, error) {
	rows, err := r.db.Query(`
SELECT service_id, start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday
FROM calendar
WHERE feed = $1`, r.id)
	if err != nil {
		return nil, fmt.Errorf("querying calendar: %w", err)
	}
	defer rows.Close()

	calendars := []*Calendar{}
	for rows.Next() {
		var serviceID, startDate, endDate string
		var monday, tuesday, wednesday, thursday, friday, saturday, sunday bool
		err := rows.Scan(
			&serviceID,
			&startDate,
			&endDate,
			&monday,
			&tuesday,
			&wednesday,
			&thursday,
			&friday,
			&saturday,
			&sunday,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning calendar: %w", err)
		}
		weekday := int8(0)
		if monday {
			weekday |= 1 << time.Monday
		}
		if tuesday {
			weekday |= 1 << time.Tuesday
		}
		if wednesday {
			weekday |= 1 << time.Wednesday
		}
		if thursday {
			weekday |= 1 << time.Thursday
		}
		if friday {
			weekday |= 1 << time.Friday
		}
		if saturday {
			weekday |= 1 << time.Saturday
		}
		if sunday {
			weekday |= 1 << time.Sunday
		}
		calendars = append(calendars, &Calendar{
			ServiceID: serviceID,
			StartDate: startDate,
			EndDate:   endDate,
			Weekday:   weekday,
		})
	}

	return calendars, nil
}

func (r *PSQLFeedReader) CalendarDates() ([]*CalendarDate, error) {
	rows, err := r.db.Query(`
SELECT service_id, date, exception_type
FROM calendar_dates
WHERE feed = $1`, r.id)
	if err != nil {
		return nil, fmt.Errorf("querying calendar dates: %w", err)
	}
	defer rows.Close()

	calendarDates := []*CalendarDate{}
	for rows.Next() {
		cd := &CalendarDate{}
		err := rows.Scan(
			&cd.ServiceID,
			&cd.Date,
			&cd.ExceptionType,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning calendar date: %w", err)
		}
		calendarDates = append(calendarDates, cd)
	}

	return calendarDates, nil
}

func (r *PSQLFeedReader) ActiveServices(date string) ([]string, error) {
	parsedDate, err := time.Parse("20060102", date)
	if err != nil {
		return nil, fmt.Errorf("invalid date: %s", date)
	}

	var weekday string
	switch parsedDate.Weekday() {
	case time.Monday:
		weekday = "monday"
	case time.Tuesday:
		weekday = "tuesday"
	case time.Wednesday:
		weekday = "wednesday"
	case time.Thursday:
		weekday = "thursday"
	case time.Friday:
		weekday = "friday"
	case time.Saturday:
		weekday = "saturday"
	case time.Sunday:
		weekday = "sunday"
	}

	rows, err := r.db.Query(`
WITH
Exceptions AS (
        SELECT service_id, exception_type
        FROM calendar_dates
        WHERE feed = $1 AND
              date = $2
),
Regular AS (
        SELECT service_id
        FROM calendar
        WHERE feed = $3 AND
              `+weekday+` = 1 AND
              start_date <= $4 AND
              end_date >= $5
)
SELECT service_id FROM Regular
WHERE service_id NOT IN (
	SELECT service_id FROM Exceptions WHERE exception_type = 2
)
UNION
SELECT service_id FROM Exceptions
WHERE exception_type = 1
`, r.id, date, r.id, date, date)
	if err != nil {
		return nil, fmt.Errorf("querying for active services: %w", err)
	}
	defer rows.Close()

	activeServices := []string{}
	for rows.Next() {
		var serviceID string
		err = rows.Scan(&serviceID)
		if err != nil {
			return nil, fmt.Errorf("scanning active services: %w", err)
		}
		activeServices = append(activeServices, serviceID)
	}

	return activeServices, nil
}

func (r *PSQLFeedReader) MinMaxStopSeq() (map[string][2]uint32, error) {
	rows, err := r.db.Query(`
SELECT
    trip_id,
    MIN(stop_sequence),
    MAX(stop_sequence)
FROM stop_times
GROUP BY trip_id`)
	if err != nil {
		return nil, fmt.Errorf("querying min/max stop sequence: %w", err)
	}
	defer rows.Close()

	res := map[string][2]uint32{}
	for rows.Next() {
		var tripID string
		var min, max uint32
		err := rows.Scan(&tripID, &min, &max)
		if err != nil {
			return nil, fmt.Errorf("scanning min/max stop sequence: %w", err)
		}
		res[tripID] = [2]uint32{min, max}
	}

	return res, nil
}

func (r *PSQLFeedReader) StopTimeEvents(filter StopTimeEventFilter) ([]*StopTimeEvent, error) {
	baseQuery := `
SELECT
    stops.id,
    stops.code,
    stops.name,
    stops.description,
    stops.lat,
    stops.lon,
    stops.url,
    stops.location_type,
    stops.parent_station,
    stops.platform_code,
    stop_times.trip_id,
    stop_times.stop_id,
    stop_times.stop_sequence,
    stop_times.arrival_time,
    stop_times.departure_time,
    stop_times.headsign,
    trips.id,
    trips.route_id,
    trips.service_id,
    trips.headsign,
    trips.short_name,
    trips.direction_id,
    routes.id,
    routes.agency_id,
    routes.short_name,
    routes.long_name,
    routes.description,
    routes.type,
    routes.url,
    routes.color,
    routes.text_color
FROM stop_times
INNER JOIN stops ON stop_times.stop_id = stops.id
INNER JOIN trips ON stop_times.trip_id = trips.id
INNER JOIN routes ON trips.route_id = routes.id
WHERE stops.feed = $1 AND
      stop_times.feed = $1 AND
      trips.feed = $1 AND
      routes.feed = $1
`

	// Apply filters to query
	fParams, fVals := []string{}, []string{r.id}
	pIdx := 2

	if filter.StopID != "" {
		fParams = append(fParams, fmt.Sprintf(
			"(stops.id = $%d OR stops.parent_station = $%d)",
			pIdx, pIdx+1,
		))
		fVals = append(fVals, filter.StopID, filter.StopID)
		pIdx += 2
	}

	if filter.RouteID != "" {
		fParams = append(fParams, fmt.Sprintf("routes.id = $%d", pIdx))
		fVals = append(fVals, filter.RouteID)
		pIdx++
	}

	if len(filter.TripIDs) > 0 {
		tripIDPlaceholders := []string{}
		for i := range filter.TripIDs {
			tripIDPlaceholders = append(tripIDPlaceholders, fmt.Sprintf("$%d", pIdx+i))
		}
		fParams = append(fParams, "trips.id IN ("+strings.Join(tripIDPlaceholders, ", ")+")")
		fVals = append(fVals, filter.TripIDs...)
		pIdx += len(filter.TripIDs)
	}

	if len(filter.ServiceIDs) > 0 {
		serviceIDPlaceholders := []string{}
		for i := range filter.ServiceIDs {
			serviceIDPlaceholders = append(serviceIDPlaceholders, fmt.Sprintf("$%d", pIdx+i))
		}
		fParams = append(fParams, "trips.service_id IN ("+strings.Join(serviceIDPlaceholders, ", ")+")")
		fVals = append(fVals, filter.ServiceIDs...)
		pIdx += len(filter.ServiceIDs)
	}

	if filter.DirectionID > -1 {
		fParams = append(fParams, fmt.Sprintf("trips.direction_id = $%d", pIdx))
		fVals = append(fVals, fmt.Sprintf("%d", filter.DirectionID))
		pIdx++
	}

	if filter.ArrivalStart != "" {
		fParams = append(fParams, fmt.Sprintf("stop_times.arrival_time >= $%d", pIdx))
		fVals = append(fVals, filter.ArrivalStart)
		pIdx++
	}

	if filter.ArrivalEnd != "" {
		fParams = append(fParams, fmt.Sprintf("stop_times.arrival_time <= $%d", pIdx))
		fVals = append(fVals, filter.ArrivalEnd)
		pIdx++
	}

	if filter.DepartureStart != "" {
		fParams = append(fParams, fmt.Sprintf("stop_times.departure_time >= $%d", pIdx))
		fVals = append(fVals, filter.DepartureStart)
		pIdx++
	}

	if filter.DepartureEnd != "" {
		fParams = append(fParams, fmt.Sprintf("stop_times.departure_time <= $%d", pIdx))
		fVals = append(fVals, filter.DepartureEnd)
		pIdx++
	}

	if len(filter.RouteTypes) > 0 {
		placeholders := []string{}
		for i, rt := range filter.RouteTypes {
			placeholders = append(placeholders, fmt.Sprintf("$%d", pIdx+i))
			fVals = append(fVals, fmt.Sprintf("%d", rt))
		}
		fParams = append(fParams, "routes.type IN ("+strings.Join(placeholders, ", ")+")")
		pIdx += len(filter.RouteTypes)
	}

	// Run query
	var query string
	if len(fParams) > 0 {
		query = baseQuery + " AND " + strings.Join(fParams, " AND ")
	} else {
		query = baseQuery
	}
	query += " ORDER BY stop_times.arrival_time ASC"

	queryValues := []interface{}{}
	for _, v := range fVals {
		queryValues = append(queryValues, v)
	}

	rows, err := r.db.Query(query, queryValues...)
	if err != nil {
		return nil, fmt.Errorf("querying for stop time events: %w", err)
	}
	defer rows.Close()

	events := []*StopTimeEvent{}
	for rows.Next() {
		stop := &Stop{}
		stopTime := &StopTime{}
		trip := &Trip{}
		route := &Route{}
		parentStation := sql.NullString{}

		err = rows.Scan(
			&stop.ID,
			&stop.Code,
			&stop.Name,
			&stop.Desc,
			&stop.Lat,
			&stop.Lon,
			&stop.URL,
			&stop.LocationType,
			&parentStation,
			&stop.PlatformCode,
			&stopTime.TripID,
			&stopTime.StopID,
			&stopTime.StopSequence,
			&stopTime.Arrival,
			&stopTime.Departure,
			&stopTime.Headsign,
			&trip.ID,
			&trip.RouteID,
			&trip.ServiceID,
			&trip.Headsign,
			&trip.ShortName,
			&trip.DirectionID,
			&route.ID,
			&route.AgencyID,
			&route.ShortName,
			&route.LongName,
			&route.Desc,
			&route.Type,
			&route.URL,
			&route.Color,
			&route.TextColor,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning stop time event: %w", err)
		}

		if parentStation.Valid {
			stop.ParentStation = parentStation.String
		}

		events = append(events, &StopTimeEvent{
			Stop:     stop,
			StopTime: stopTime,
			Trip:     trip,
			Route:    route,
		})
	}

	// Retrieve parent stations where applicable
	//
	// NOTE: this used to be done in the main query as a left
	// outer join, but performance was awful. May not even need to
	// retrieve this data at all.
	parents := map[string]*Stop{}
	for _, event := range events {
		if event.Stop.ParentStation == "" {
			continue
		}

		if _, ok := parents[event.Stop.ParentStation]; !ok {
			parents[event.Stop.ParentStation] = &Stop{}
		}
	}

	if len(parents) == 0 {
		return events, nil
	}

	placeholders := []string{}
	parentIDs := []interface{}{}
	parentIDs = append(parentIDs, r.id)
	i := 2
	for id := range parents {
		parentIDs = append(parentIDs, id)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		i += 1
	}

	rows, err = r.db.Query(`
SELECT id, code, name, description, lat, lon, url, location_type, platform_code
FROM stops
WHERE feed = $1 AND
      id IN (`+strings.Join(placeholders, ", ")+`)
`, parentIDs...)
	if err != nil {
		return nil, fmt.Errorf("querying for parent stations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		stop := &Stop{}
		err = rows.Scan(
			&stop.ID,
			&stop.Code,
			&stop.Name,
			&stop.Desc,
			&stop.Lat,
			&stop.Lon,
			&stop.URL,
			&stop.LocationType,
			&stop.PlatformCode,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning parent station: %w", err)
		}

		parents[stop.ID] = stop
	}

	for _, event := range events {
		event.ParentStation = parents[event.Stop.ParentStation]
	}

	return events, nil
}

func (r *PSQLFeedReader) RouteDirections(stopID string) ([]*RouteDirection, error) {
	rows, err := r.db.Query(`
SELECT trips.route_id, trips.direction_id, trips.headsign, stop_times.headsign
FROM stop_times
INNER JOIN trips ON trips.id = stop_times.trip_id
WHERE stop_times.feed = $1 AND
      trips.feed = $1 AND
      stop_times.stop_id = $2 AND
      stop_times.stop_sequence != (SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = trips.id)
`, r.id, stopID)
	if err != nil {
		return nil, fmt.Errorf("querying for route directions: %w", err)
	}
	defer rows.Close()

	type key struct {
		RouteID     string
		DirectionID int8
	}

	deduped := map[key]map[string]bool{}
	for rows.Next() {
		var routeID string
		var directionID int8
		var tripHeadsign string
		var stopHeadsign string
		err = rows.Scan(&routeID, &directionID, &tripHeadsign, &stopHeadsign)
		if err != nil {
			return nil, fmt.Errorf("scanning route directions: %w", err)
		}

		key := key{
			RouteID:     routeID,
			DirectionID: directionID,
		}
		if _, ok := deduped[key]; !ok {
			deduped[key] = map[string]bool{}
		}
		headsign := stopHeadsign
		if headsign == "" {
			headsign = tripHeadsign
		}
		deduped[key][headsign] = true
	}

	routeDirections := []*RouteDirection{}
	for key, headsignSet := range deduped {
		headsigns := []string{}
		for headsign := range headsignSet {
			headsigns = append(headsigns, headsign)
		}
		routeDirections = append(routeDirections, &RouteDirection{
			StopID:      stopID,
			RouteID:     key.RouteID,
			DirectionID: key.DirectionID,
			Headsigns:   headsigns,
		})
	}

	return routeDirections, nil
}

func (r *PSQLFeedReader) getStops() ([]*Stop, error) {
	row, err := r.db.Query(`
SELECT
    stops.id,
    stops.code,
    stops.name,
    stops.description,
    stops.lat,
    stops.lon,
    stops.url,
    stops.location_type,
    stops.parent_station,
    stops.platform_code
FROM
    stops
WHERE
    stops.location_type = 0 AND parent_station IS NULL OR stops.location_type = 1`)
	if err != nil {
		return nil, fmt.Errorf("querying for nearby stops: %w", err)
	}

	stops := []*Stop{}
	for row.Next() {
		stop := &Stop{}
		parentStation := sql.NullString{}
		err = row.Scan(
			&stop.ID,
			&stop.Code,
			&stop.Name,
			&stop.Desc,
			&stop.Lat,
			&stop.Lon,
			&stop.URL,
			&stop.LocationType,
			&parentStation,
			&stop.PlatformCode,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning stop: %w", err)
		}

		if parentStation.Valid {
			stop.ParentStation = parentStation.String
		}

		stops = append(stops, stop)
	}

	return stops, nil
}

func (r *PSQLFeedReader) getStopsByRouteType(routeTypes []RouteType) ([]*Stop, error) {
	queryValues := []interface{}{}
	for _, rt := range routeTypes {
		queryValues = append(queryValues, rt)
	}
	routeTypePlaceholders := []string{}
	for i := range routeTypes {
		routeTypePlaceholders = append(routeTypePlaceholders, fmt.Sprintf("$%d", i+1))
	}

	rows, err := r.db.Query(`
SELECT
    stops.id,
    stops.code,
    stops.name,
    stops.description,
    stops.lat,
    stops.lon,
    stops.url,
    stops.location_type,
    stops.parent_station,
    stops.platform_code,
    parent.id,
    parent.code,
    parent.name,
    parent.description,
    parent.lat,
    parent.lon,
    parent.url,
    parent.location_type,
    parent.platform_code
FROM stop_times
INNER JOIN trips ON stop_times.trip_id = trips.id
INNER JOIN routes ON trips.route_id = routes.id
INNER JOIN stops ON stop_times.stop_id = stops.id
LEFT OUTER JOIN stops AS parent ON stops.parent_station = parent.id
WHERE
    stops.location_type = 0 AND
    routes.type IN (`+strings.Join(routeTypePlaceholders, ", ")+`)
`, queryValues...)
	if err != nil {
		return nil, fmt.Errorf("querying for stops by route type: %w", err)
	}
	defer rows.Close()

	allStops := map[string]*Stop{}
	for rows.Next() {
		s := &Stop{}
		//stopParentStation := sql.NullString{}
		parentID := sql.NullString{}
		parentCode := sql.NullString{}
		parentName := sql.NullString{}
		parentDesc := sql.NullString{}
		parentLat := sql.NullFloat64{}
		parentLon := sql.NullFloat64{}
		parentURL := sql.NullString{}
		parentLocationType := sql.NullInt64{}
		parentPlatformCode := sql.NullString{}
		err := rows.Scan(
			&s.ID,
			&s.Code,
			&s.Name,
			&s.Desc,
			&s.Lat,
			&s.Lon,
			&s.URL,
			&s.LocationType,
			&s.ParentStation,
			&s.PlatformCode,
			&parentID,
			&parentCode,
			&parentName,
			&parentDesc,
			&parentLat,
			&parentLon,
			&parentURL,
			&parentLocationType,
			&parentPlatformCode,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning stop: %w", err)
		}

		if parentID.Valid {
			allStops[parentID.String] = &Stop{
				ID:           parentID.String,
				Code:         parentCode.String,
				Name:         parentName.String,
				Desc:         parentDesc.String,
				Lat:          parentLat.Float64,
				Lon:          parentLon.Float64,
				URL:          parentURL.String,
				LocationType: LocationType(parentLocationType.Int64),
				PlatformCode: parentPlatformCode.String,
			}
		} else {
			allStops[s.ID] = s
		}
	}

	stops := []*Stop{}
	for _, s := range allStops {
		stops = append(stops, s)
	}

	return stops, nil
}

func (r *PSQLFeedReader) NearbyStops(lat float64, lng float64, limit int, routeTypes []RouteType) ([]Stop, error) {
	var stops []*Stop
	var err error

	// TODO: Look into using postgis for this.
	//
	// Consider doing a single query w joins regardless of routeTypes.

	if len(routeTypes) == 0 {
		stops, err = r.getStops()
		if err != nil {
			return nil, fmt.Errorf("getting all stops: %w", err)
		}
	} else {
		// NOTE: With this query, only stops that have an
		// actual trip of the correct route type passing
		// through will be included in the result.
		stops, err = r.getStopsByRouteType(routeTypes)
		if err != nil {
			return nil, fmt.Errorf("getting stops by route type: %w", err)
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
