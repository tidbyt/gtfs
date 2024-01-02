package storage

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteConfig struct {
	OnDisk    bool
	Directory string
}

type SQLiteStorage struct {
	SQLiteConfig

	feedDB *sql.DB
	feeds  map[string]*sql.DB
}

type SQLiteFeedWriter struct {
	db                  *sql.DB
	stopTimeInsertQuery *sql.Stmt
	stopTimeInsertTx    *sql.Tx
}

type SQLiteFeedReader struct {
	db *sql.DB
}

func NewSQLiteStorage(cfg ...SQLiteConfig) (*SQLiteStorage, error) {
	onDisk := false
	directory := ""
	if len(cfg) > 0 {
		onDisk = cfg[0].OnDisk
		directory = cfg[0].Directory
	}

	sourceName := ":memory:"
	if onDisk {
		sourceName = directory + "/gtfs.db"
	}

	db, err := sql.Open("sqlite3", sourceName)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS feed (
    sha256 TEXT,
    url TEXT NOT NULL,
    retrieved_at TIMESTAMP NOT NULL,
    calendar_start TEXT NOT NULL,
    calendar_end TEXT NOT NULL,
    feed_start TEXT NOT NULL,
    feed_end TEXT NOT NULL,
    timezone TEXT NOT NULL,
    max_arrival TEXT NOT NULL,
    max_departure TEXT NOT NULL,
PRIMARY KEY (sha256, url)
);

CREATE TABLE IF NOT EXISTS feed_request (
    url TEXT NOT NULL,
    refreshed_at TIMESTAMP NOT NULL,
PRIMARY KEY (url)
);

CREATE TABLE IF NOT EXISTS feed_consumer (
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    headers TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
PRIMARY KEY (name, url)
);`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("creating feed table: %w", err)
	}

	return &SQLiteStorage{
		SQLiteConfig: SQLiteConfig{
			OnDisk:    onDisk,
			Directory: directory,
		},
		feedDB: db,
		feeds:  map[string]*sql.DB{},
	}, nil
}

func (s *SQLiteStorage) ListFeeds(filter ListFeedsFilter) ([]*FeedMetadata, error) {
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
	if filter.URL != "" {
		conditions = append(conditions, "url = ?")
		params = append(params, filter.URL)
	}
	if filter.SHA256 != "" {
		conditions = append(conditions, "sha256 = ?")
		params = append(params, filter.SHA256)
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY retrieved_at DESC"

	rows, err := s.feedDB.Query(query, params...)
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

func (s *SQLiteStorage) ListFeedRequests(url string) ([]FeedRequest, error) {
	query := `
SELECT
    req.url,
    req.refreshed_at,
    con.name,
    con.headers,
    con.created_at,
    con.updated_at
FROM feed_request req
LEFT JOIN feed_consumer con ON req.url = con.url`

	var rows *sql.Rows
	var err error
	if url != "" {
		query += " WHERE req.url = ?"
		rows, err = s.feedDB.Query(query, url)
	} else {
		rows, err = s.feedDB.Query(query)
	}
	if err != nil {
		return nil, fmt.Errorf("listing feed requests: %w", err)
	}

	requests := map[string]*FeedRequest{}
	for rows.Next() {
		var req FeedRequest
		var con FeedConsumer
		var name sql.NullString
		var headers sql.NullString
		var createdAt sql.NullTime
		var updatedAt sql.NullTime
		err := rows.Scan(
			&req.URL,
			&req.RefreshedAt,
			&name,
			&headers,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning feed request: %w", err)
		}

		if _, ok := requests[req.URL]; !ok {
			requests[req.URL] = &req
		}
		if name.Valid {
			con.Name = name.String
			con.Headers = headers.String
			con.CreatedAt = createdAt.Time
			con.UpdatedAt = updatedAt.Time
			requests[req.URL].Consumers = append(requests[req.URL].Consumers, con)
		}
	}

	reqs := []FeedRequest{}
	for _, req := range requests {
		reqs = append(reqs, *req)
	}

	return reqs, nil
}

func (s *SQLiteStorage) WriteFeedMetadata(feed *FeedMetadata) error {
	_, err := s.feedDB.Exec(`
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
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

func (s *SQLiteStorage) WriteFeedRequest(req FeedRequest) error {
	tx, err := s.feedDB.Begin()
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}

	query := `
INSERT INTO feed_request (url, refreshed_at)
VALUES (?, ?)
ON CONFLICT (url)`

	if req.RefreshedAt.IsZero() {
		query += " DO NOTHING"
	} else {
		query += "DO UPDATE SET refreshed_at = excluded.refreshed_at"
	}

	_, err = tx.Exec(query, req.URL, req.RefreshedAt)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("inserting feed request: %w", err)
	}

	for _, con := range req.Consumers {
		// Write the consumer record. Only update updated_at
		// if headers have changed.
		_, err = tx.Exec(`
INSERT INTO feed_consumer (name, url, headers, created_at, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (name, url) DO UPDATE SET
    headers = excluded.headers,
    updated_at = CASE
        WHEN excluded.headers != feed_consumer.headers THEN excluded.updated_at
        ELSE feed_consumer.updated_at
    END`,
			con.Name, req.URL, con.Headers, con.CreatedAt, con.UpdatedAt)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("inserting feed consumer: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (s *SQLiteStorage) DeleteFeedMetadata(url string, sha256 string) error {
	_, err := s.feedDB.Exec(`
DELETE FROM feed
WHERE url = ? AND sha256 = ?
`, url, sha256)
	return err
}

func (s *SQLiteStorage) GetReader(feedID string) (FeedReader, error) {
	db, found := s.feeds[feedID]
	if found {
		return &SQLiteFeedReader{
			db: db,
		}, nil
	}
	if !s.OnDisk {
		return nil, fmt.Errorf("feed %s does not exist", feedID)
	}

	sourceName := ":memory:"
	if s.OnDisk {
		sourceName = s.Directory + "/" + feedID + ".db"
		if _, err := os.Stat(sourceName); os.IsNotExist(err) {
			return nil, fmt.Errorf("feed %s does not exist at %s", feedID, sourceName)
		}
	}

	db, err := sql.Open("sqlite3", sourceName)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	s.feeds[feedID] = db

	return &SQLiteFeedReader{
		db: db,
	}, nil
}

func (s *SQLiteStorage) GetWriter(feedID string) (FeedWriter, error) {
	sourceName := ":memory:"
	if s.OnDisk {
		sourceName = s.Directory + "/" + feedID + ".db"
		// delete file if it exists
		if _, err := os.Stat(sourceName); err == nil {
			err := os.Remove(sourceName)
			if err != nil {
				return nil, fmt.Errorf("removing existing database: %w", err)
			}
		}
	}

	db, err := sql.Open("sqlite3", sourceName)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	for name, query := range map[string]string{
		"agency": `
CREATE TABLE agency (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    timezone TEXT NOT NULL
);`,
		"stops": `
CREATE TABLE stops (
    id TEXT PRIMARY KEY,
    code TEXT,
    name TEXT NOT NULL,
    desc TEXT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    url TEXT,
    location_type INTEGER NOT NULL,
    parent_station TEXT,
    platform_code TEXT
);
CREATE INDEX stops_parent_station ON stops (parent_station);
`,
		"routes": `
CREATE TABLE routes (
    id TEXT PRIMARY KEY,
    agency_id TEXT,
    short_name TEXT,
    long_name TEXT NOT NULL,
    desc TEXT,
    type INTEGER NOT NULL,
    url TEXT,
    color TEXT,
    text_color TEXT
);`,
		"trips": `
CREATE TABLE trips (
    id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    headsign TEXT,
    short_name TEXT,
    direction_id INTEGER
);
CREATE INDEX trips_route_id ON trips (route_id);
CREATE INDEX trips_service_id ON trips (service_id);
`,
		"stop_times": `
CREATE TABLE stop_times (
    trip_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    arrival_time TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    headsign TEXT
);
CREATE INDEX stop_times_trip_id ON stop_times (trip_id);
CREATE INDEX stop_times_stop_id ON stop_times (stop_id);
CREATE INDEX stop_times_arrival_time ON stop_times (arrival_time);
CREATE INDEX stop_times_departure_time ON stop_times (departure_time);
`,
		"calendar": `
CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    monday integer NOT NULL,
    tuesday integer NOT NULL,
    wednesday integer NOT NULL,
    thursday integer NOT NULL,
    friday integer NOT NULL,
    saturday integer NOT NULL,
    sunday integer NOT NULL
);`,
		"calendar_dates": `
CREATE TABLE calendar_dates (
    service_id TEXT NOT NULL,
    date TEXT NOT NULL,
    exception_type INTEGER NOT NULL
);`,
	} {
		_, err = db.Exec(query)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("creating %s table: %s", name, err)
		}
	}

	s.feeds[feedID] = db

	return &SQLiteFeedWriter{
		db: db,
	}, nil
}

func (f *SQLiteFeedWriter) WriteAgency(a *Agency) error {
	_, err := f.db.Exec(`
INSERT INTO agency (id, name, url, timezone)
VALUES (?, ?, ?, ?)`,
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

func (f *SQLiteFeedWriter) WriteStop(stop *Stop) error {
	_, err := f.db.Exec(`
INSERT INTO stops (id, code, name, desc, lat, lon, url, location_type, parent_station, platform_code)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		stop.ID,
		stop.Code,
		stop.Name,
		stop.Desc,
		stop.Lat,
		stop.Lon,
		stop.URL,
		stop.LocationType,
		stop.ParentStation,
		stop.PlatformCode,
	)
	if err != nil {
		return fmt.Errorf("inserting stop: %w", err)
	}
	return nil
}

func (f *SQLiteFeedWriter) WriteRoute(route *Route) error {
	_, err := f.db.Exec(`
INSERT INTO routes (id, agency_id, short_name, long_name, desc, type, url, color, text_color)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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

func (f *SQLiteFeedWriter) BeginTrips() error {
	return nil
}

func (f *SQLiteFeedWriter) WriteTrip(trip *Trip) error {
	_, err := f.db.Exec(`
INSERT INTO trips (id, route_id, service_id, headsign, short_name, direction_id)
VALUES (?, ?, ?, ?, ?, ?)`,
		trip.ID,
		trip.RouteID,
		trip.ServiceID,
		trip.Headsign,
		trip.ShortName,
		trip.DirectionID,
	)
	if err != nil {
		return fmt.Errorf("inserting trip: %w", err)
	}
	return nil
}

func (f *SQLiteFeedWriter) EndTrips() error {
	return nil
}

func (f *SQLiteFeedWriter) BeginStopTimes() error {
	// transaction with prepared statement.
	var err error
	f.stopTimeInsertTx, err = f.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning stop_time insert transaction: %w", err)
	}

	f.stopTimeInsertQuery, err = f.stopTimeInsertTx.Prepare(`
INSERT INTO stop_times (trip_id, stop_id, stop_sequence, arrival_time, departure_time, stop_id, headsign)
VALUES (?, ?, ?, ?, ?, ? ,?)`)
	if err != nil {
		f.stopTimeInsertTx.Rollback()
		f.stopTimeInsertTx = nil
		return fmt.Errorf("preparing stop_time insert: %w", err)
	}

	return nil
}

func (f *SQLiteFeedWriter) WriteStopTime(stopTime *StopTime) error {
	_, err := f.stopTimeInsertQuery.Exec(
		stopTime.TripID,
		stopTime.StopID,
		stopTime.StopSequence,
		stopTime.Arrival,
		stopTime.Departure,
		stopTime.StopID,
		stopTime.Headsign,
	)
	if err != nil {
		f.stopTimeInsertQuery.Close()
		f.stopTimeInsertTx.Rollback()
		f.stopTimeInsertTx = nil
		f.stopTimeInsertQuery = nil
		return fmt.Errorf("inserting stop_time: %w", err)
	}

	return nil
}

func (f *SQLiteFeedWriter) EndStopTimes() error {
	// commit transaction and clean up
	f.stopTimeInsertQuery.Close()
	err := f.stopTimeInsertTx.Commit()
	if err != nil {
		return fmt.Errorf("committing stop_time insert transaction: %w", err)
	}
	f.stopTimeInsertTx = nil
	f.stopTimeInsertQuery = nil

	return nil
}

func (f *SQLiteFeedWriter) WriteCalendar(cal *Calendar) error {
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

	_, err := f.db.Exec(`
INSERT INTO calendar (service_id, start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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

func (f *SQLiteFeedWriter) WriteCalendarDate(cd *CalendarDate) error {
	_, err := f.db.Exec(`
INSERT INTO calendar_dates (service_id, date, exception_type)
VALUES (?, ?, ?)`,
		cd.ServiceID,
		cd.Date,
		cd.ExceptionType,
	)
	if err != nil {
		return fmt.Errorf("inserting calendar date: %w", err)
	}

	return nil
}

func (f *SQLiteFeedWriter) Close() error {
	_, err := f.db.Exec(`ANALYZE;`)
	if err != nil {
		f.db.Close()
		return fmt.Errorf("analyzing database: %s", err)
	}

	return nil
}

func (f *SQLiteFeedReader) ActiveServices(date string) ([]string, error) {
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

	rows, err := f.db.Query(`
WITH
Exceptions AS (
 	SELECT service_id, exception_type
	FROM calendar_dates
	WHERE date = ?
),
Regular AS (
	SELECT service_id
        FROM calendar
	WHERE `+weekday+` = 1 AND
              start_date <= ? AND
	      end_date >= ?
)
SELECT service_id
FROM Regular
WHERE service_id NOT IN (
	SELECT service_id FROM Exceptions WHERE exception_type = 2
)
UNION
SELECT service_id
FROM Exceptions
WHERE exception_type = 1
`, date, date, date)
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

func (f *SQLiteFeedReader) getStops() ([]*Stop, error) {
	row, err := f.db.Query(`
SELECT
    stops.id,
    stops.code,
    stops.name,
    stops.desc,
    stops.lat,
    stops.lon,
    stops.url,
    stops.location_type,
    stops.parent_station,
    stops.platform_code
FROM
    stops
WHERE
    stops.location_type = 0 AND parent_station = "" OR stops.location_type = 1`)
	if err != nil {
		return nil, fmt.Errorf("querying for nearby stops: %w", err)
	}

	stops := []*Stop{}
	for row.Next() {
		stop := &Stop{}
		err = row.Scan(
			&stop.ID,
			&stop.Code,
			&stop.Name,
			&stop.Desc,
			&stop.Lat,
			&stop.Lon,
			&stop.URL,
			&stop.LocationType,
			&stop.ParentStation,
			&stop.PlatformCode,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning stop: %w", err)
		}

		stops = append(stops, stop)
	}

	return stops, nil
}

func (f *SQLiteFeedReader) getStopsByRouteType(routeTypes []RouteType) ([]*Stop, error) {
	queryValues := []interface{}{}
	for _, rt := range routeTypes {
		queryValues = append(queryValues, rt)
	}
	routeTypePlaceholders := []string{}
	for range routeTypes {
		routeTypePlaceholders = append(routeTypePlaceholders, "?")
	}

	rows, err := f.db.Query(`
SELECT
    stops.id,
    stops.code,
    stops.name,
    stops.desc,
    stops.lat,
    stops.lon,
    stops.url,
    stops.location_type,
    stops.parent_station,
    stops.platform_code,
    parent.id,
    parent.code,
    parent.name,
    parent.desc,
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

func (f *SQLiteFeedReader) NearbyStops(lat float64, lng float64, limit int, routeTypes []RouteType) ([]Stop, error) {
	var stops []*Stop
	var err error

	if len(routeTypes) == 0 {
		stops, err = f.getStops()
		if err != nil {
			return nil, fmt.Errorf("getting all stops: %w", err)
		}
	} else {
		// NOTE: With this query, only stops that have an
		// actual trip of the correct route type passing
		// through will be included in the result.
		stops, err = f.getStopsByRouteType(routeTypes)
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

func (f *SQLiteFeedReader) Agencies() ([]*Agency, error) {
	rows, err := f.db.Query(`
SELECT id, name, url, timezone
FROM agency`)
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

func (f *SQLiteFeedReader) Stops() ([]*Stop, error) {
	rows, err := f.db.Query(`
SELECT id, code, name, desc, lat, lon, url, location_type, parent_station, platform_code
FROM stops`)
	if err != nil {
		return nil, fmt.Errorf("querying stops: %w", err)
	}
	defer rows.Close()

	stops := []*Stop{}
	for rows.Next() {
		s := &Stop{}
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
		)
		if err != nil {
			return nil, fmt.Errorf("scanning stop: %w", err)
		}
		stops = append(stops, s)
	}

	return stops, nil
}

func (f *SQLiteFeedReader) Routes() ([]*Route, error) {
	rows, err := f.db.Query(`
SELECT id, agency_id, short_name, long_name, desc, type, url, color, text_color
FROM routes`)
	if err != nil {
		return nil, fmt.Errorf("querying routes: %w", err)
	}
	defer rows.Close()

	routes := []*Route{}
	for rows.Next() {
		r := &Route{}
		err := rows.Scan(
			&r.ID,
			&r.AgencyID,
			&r.ShortName,
			&r.LongName,
			&r.Desc,
			&r.Type,
			&r.URL,
			&r.Color,
			&r.TextColor,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning route: %w", err)
		}
		routes = append(routes, r)
	}

	return routes, nil
}

func (f *SQLiteFeedReader) Trips() ([]*Trip, error) {
	rows, err := f.db.Query(`
SELECT id, route_id, service_id, headsign, short_name, direction_id
FROM trips`)
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

func (f *SQLiteFeedReader) StopTimes() ([]*StopTime, error) {
	rows, err := f.db.Query(`
SELECT trip_id, stop_id, headsign, stop_sequence, arrival_time, departure_time
FROM stop_times`)
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

func (f *SQLiteFeedReader) Calendars() ([]*Calendar, error) {
	rows, err := f.db.Query(`
SELECT service_id, start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday
FROM calendar`)
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

func (f *SQLiteFeedReader) CalendarDates() ([]*CalendarDate, error) {
	rows, err := f.db.Query(`
SELECT service_id, date, exception_type
FROM calendar_dates`)
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

func (f *SQLiteFeedReader) MinMaxStopSeq() (map[string][2]uint32, error) {
	rows, err := f.db.Query(`
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

func (f *SQLiteFeedReader) StopTimeEvents(filter StopTimeEventFilter) ([]*StopTimeEvent, error) {
	baseQuery := `
SELECT
    stops.id,
    stops.code,
    stops.name,
    stops.desc,
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
    routes.desc,
    routes.type,
    routes.url,
    routes.color,
    routes.text_color
FROM stop_times
INNER JOIN stops ON stop_times.stop_id = stops.id
INNER JOIN trips ON stop_times.trip_id = trips.id
INNER JOIN routes ON trips.route_id = routes.id
`

	// Apply filters to query
	fParams, fVals := []string{}, []string{}

	if filter.StopID != "" {
		fParams = append(fParams, "(stops.id == ? OR stops.parent_station == ?)")
		fVals = append(fVals, filter.StopID, filter.StopID)
	}

	if filter.RouteID != "" {
		fParams = append(fParams, "routes.id = ?")
		fVals = append(fVals, filter.RouteID)
	}

	if len(filter.TripIDs) > 0 {
		tripIDPlaceholders := []string{}
		for range filter.TripIDs {
			tripIDPlaceholders = append(tripIDPlaceholders, "?")
		}
		fParams = append(fParams, "trips.id IN ("+strings.Join(tripIDPlaceholders, ", ")+")")
		fVals = append(fVals, filter.TripIDs...)
	}

	if len(filter.ServiceIDs) > 0 {
		serviceIDPlaceholders := []string{}
		for range filter.ServiceIDs {
			serviceIDPlaceholders = append(serviceIDPlaceholders, "?")
		}
		fParams = append(fParams, "trips.service_id IN ("+strings.Join(serviceIDPlaceholders, ", ")+")")
		fVals = append(fVals, filter.ServiceIDs...)
	}

	if filter.DirectionID > -1 {
		fParams = append(fParams, "trips.direction_id = ?")
		fVals = append(fVals, fmt.Sprintf("%d", filter.DirectionID))
	}

	if filter.ArrivalStart != "" {
		fParams = append(fParams, "stop_times.arrival_time >= ?")
		fVals = append(fVals, filter.ArrivalStart)
	}

	if filter.ArrivalEnd != "" {
		fParams = append(fParams, "stop_times.arrival_time <= ?")
		fVals = append(fVals, filter.ArrivalEnd)
	}

	if filter.DepartureStart != "" {
		fParams = append(fParams, "stop_times.departure_time >= ?")
		fVals = append(fVals, filter.DepartureStart)
	}

	if filter.DepartureEnd != "" {
		fParams = append(fParams, "stop_times.departure_time <= ?")
		fVals = append(fVals, filter.DepartureEnd)
	}

	if len(filter.RouteTypes) > 0 {
		placeholders := []string{}
		for _, rt := range filter.RouteTypes {
			placeholders = append(placeholders, "?")
			fVals = append(fVals, fmt.Sprintf("%d", rt))
		}
		fParams = append(fParams, "routes.type IN ("+strings.Join(placeholders, ", ")+")")
	}

	// Run query
	var query string
	if len(fParams) > 0 {
		query = baseQuery + " WHERE " + strings.Join(fParams, " AND ")
	} else {
		query = baseQuery
	}

	queryValues := []interface{}{}
	for _, v := range fVals {
		queryValues = append(queryValues, v)
	}

	rows, err := f.db.Query(query, queryValues...)
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

		err = rows.Scan(
			&stop.ID,
			&stop.Code,
			&stop.Name,
			&stop.Desc,
			&stop.Lat,
			&stop.Lon,
			&stop.URL,
			&stop.LocationType,
			&stop.ParentStation,
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
	for id := range parents {
		parentIDs = append(parentIDs, id)
		placeholders = append(placeholders, "?")
	}

	rows, err = f.db.Query(`
SELECT id, code, name, desc, lat, lon, url, location_type, platform_code
FROM stops
WHERE id IN (`+strings.Join(placeholders, ", ")+`)
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

func (f *SQLiteFeedReader) RouteDirections(stopID string) ([]*RouteDirection, error) {

	rows, err := f.db.Query(`
SELECT trips.route_id, trips.direction_id, trips.headsign, stop_times.headsign
FROM stop_times
INNER JOIN trips ON trips.id = stop_times.trip_id
WHERE stop_times.stop_id = ? AND
      stop_times.stop_sequence != (SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = trips.id)
`, stopID)
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
