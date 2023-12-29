package storage_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/parse"
	"tidbyt.dev/gtfs/storage"
)

// Tests of the storage implementations. The in-memory and sqlite
// implementations are always run, while postgres require the
// PostgresConnStr below to be set.

const (
	PostgresConnStr = "" // "postgres://postgres:mysecretpassword@localhost:5432/gtfs?sslmode=disable"
)

type StorageBuilder func() (storage.Storage, error)

func readerFromFiles(t *testing.T, sb StorageBuilder, files map[string][]string) storage.FeedReader {
	storage, err := sb()
	require.NoError(t, err)

	writer, err := storage.GetWriter("unit-test")
	require.NoError(t, err)

	services := map[string]bool{}
	routes := map[string]bool{}
	trips := map[string]bool{}
	stops := map[string]bool{}

	if files["calendar.txt"] != nil {
		services, _, _, err = parse.ParseCalendar(
			writer,
			bytes.NewBufferString(strings.Join(files["calendar.txt"], "\n")),
		)
		require.NoError(t, err)
	}
	if files["calendar_dates.txt"] != nil {
		cdServices, _, _, err := parse.ParseCalendarDates(
			writer,
			bytes.NewBufferString(strings.Join(files["calendar_dates.txt"], "\n")),
		)
		require.NoError(t, err)
		for service := range cdServices {
			services[service] = true
		}
	}
	if files["routes.txt"] != nil {
		routes, err = parse.ParseRoutes(
			writer,
			bytes.NewBufferString(strings.Join(files["routes.txt"], "\n")),
			map[string]bool{},
		)
		require.NoError(t, err)
	}
	if files["trips.txt"] != nil {
		require.NoError(t, writer.BeginTrips())
		trips, err = parse.ParseTrips(
			writer,
			bytes.NewBufferString(strings.Join(files["trips.txt"], "\n")),
			routes,
			services,
		)
		require.NoError(t, err)
		require.NoError(t, writer.EndTrips())
	}
	if files["stops.txt"] != nil {
		stops, err = parse.ParseStops(
			writer,
			bytes.NewBufferString(strings.Join(files["stops.txt"], "\n")),
		)
		require.NoError(t, err)
	}
	if files["stop_times.txt"] != nil {
		require.NoError(t, writer.BeginStopTimes())
		_, _, err := parse.ParseStopTimes(
			writer,
			bytes.NewBufferString(strings.Join(files["stop_times.txt"], "\n")),
			trips,
			stops,
		)
		require.NoError(t, err)
		require.NoError(t, writer.EndStopTimes())
	}

	require.NoError(t, writer.Close())

	reader, err := storage.GetReader("unit-test")
	require.NoError(t, err)

	return reader
}

func testInitiallyEmpty(t *testing.T, sb StorageBuilder) {
	s, err := sb()
	require.NoError(t, err)

	/* These don't make a whole lot of sense TBH.
	// Before feed is created, it can't be read
	_, err = s.GetReader("unit-test")G
	assert.Error(t, err)
	*/

	// Create it
	writer, err := s.GetWriter("unit-test")
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())

	// And all fields can be read
	reader, err := s.GetReader("unit-test")
	assert.NoError(t, err)

	// NTS: not sure it's reasonable to expect that just calling
	// Finish() on a writer renders the feed readable.

	agencies, err := reader.Agencies()
	require.NoError(t, err)
	assert.Equal(t, 0, len(agencies))

	stops, err := reader.Stops()
	require.NoError(t, err)
	assert.Equal(t, 0, len(stops))

	routes, err := reader.Routes()
	require.NoError(t, err)
	assert.Equal(t, 0, len(routes))

	trips, err := reader.Trips()
	require.NoError(t, err)
	assert.Equal(t, 0, len(trips))

	stopTimes, err := reader.StopTimes()
	require.NoError(t, err)
	assert.Equal(t, 0, len(stopTimes))

	calendar, err := reader.Calendars()
	require.NoError(t, err)
	assert.Equal(t, 0, len(calendar))

	calendarDates, err := reader.CalendarDates()
	require.NoError(t, err)
	assert.Equal(t, 0, len(calendarDates))
}

func testBasicReadingAndWriting(t *testing.T, sb StorageBuilder) {
	s, err := sb()
	require.NoError(t, err)

	writer, err := s.GetWriter("unit-test")
	require.NoError(t, err)

	// Write some Agencies
	err = writer.WriteAgency(&storage.Agency{
		ID:       "agency_1",
		Name:     "Agency 1",
		URL:      "http://example.com/agency_1",
		Timezone: "America/Los_Angeles",
	})
	require.NoError(t, err)
	err = writer.WriteAgency(&storage.Agency{
		ID:       "agency_2",
		Name:     "Agency 2",
		URL:      "http://example.com/agency_2",
		Timezone: "America/New_York",
	})
	require.NoError(t, err)

	// Write some Stops
	err = writer.WriteStop(&storage.Stop{
		ID:            "stop_1",
		Code:          "stop_code_1",
		Name:          "Stop 1",
		Desc:          "Stop description 1",
		Lat:           1.0,
		Lon:           2.0,
		URL:           "http://example.com/stop_1",
		LocationType:  storage.LocationTypeStop,
		ParentStation: "stop_2",
		PlatformCode:  "platform_1",
	})
	require.NoError(t, err)
	err = writer.WriteStop(&storage.Stop{
		ID:            "stop_2",
		Code:          "stop_code_2",
		Name:          "Stop 2",
		Desc:          "Stop description 2",
		Lat:           3.0,
		Lon:           4.0,
		URL:           "http://example.com/stop_2",
		LocationType:  storage.LocationTypeStation,
		ParentStation: "",
		PlatformCode:  "",
	})
	require.NoError(t, err)

	// Write some Routes
	err = writer.WriteRoute(&storage.Route{
		ID:        "route_1",
		AgencyID:  "agency_1",
		ShortName: "1",
		LongName:  "Route 1",
		Desc:      "Route description 1",
		Type:      storage.RouteTypeTram,
		URL:       "http://example.com/route_1",
		Color:     "000011",
		TextColor: "FFFF22",
	})
	require.NoError(t, err)
	err = writer.WriteRoute(&storage.Route{
		ID:        "route_2",
		AgencyID:  "agency_2",
		ShortName: "2",
		LongName:  "Route 2",
		Desc:      "Route description 2",
		Type:      storage.RouteTypeSubway,
		URL:       "http://example.com/route_2",
		Color:     "000022",
		TextColor: "FFFF33",
	})
	require.NoError(t, err)

	// Write some Trips
	require.NoError(t, writer.BeginTrips())
	err = writer.WriteTrip(&storage.Trip{
		ID:          "trip_1",
		RouteID:     "route_1",
		ServiceID:   "service_1",
		Headsign:    "Headsign 1",
		ShortName:   "R1",
		DirectionID: 0,
	})
	require.NoError(t, err)
	err = writer.WriteTrip(&storage.Trip{
		ID:          "trip_2",
		RouteID:     "route_2",
		ServiceID:   "service_2",
		Headsign:    "Headsign 2",
		ShortName:   "R2",
		DirectionID: 1,
	})
	require.NoError(t, err)
	require.NoError(t, writer.EndTrips())

	// Write some StopTimes
	require.NoError(t, writer.BeginStopTimes())
	err = writer.WriteStopTime(&storage.StopTime{
		TripID:       "trip_1",
		StopID:       "stop_1",
		Headsign:     "StopTime headsign 1",
		StopSequence: 1,
		Arrival:      "142033",
		Departure:    "142034",
	})
	require.NoError(t, err)
	err = writer.WriteStopTime(&storage.StopTime{
		TripID:       "trip_2",
		StopID:       "stop_2",
		Headsign:     "StopTime headsign 2",
		StopSequence: 2,
		Arrival:      "142035",
		Departure:    "142036",
	})
	require.NoError(t, err)
	require.NoError(t, writer.EndStopTimes())

	// Write some Calendars
	err = writer.WriteCalendar(&storage.Calendar{
		ServiceID: "service_1",
		StartDate: "20200101",
		EndDate:   "20201231",
		Weekday:   0x7f,
	})
	require.NoError(t, err)
	err = writer.WriteCalendar(&storage.Calendar{
		ServiceID: "service_2",
		StartDate: "20210101",
		EndDate:   "20211231",
		Weekday:   1 << time.Tuesday,
	})
	require.NoError(t, err)

	// Write some CalendarDates
	err = writer.WriteCalendarDate(&storage.CalendarDate{
		ServiceID:     "service_1",
		Date:          "20210101",
		ExceptionType: 1,
	})
	require.NoError(t, err)
	err = writer.WriteCalendarDate(&storage.CalendarDate{
		ServiceID:     "service_2",
		Date:          "20200101",
		ExceptionType: 2,
	})
	require.NoError(t, err)

	require.NoError(t, writer.Close())

	// Check if all the data can be read back correctly through
	// the simple readers.

	reader, err := s.GetReader("unit-test")
	require.NoError(t, err)

	agencies, err := reader.Agencies()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*storage.Agency{
		{
			ID:       "agency_1",
			Name:     "Agency 1",
			URL:      "http://example.com/agency_1",
			Timezone: "America/Los_Angeles",
		},
		{
			ID:       "agency_2",
			Name:     "Agency 2",
			URL:      "http://example.com/agency_2",
			Timezone: "America/New_York",
		},
	}, agencies)

	stops, err := reader.Stops()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*storage.Stop{
		{
			ID:            "stop_1",
			Code:          "stop_code_1",
			Name:          "Stop 1",
			Desc:          "Stop description 1",
			Lat:           1.0,
			Lon:           2.0,
			URL:           "http://example.com/stop_1",
			LocationType:  0,
			ParentStation: "stop_2",
			PlatformCode:  "platform_1",
		},
		{
			ID:            "stop_2",
			Code:          "stop_code_2",
			Name:          "Stop 2",
			Desc:          "Stop description 2",
			Lat:           3.0,
			Lon:           4.0,
			URL:           "http://example.com/stop_2",
			LocationType:  1,
			ParentStation: "",
			PlatformCode:  "",
		},
	}, stops)

	routes, err := reader.Routes()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*storage.Route{
		{
			ID:        "route_1",
			AgencyID:  "agency_1",
			ShortName: "1",
			LongName:  "Route 1",
			Desc:      "Route description 1",
			Type:      storage.RouteTypeTram,
			URL:       "http://example.com/route_1",
			Color:     "000011",
			TextColor: "FFFF22",
		},
		{
			ID:        "route_2",
			AgencyID:  "agency_2",
			ShortName: "2",
			LongName:  "Route 2",
			Desc:      "Route description 2",
			Type:      storage.RouteTypeSubway,
			URL:       "http://example.com/route_2",
			Color:     "000022",
			TextColor: "FFFF33",
		},
	}, routes)

	trips, err := reader.Trips()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*storage.Trip{
		{
			ID:          "trip_1",
			RouteID:     "route_1",
			ServiceID:   "service_1",
			Headsign:    "Headsign 1",
			ShortName:   "R1",
			DirectionID: 0,
		},
		{
			ID:          "trip_2",
			RouteID:     "route_2",
			ServiceID:   "service_2",
			Headsign:    "Headsign 2",
			ShortName:   "R2",
			DirectionID: 1,
		},
	}, trips)

	stopTimes, err := reader.StopTimes()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*storage.StopTime{
		{
			TripID:       "trip_1",
			StopID:       "stop_1",
			Headsign:     "StopTime headsign 1",
			StopSequence: 1,
			Arrival:      "142033",
			Departure:    "142034",
		},
		{
			TripID:       "trip_2",
			StopID:       "stop_2",
			Headsign:     "StopTime headsign 2",
			StopSequence: 2,
			Arrival:      "142035",
			Departure:    "142036",
		},
	}, stopTimes)

	calendars, err := reader.Calendars()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*storage.Calendar{
		{
			ServiceID: "service_1",
			StartDate: "20200101",
			EndDate:   "20201231",
			Weekday:   0x7f,
		},
		{
			ServiceID: "service_2",
			StartDate: "20210101",
			EndDate:   "20211231",
			Weekday:   1 << time.Tuesday,
		},
	}, calendars)

	calendarDates, err := reader.CalendarDates()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*storage.CalendarDate{
		{
			ServiceID:     "service_1",
			Date:          "20210101",
			ExceptionType: 1,
		},
		{
			ServiceID:     "service_2",
			Date:          "20200101",
			ExceptionType: 2,
		},
	}, calendarDates)

}

func testActiveServicesCalendarOnly(t *testing.T, sb StorageBuilder) {
	// Calendar only
	// Feb 15-17 spans Saturday - Monday. This cal is not active
	// Sunday.
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"s,20200215,20200217,1,0,0,0,0,1,0",
		},
	})
	for _, c := range []struct {
		Date   string
		Active []string
		Msg    string
	}{
		{"20200214", []string{}, "friday outside date range"},
		{"20200215", []string{"s"}, "saturday should be active"},
		{"20200216", []string{}, "sunday should not be active"},
		{"20200217", []string{"s"}, "monday should be active"},
		{"20200218", []string{}, "tuesday outside date range"},
	} {
		active, err := reader.ActiveServices(c.Date)
		assert.NoError(t, err)
		assert.Equal(t, c.Active, active, c.Msg)
	}
}

func testActiveServicesServiceAdded(t *testing.T, sb StorageBuilder) {
	// Same calendar as above, but with service added on the (Sunday) 16th
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"s,20200215,20200217,1,0,0,0,0,1,0",
		},
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"s,20200216,1",
		},
	})
	for _, c := range []struct {
		Date   string
		Active []string
		Msg    string
	}{
		{"20200214", []string{}, "friday outside date range"},
		{"20200215", []string{"s"}, "saturday should be active"},
		{"20200216", []string{"s"}, "sunday has calendar date added"},
		{"20200217", []string{"s"}, "monday should be active"},
		{"20200218", []string{}, "tuesday outside date range"},
	} {
		active, err := reader.ActiveServices(c.Date)
		assert.NoError(t, err)
		assert.Equal(t, c.Active, active, c.Msg)
	}
}

func testActiveServicesServiceRemoved(t *testing.T, sb StorageBuilder) {
	// Same calendar, but with service removed on the 15th
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"s,20200215,20200217,1,0,0,0,0,1,0",
		},
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"s,20200215,2",
		},
	})
	for _, c := range []struct {
		Date   string
		Active []string
		Msg    string
	}{
		{"20200214", []string{}, "friday outside date range"},
		{"20200215", []string{}, "saturday was removed (calendary date)"},
		{"20200216", []string{}, "sunday was never active"},
		{"20200217", []string{"s"}, "monday should be active"},
		{"20200218", []string{}, "tuesday outside date range"},
	} {
		active, err := reader.ActiveServices(c.Date)
		assert.NoError(t, err)
		assert.Equal(t, c.Active, active, c.Msg)
	}
}

func testActiveServicesServiceAddedOutsideDateRange(t *testing.T, sb StorageBuilder) {
	// Same calendar, but with service added on a day outside of date range
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"s,20200215,20200217,1,0,0,0,0,1,0",
		},
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"s,20200214,1",
		},
	})
	for _, c := range []struct {
		Date   string
		Active []string
		Msg    string
	}{
		{"20200213", []string{}, "thursday outside date range"},
		{"20200214", []string{"s"}, "added friday (calendar date)"},
		{"20200215", []string{"s"}, "saturday should be active"},
		{"20200216", []string{}, "sunday was never active"},
		{"20200217", []string{"s"}, "monday should be active"},
		{"20200218", []string{}, "tuesday outside date range"},
	} {
		active, err := reader.ActiveServices(c.Date)
		assert.NoError(t, err)
		assert.Equal(t, c.Active, active, c.Msg)
	}
}

func testActiveServicesCalendarDatesOnly(t *testing.T, sb StorageBuilder) {
	// CalendarDate only (feeds are allowed to use calendar_date
	// without any calendar records)
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"s,20200216,1",
		},
	})
	for _, c := range []struct {
		Date   string
		Active []string
		Msg    string
	}{
		{"20200215", []string{}, "saturday should not be active"},
		{"20200216", []string{"s"}, "sunday should be active"},
		{"20200217", []string{}, "monday should not be active"},
	} {
		active, err := reader.ActiveServices(c.Date)
		assert.NoError(t, err)
		assert.Equal(t, c.Active, active, c.Msg)
	}
}

func testActiveServicesCalendarDatesOnlyRemoved(t *testing.T, sb StorageBuilder) {
	// Service disabled on a day without service
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"s,20200216,2",
		},
	})
	for _, c := range []struct {
		Date   string
		Active []string
		Msg    string
	}{
		{"20200215", []string{}, "saturday should not be active"},
		{"20200216", []string{}, "sunday should not be active"},
		{"20200217", []string{}, "monday should not be active"},
	} {
		active, err := reader.ActiveServices(c.Date)
		assert.NoError(t, err)
		assert.Equal(t, c.Active, active, c.Msg)
	}
}

func testActiveServicesLotsOfRecords(t *testing.T, sb StorageBuilder) {
	// Multiple services with dates removed and added all over the
	// place. Jan 1st, 2023, was a monday.
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"wd,20230101,20230131,1,1,1,1,1,0,0", // weekdays
			"we,20230101,20230131,0,0,0,0,0,1,1", // weekends
			"xx,20230115,20230121,1,1,1,1,1,1,1", // full week mid-month
		},
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"wd,20230102,2", // remove monday the 2nd
			"we,20230107,2", // remove sunday the 7th
			"wd,20230202,1", // add thursday feb 2nd
			"we,20230205,1", // add sunday feb 5th
			"xx,20230120,2", // remove saturday the 20th (from mid-month schedule)
			"xx,20230121,2", // remove sunday the 21st (from mid-month schedule)
		},
	})
	for _, c := range []struct {
		Date   string
		Active []string
		Msg    string
	}{
		{"20221231", []string{}, "dec 31st outside schedule"},
		{"20230101", []string{"we"}, "jan 1st weekend"},
		{"20230102", []string{}, "jan 2nd removed"},
		{"20230103", []string{"wd"}, "jan 3rd weekday"},
		{"20230104", []string{"wd"}, "jan 4th weekday"},
		{"20230105", []string{"wd"}, "jan 5th weekday"},
		{"20230106", []string{"wd"}, "jan 6th weekday"},
		{"20230107", []string{}, "jan 7th removed"},
		{"20230108", []string{"we"}, "jan 8th weekend"},
		{"20230109", []string{"wd"}, "jan 9th weekday"},
		{"20230110", []string{"wd"}, "jan 10th weekday"},
		{"20230111", []string{"wd"}, "jan 11th weekday"},
		{"20230112", []string{"wd"}, "jan 12th weekday"},
		{"20230113", []string{"wd"}, "jan 13th weekday"},
		{"20230114", []string{"we"}, "jan 14th weekend"},
		{"20230115", []string{"we", "xx"}, "jan 15th mid-weeknd + mid-month"},
		{"20230116", []string{"wd", "xx"}, "jan 16th weekday + mid-month"},
		{"20230117", []string{"wd", "xx"}, "jan 17th weekday + mid-month"},
		{"20230118", []string{"wd", "xx"}, "jan 18th weekday + mid-month"},
		{"20230119", []string{"wd", "xx"}, "jan 19th weekday + mid-month"},
		{"20230120", []string{"wd"}, "jan 20th weekday (mid-month disabled)"},
		{"20230121", []string{"we"}, "jan 21st weekend (mid-month disabled)"},
		{"20230122", []string{"we"}, "jan 22nd weekend"},
		{"20230201", []string{}, "feb 1st outside schedule"},
		{"20230202", []string{"wd"}, "feb 2nd was added"},
		{"20230203", []string{}, "feb 3rd outside schedule"},
		{"20230204", []string{}, "feb 4th outside schedule"},
		{"20230205", []string{"we"}, "feb 5th was added"},
		{"20230206", []string{}, "feb 6th outside schedule"},
	} {
		active, err := reader.ActiveServices(c.Date)
		assert.NoError(t, err)
		sort.Strings(active)
		assert.Equal(t, c.Active, active, c.Msg)
	}
}

func testActiveServicesNoCalendar(t *testing.T, sb StorageBuilder) {
	// Neither calendar nor calendar_date means no service
	reader := readerFromFiles(t, sb, map[string][]string{})
	active, err := reader.ActiveServices("20200215")
	assert.NoError(t, err)
	assert.Equal(t, []string{}, active, "no service at all means nothing's ever active")

}

// Tests StopTimeEvents with filters on StopID, Arrival, Departure and DirectionID
func testStopTimeEventFilter_TimeStopdirection(t *testing.T, sb StorageBuilder) {
	// Single route with two stops, and several trips passing through over time.
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday",
			"weekday,20200101,20200131,1",
		},
		"stops.txt": {
			"stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,location_type",
			"a,aa,A,StopA,96,69,http://stops/a,0",
			"b,bb,B,StopB,96,69,http://stops/b,0",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"r,R,0",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,headsign,direction_id",
			"1,r,weekday,south,0",
			"2,r,weekday,north,1",
			"3,r,weekday,south,0",
			"4,r,weekday,north,1",
			"5,r,weekday,south,0",
			"6,r,weekday,north,1",
			"7,r,weekday,south,0",
			"8,r,weekday,north,1",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_headsign,stop_sequence,arrival_time,departure_time",
			"1,a,,1,00:00:00,00:00:30",
			"1,b,,2,00:01:00,00:01:30",
			"2,b,,1,01:02:00,01:02:30",
			"2,a,,2,01:03:00,01:03:30",
			"3,a,,1,02:04:00,02:04:30",
			"3,b,,2,02:05:00,02:05:30",
			"4,b,,1,03:06:00,03:06:30",
			"4,a,,2,03:07:00,03:07:30",
			"5,a,,1,04:08:00,04:08:30",
			"5,b,,2,04:09:00,04:09:30",
			"6,b,,1,05:10:00,05:10:30",
			"6,a,,2,05:11:00,05:11:30",
			"7,a,,1,06:12:00,06:12:30",
			"7,b,,2,06:13:00,06:13:30",
			"8,b,,1,07:14:00,07:14:30",
			"8,a,,2,07:15:00,07:15:30",
		},
	})

	// Between 2 and 5 AM, trips 3 - 4 are arriving at stop a
	events, err := reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:   []string{"weekday"},
		StopID:       "a",
		ArrivalStart: "020000",
		ArrivalEnd:   "050000",
		DirectionID:  -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "3",
		StopID:       "a",
		StopSequence: 1,
		Arrival:      "020400",
		Departure:    "020430",
	}, events[0].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "4",
		StopID:       "a",
		StopSequence: 2,
		Arrival:      "030700",
		Departure:    "030730",
	}, events[1].StopTime)

	// Between 6 and 7, trip 7 departs from stop b
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:     []string{"weekday"},
		StopID:         "b",
		DepartureStart: "060000",
		DepartureEnd:   "070000",
		DirectionID:    -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "7",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "061300",
		Departure:    "061330",
	}, events[0].StopTime)

	// After 6:12:45, there are 3 stop times departing from some stop
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:     []string{"weekday"},
		DepartureStart: "061245",
		DirectionID:    -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "7",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "061300",
		Departure:    "061330",
	}, events[0].StopTime)

	assert.Equal(t, &storage.StopTime{
		TripID:       "8",
		StopID:       "b",
		StopSequence: 1,
		Arrival:      "071400",
		Departure:    "071430",
	}, events[1].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "8",
		StopID:       "a",
		StopSequence: 2,
		Arrival:      "071500",
		Departure:    "071530",
	}, events[2].StopTime)

	// Before 1, there's 1 arrival at stop a
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs: []string{"weekday"},
		StopID:     "a",
		ArrivalEnd: "010000",
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "1",
		StopID:       "a",
		StopSequence: 1,
		Arrival:      "000000",
		Departure:    "000030",
	}, events[0].StopTime)

	// Before (or at) 2:05:30, there are 2 southbound (id 0)
	// departures from stop b.
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:   []string{"weekday"},
		StopID:       "b",
		DepartureEnd: "020530",
		DirectionID:  0, // southbound
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "1",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "000100",
		Departure:    "000130",
	}, events[0].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "3",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "020500",
		Departure:    "020530",
	}, events[1].StopTime)

	// After (or at) 05:11:00, there are 2 northbound (id 1)
	// arrivals at stop a
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:   []string{"weekday"},
		StopID:       "a",
		ArrivalStart: "051100",
		DirectionID:  1, // northbound
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "6",
		StopID:       "a",
		StopSequence: 2,
		Arrival:      "051100",
		Departure:    "051130",
	}, events[0].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "8",
		StopID:       "a",
		StopSequence: 2,
		Arrival:      "071500",
		Departure:    "071530",
	}, events[1].StopTime)

	// Direction only
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:  []string{"weekday"},
		DirectionID: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, 8, len(events))

	// Direction and stop
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:  []string{"weekday"},
		StopID:      "a",
		DirectionID: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(events))
}

// Tests StopTimeEvents with filters on RouteID and RouteType
func testStopTimeEventFilter_RouteAndRouteType(t *testing.T, sb StorageBuilder) {
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday",
			"weekday,20170101,20170131,1",
		},
		"stops.txt": {
			"stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,location_type",
			"a,aa,A,StopA,96,69,http://stops/a,0",
			"b,bb,B,StopB,96,69,http://stops/b,0",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"r1,R1,0", // Tram
			"r2,R2,1", // Subway
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"r1_t1,r1,weekday,0",
			"r1_t2,r1,weekday,1",
			"r2_t1,r2,weekday,0",
			"r2_t2,r2,weekday,1",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,arrival_time,departure_time",
			"r1_t1,a,1,00:00:00,00:00:30",
			"r1_t1,b,2,00:01:00,00:01:30",
			"r1_t2,b,1,00:02:00,00:02:30",
			"r1_t2,a,2,00:03:00,00:03:30",
			"r2_t1,a,1,00:04:00,00:04:30",
			"r2_t1,b,2,00:05:00,00:05:30",
			"r2_t2,b,1,00:06:00,00:06:30",
			"r2_t2,a,2,00:07:00,00:07:30",
		},
	})

	// Route r1 has 4 stop events
	events, err := reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:  []string{"weekday"},
		RouteID:     "r1",
		DirectionID: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "r1_t1",
		StopID:       "a",
		StopSequence: 1,
		Arrival:      "000000",
		Departure:    "000030",
	}, events[0].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "r1_t1",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "000100",
		Departure:    "000130",
	}, events[1].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "r1_t2",
		StopID:       "b",
		StopSequence: 1,
		Arrival:      "000200",
		Departure:    "000230",
	}, events[2].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "r1_t2",
		StopID:       "a",
		StopSequence: 2,
		Arrival:      "000300",
		Departure:    "000330",
	}, events[3].StopTime)

	// Route r1 has a stop events at stop b departing after 00:02:00
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:     []string{"weekday"},
		RouteID:        "r1",
		DepartureStart: "000200",
		StopID:         "b",
		DirectionID:    -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "r1_t2",
		StopID:       "b",
		StopSequence: 1,
		Arrival:      "000200",
		Departure:    "000230",
	}, events[0].StopTime)

	// Stop b has 2 Trams stopping there
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:  []string{"weekday"},
		StopID:      "b",
		RouteTypes:  []storage.RouteType{storage.RouteTypeTram},
		DirectionID: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "r1_t1",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "000100",
		Departure:    "000130",
	}, events[0].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "r1_t2",
		StopID:       "b",
		StopSequence: 1,
		Arrival:      "000200",
		Departure:    "000230",
	}, events[1].StopTime)

	// Before 00:05:00, stop a has 1 Subway stopping there
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:  []string{"weekday"},
		StopID:      "a",
		ArrivalEnd:  "000500",
		RouteTypes:  []storage.RouteType{storage.RouteTypeSubway},
		DirectionID: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "r2_t1",
		StopID:       "a",
		StopSequence: 1,
		Arrival:      "000400",
		Departure:    "000430",
	}, events[0].StopTime)
}

func testStopTimeEventFilter_Service(t *testing.T, sb StorageBuilder) {
	// Three services. The single route has 2 trips for service
	// q1, 1 trip for q2, and no trips for service q3.
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday",
			"q1,20170101,20170331,1",
			"q2,20170401,20170630,1",
			"q3,20170701,20170930,1",
		},
		"stops.txt": {
			"stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,location_type",
			"a,aa,A,StopA,96,69,http://stops/a,0",
			"b,bb,B,StopB,96,69,http://stops/b,0",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"r,R2,3",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"q1_t1,r,q1,0",
			"q1_t2,r,q1,0",
			"q2_t1,r,q2,0",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,arrival_time,departure_time",
			"q1_t1,a,1,00:01:00,00:01:30",
			"q1_t1,b,2,00:02:00,00:02:30",
			"q1_t2,b,1,00:02:00,00:02:30",
			"q1_t2,a,2,00:03:00,00:03:30",
			"q2_t1,a,1,00:04:00,00:04:30",
			"q2_t1,b,2,00:05:00,00:05:30",
		},
	})

	// Querying for the q3 service will not produce any results,
	// as it has no trips.
	events, err := reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:  []string{"q3"},
		DirectionID: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(events))

	// Stops at b during the q2 service
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		StopID:      "b",
		ServiceIDs:  []string{"q2"},
		DirectionID: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "q2_t1",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "000500",
		Departure:    "000530",
	}, events[0].StopTime)

	// Stops at b during the q1 service
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		StopID:      "b",
		ServiceIDs:  []string{"q1"},
		DirectionID: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "q1_t1",
		StopID:       "b",
		StopSequence: 2,
		Arrival:      "000200",
		Departure:    "000230",
	}, events[0].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "q1_t2",
		StopID:       "b",
		StopSequence: 1,
		Arrival:      "000200",
		Departure:    "000230",
	}, events[1].StopTime)

	// Arrivals at stop a during all services, between 00:02:30
	// and 00:05:00
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		StopID:       "a",
		ServiceIDs:   []string{"q1", "q2", "q3"},
		ArrivalStart: "000230",
		ArrivalEnd:   "000500",
		DirectionID:  -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "q1_t2",
		StopID:       "a",
		StopSequence: 2,
		Arrival:      "000300",
		Departure:    "000330",
	}, events[0].StopTime)
	assert.Equal(t, &storage.StopTime{
		TripID:       "q2_t1",
		StopID:       "a",
		StopSequence: 1,
		Arrival:      "000400",
		Departure:    "000430",
	}, events[1].StopTime)
}

// Verify all data is included in returned events
func testStopTimeEvent_AllTheFields(t *testing.T, sb StorageBuilder) {
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday",
			"weekday,20170701,20170930,1",
		},
		"stops.txt": {
			"stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,location_type,parent_station,platform_code",
			"137,code137,Stop 137,Stop no 137,47.11,19.92,http://stops/137,0,,pcode137",
			"138,code138,Stop 138,Stop no 138,47.12,19.93,http://stops/138,0,139,pcode138",
			"139,code139,Station 139,Station no 139,47.13,19.94,http://stops/139,1,,pcode139",
		},
		"routes.txt": {
			"route_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color",
			"r,R,The R,Route R,3,http://routes/r,FF0000,0000FF",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id,trip_headsign,trip_short_name",
			"1,r,weekday,1,Headsign 1,trip1",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,arrival_time,departure_time,stop_headsign",
			"1,137,1,12:34:56,23:45:31,stop headsign 1",
			"1,138,2,12:34:57,23:45:32,stop headsign 2",
		},
	})

	events, err := reader.StopTimeEvents(storage.StopTimeEventFilter{
		ServiceIDs:  []string{"weekday"},
		DirectionID: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "1",
		StopID:       "137",
		StopSequence: 1,
		Arrival:      "123456",
		Departure:    "234531",
		Headsign:     "stop headsign 1",
	}, events[0].StopTime)
	assert.Equal(t, &storage.Trip{
		ID:          "1",
		RouteID:     "r",
		ServiceID:   "weekday",
		Headsign:    "Headsign 1",
		ShortName:   "trip1",
		DirectionID: 1,
	}, events[0].Trip)
	assert.Equal(t, &storage.Route{
		ID:        "r",
		ShortName: "R",
		LongName:  "The R",
		Desc:      "Route R",
		Type:      storage.RouteTypeBus,
		URL:       "http://routes/r",
		Color:     "FF0000",
		TextColor: "0000FF",
	}, events[0].Route)
	assert.Equal(t, &storage.Stop{
		ID:            "137",
		Code:          "code137",
		Name:          "Stop 137",
		Desc:          "Stop no 137",
		Lat:           47.11,
		Lon:           19.92,
		URL:           "http://stops/137",
		LocationType:  storage.LocationTypeStop,
		ParentStation: "",
		PlatformCode:  "pcode137",
	}, events[0].Stop)
	assert.Nil(t, events[0].ParentStation)
	assert.Equal(t, &storage.StopTime{
		TripID:       "1",
		StopID:       "138",
		StopSequence: 2,
		Arrival:      "123457",
		Departure:    "234532",
		Headsign:     "stop headsign 2",
	}, events[1].StopTime)
	assert.Equal(t, events[0].Trip, events[1].Trip)
	assert.Equal(t, events[0].Route, events[1].Route)
	assert.Equal(t, &storage.Stop{
		ID:            "138",
		Code:          "code138",
		Name:          "Stop 138",
		Desc:          "Stop no 138",
		Lat:           47.12,
		Lon:           19.93,
		URL:           "http://stops/138",
		LocationType:  storage.LocationTypeStop,
		ParentStation: "139",
		PlatformCode:  "pcode138",
	}, events[1].Stop)
	assert.Equal(t, &storage.Stop{
		ID:            "139",
		Code:          "code139",
		Name:          "Station 139",
		Desc:          "Station no 139",
		Lat:           47.13,
		Lon:           19.94,
		URL:           "http://stops/139",
		LocationType:  storage.LocationTypeStation,
		ParentStation: "",
		PlatformCode:  "pcode139",
	}, events[1].ParentStation)

}

func testStopTimeEvent_ParentStations(t *testing.T, sb StorageBuilder) {
	// Three services. The single route has 2 trips for service
	// q1, 1 trip for q2, and no trips for service q3.
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday",
			"weekday,20170101,20171231,1",
		},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station",
			"stop1,Stop 1,47.11,19.92,0,station_a",
			"stop2,Stop 2,47.12,19.93,0,station_a",
			"stop3,Stop 3,47.13,19.94,0,station_b",
			"station_a,Station A,47.14,19.95,1,",
			"station_b,Station B,47.15,19.96,1,",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"r,R2,3",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"t1,r,weekday,0",
			"t2,r,weekday,0",
			"t3,r,weekday,0",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,arrival_time,departure_time",
			"t1,stop1,1,01:00:00,01:01:15",
			"t1,stop2,2,01:02:00,01:03:15",
			"t2,stop1,1,02:00:00,02:01:15",
			"t2,stop2,2,02:02:00,02:03:15",
			"t3,stop1,1,03:00:00,03:01:15",
			"t3,stop2,2,03:02:00,03:03:15",
			"t3,stop3,3,03:04:00,03:05:15",
		},
	})

	// Stop 3 has 1 event. The stop has its parent station
	// included in result.
	events, err := reader.StopTimeEvents(storage.StopTimeEventFilter{
		StopID: "stop3",
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, &storage.StopTime{
		TripID:       "t3",
		StopID:       "stop3",
		StopSequence: 3,
		Arrival:      "030400",
		Departure:    "030515",
	}, events[0].StopTime)
	assert.Equal(t, &storage.Stop{
		ID:            "stop3",
		Name:          "Stop 3",
		Lat:           47.13,
		Lon:           19.94,
		LocationType:  storage.LocationTypeStop,
		ParentStation: "station_b",
	}, events[0].Stop)
	assert.Equal(t, &storage.Stop{
		ID:           "station_b",
		Name:         "Station B",
		Lat:          47.15,
		Lon:          19.96,
		LocationType: storage.LocationTypeStation,
	}, events[0].ParentStation)

	// Selecting parent station in filter produces the same result
	events2, err := reader.StopTimeEvents(storage.StopTimeEventFilter{
		StopID: "station_b",
	})
	assert.NoError(t, err)
	assert.Equal(t, events, events2)

	// Station A has two stops, so it'll yield all their stop
	// times.
	events, err = reader.StopTimeEvents(storage.StopTimeEventFilter{
		StopID: "station_a",
	})
	assert.NoError(t, err)
	assert.Equal(t, 6, len(events))

	assert.Equal(t, "stop1", events[0].StopTime.StopID)
	assert.Equal(t, "010000", events[0].StopTime.Arrival)
	assert.Equal(t, "station_a", events[0].ParentStation.ID)

	assert.Equal(t, "stop2", events[1].StopTime.StopID)
	assert.Equal(t, "010200", events[1].StopTime.Arrival)
	assert.Equal(t, "station_a", events[1].ParentStation.ID)

	assert.Equal(t, "stop1", events[2].StopTime.StopID)
	assert.Equal(t, "020000", events[2].StopTime.Arrival)
	assert.Equal(t, "station_a", events[2].ParentStation.ID)

	assert.Equal(t, "stop2", events[3].StopTime.StopID)
	assert.Equal(t, "020200", events[3].StopTime.Arrival)
	assert.Equal(t, "station_a", events[3].ParentStation.ID)

	assert.Equal(t, "stop1", events[4].StopTime.StopID)
	assert.Equal(t, "030000", events[4].StopTime.Arrival)
	assert.Equal(t, "station_a", events[4].ParentStation.ID)

	assert.Equal(t, "stop2", events[5].StopTime.StopID)
	assert.Equal(t, "030200", events[5].StopTime.Arrival)
	assert.Equal(t, "station_a", events[5].ParentStation.ID)
}

func testRouteDirections(t *testing.T, sb StorageBuilder) {
	// A bunch of trips:
	//  A1 goes alpha - beta - gamma
	//  A2 goes alpha - beta - gamma - delta - epsilon
	//  A3 goes gamma - epsilon
	//  B1 goes beta - gamma - epsilon
	//  B2 goes alpha - beta - gamma - delta - epsilon
	//
	// This is a pretty non-sensical data set, but it's enough to
	// make sure the RouteDirections are is computed correctly.
	reader := readerFromFiles(t, sb, map[string][]string{
		"calendar.txt": {"service_id,start_date,end_date", "nodays,20200101,20201231"},
		"routes.txt":   {"route_id,route_short_name,route_type", "RouteA,A,0", "RouteB,B,0"},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"alpha,alpha,1,1",
			"beta,beta,2,2",
			"gamma,gamma,3,3",
			"delta,delta,4,4",
			"epsilon,epsilon,5,5",
		},
		"trips.txt": {
			"service_id,trip_id,route_id,direction_id,trip_headsign",
			"nodays,tripA1,RouteA,0,heaven",
			"nodays,tripA2,RouteA,1,hell",
			"nodays,tripA3,RouteA,1,hell",
			"nodays,tripB1,RouteB,0,denmark",
			"nodays,tripB2,RouteB,0,whole foods",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"tripA1,alpha,1,1:2:3,1:2:3",
			"tripA1,beta,2,1:2:3,1:2:3",
			"tripA1,gamma,3,1:2:3,1:2:3",
			"tripA2,alpha,1,1:2:3,1:2:3",
			"tripA2,beta,2,1:2:3,1:2:3",
			"tripA2,gamma,3,1:2:3,1:2:3",
			"tripA2,delta,4,1:2:3,1:2:3",
			"tripA2,epsilon,5,1:2:3,1:2:3",
			"tripA3,gamma,1,1:2:3,1:2:3",
			"tripA3,epsilon,2,1:2:3,1:2:3",
			"tripB1,beta,1,1:2:3,1:2:3",
			"tripB1,gamma,2,1:2:3,1:2:3",
			"tripB1,epsilon,3,1:2:3,1:2:3",
			"tripB2,alpha,1,1:2:3,1:2:3",
			"tripB2,beta,2,1:2:3,1:2:3",
			"tripB2,gamma,3,1:2:3,1:2:3",
			"tripB2,delta,4,1:2:3,1:2:3",
			"tripB2,epsilon,5,1:2:3,1:2:3",
		},
	})

	orderPlease := func(rds []*storage.RouteDirection) []*storage.RouteDirection {
		rds = append([]*storage.RouteDirection{}, rds...)
		sort.Slice(rds, func(i, j int) bool {
			return 0 > strings.Compare(
				fmt.Sprintf(
					"%s-%s-%d",
					rds[i].StopID,
					rds[i].RouteID,
					rds[i].DirectionID,
				),
				fmt.Sprintf(
					"%s-%s-%d",
					rds[j].StopID,
					rds[j].RouteID,
					rds[j].DirectionID,
				))
		})
		for _, rd := range rds {
			sort.Strings(rd.Headsigns)
		}
		return rds
	}

	// stop alpha
	rds, err := reader.RouteDirections("alpha")
	assert.NoError(t, err)
	assert.Equal(t, []*storage.RouteDirection{
		{
			StopID:      "alpha",
			RouteID:     "RouteA",
			DirectionID: 0,
			Headsigns:   []string{"heaven"},
		},
		{
			StopID:      "alpha",
			RouteID:     "RouteA",
			DirectionID: 1,
			Headsigns:   []string{"hell"},
		},
		{
			StopID:      "alpha",
			RouteID:     "RouteB",
			DirectionID: 0,
			Headsigns:   []string{"whole foods"},
		},
	}, orderPlease(rds))

	// stop beta
	rds, err = reader.RouteDirections("beta")
	assert.NoError(t, err)
	assert.Equal(t, []*storage.RouteDirection{
		{
			StopID:      "beta",
			RouteID:     "RouteA",
			DirectionID: 0,
			Headsigns:   []string{"heaven"},
		},
		{
			StopID:      "beta",
			RouteID:     "RouteA",
			DirectionID: 1,
			Headsigns:   []string{"hell"},
		},
		{
			StopID:      "beta",
			RouteID:     "RouteB",
			DirectionID: 0,
			Headsigns:   []string{"denmark", "whole foods"},
		},
	}, orderPlease(rds))

	// stop gamma
	rds, err = reader.RouteDirections("gamma")
	assert.NoError(t, err)
	assert.Equal(t, []*storage.RouteDirection{
		{
			StopID:      "gamma",
			RouteID:     "RouteA",
			DirectionID: 1,
			Headsigns:   []string{"hell"},
		},
		{
			StopID:      "gamma",
			RouteID:     "RouteB",
			DirectionID: 0,
			Headsigns:   []string{"denmark", "whole foods"},
		},
	}, orderPlease(rds))

	// stop delta
	rds, err = reader.RouteDirections("delta")
	assert.NoError(t, err)
	assert.Equal(t, []*storage.RouteDirection{
		{
			StopID:      "delta",
			RouteID:     "RouteA",
			DirectionID: 1,
			Headsigns:   []string{"hell"},
		},
		{
			StopID:      "delta",
			RouteID:     "RouteB",
			DirectionID: 0,
			Headsigns:   []string{"whole foods"},
		},
	}, orderPlease(rds))

	// stop epsilon
	// No directions here, since nothing ever departs from epsilon.
	rds, err = reader.RouteDirections("epsilon")
	assert.NoError(t, err)
	assert.Equal(t, []*storage.RouteDirection{}, rds)
}

func testNearbyStops(t *testing.T, sb StorageBuilder) {
	reader := readerFromFiles(t, sb, map[string][]string{
		"stops.txt": {
			"stop_id,stop_lat,stop_lon,stop_name",
			"nyc,40.700000,-74.100000,nyc",
			"philly,40.000000,-75.200000,philly",
			"sf,37.800000,-122.500000,sf",
			"la,34.000000,-118.500000,la",
			"sto,59.300000,17.900000,sto",
			"lon,51.500000,-0.200000,lon",
			"rey,64.100000,-21.900000,rey",
		},
	})

	stop := func(id string, lat, lng float64) storage.Stop {
		return storage.Stop{ID: id, Name: id, Lat: lat, Lon: lng}
	}

	for _, tc := range []struct {
		Lat, Lon float64
		Limit    int
		Msg      string
		Expected []storage.Stop
	}{
		// Centered around NYC, increasing limit
		{40.0, -74.0, 1, "1 stop near nyc", []storage.Stop{stop("nyc", 40.7, -74.1)}},
		{40.0, -74.0, 2, "2 stops near nyc", []storage.Stop{stop("nyc", 40.7, -74.1), stop("philly", 40.0, -75.2)}},
		{40.0, -74.0, 5, "5 stops near nyc", []storage.Stop{
			stop("nyc", 40.7, -74.1),
			stop("philly", 40.0, -75.2),
			stop("la", 34.0, -118.5),
			stop("sf", 37.8, -122.5),
			stop("rey", 64.1, -21.9),
		}},
		{40.0, -74.0, 7, "7 stops near nyc", []storage.Stop{
			stop("nyc", 40.7, -74.1),
			stop("philly", 40.0, -75.2),
			stop("la", 34.0, -118.5),
			stop("sf", 37.8, -122.5),
			stop("rey", 64.1, -21.9),
			stop("lon", 51.5, -0.2),
			stop("sto", 59.3, 17.9),
		}},
		{40.0, -74.0, 0, "unlimited stops near nyc", []storage.Stop{
			stop("nyc", 40.7, -74.1),
			stop("philly", 40.0, -75.2),
			stop("la", 34.0, -118.5),
			stop("sf", 37.8, -122.5),
			stop("rey", 64.1, -21.9),
			stop("lon", 51.5, -0.2),
			stop("sto", 59.3, 17.9),
		}},
		{40.0, -74.0, 1000000, "100000 stops near nyc", []storage.Stop{
			stop("nyc", 40.7, -74.1),
			stop("philly", 40.0, -75.2),
			stop("la", 34.0, -118.5),
			stop("sf", 37.8, -122.5),
			stop("rey", 64.1, -21.9),
			stop("lon", 51.5, -0.2),
			stop("sto", 59.3, 17.9),
		}},

		// Centered around Reykjavik
		{64.0, -22.0, 4, "4 stops near rey", []storage.Stop{
			stop("rey", 64.1, -21.9),
			stop("lon", 51.5, -0.2),
			stop("sto", 59.3, 17.9),
			stop("nyc", 40.7, -74.1),
		}},
		{64.0, -22.0, 0, "unlimited stops near rey", []storage.Stop{
			stop("rey", 64.1, -21.9),
			stop("lon", 51.5, -0.2),
			stop("sto", 59.3, 17.9),
			stop("nyc", 40.7, -74.1),
			stop("philly", 40.0, -75.2),
			stop("sf", 37.8, -122.5),
			stop("la", 34.0, -118.5),
		}},

		// Centered around Stockholm
		{59.0, 18.0, 1, "1 stop near sto", []storage.Stop{stop("sto", 59.3, 17.9)}},
		{59.0, 18.0, 2, "2 stops near sto", []storage.Stop{stop("sto", 59.3, 17.9), stop("lon", 51.5, -0.2)}},
		{59.0, 18.0, 3, "3 stops near sto", []storage.Stop{
			stop("sto", 59.3, 17.9),
			stop("lon", 51.5, -0.2),
			stop("rey", 64.1, -21.9),
		}},
		{59.0, 18.0, 4, "4 stops near sto", []storage.Stop{
			stop("sto", 59.3, 17.9),
			stop("lon", 51.5, -0.2),
			stop("rey", 64.1, -21.9),
			stop("nyc", 40.7, -74.1),
		}},
		{59.0, 18.0, 0, "unlimited stops near sto", []storage.Stop{
			stop("sto", 59.3, 17.9),
			stop("lon", 51.5, -0.2),
			stop("rey", 64.1, -21.9),
			stop("nyc", 40.7, -74.1),
			stop("philly", 40.0, -75.2),
			stop("sf", 37.8, -122.5),
			stop("la", 34.0, -118.5),
		}},
	} {
		stops, err := reader.NearbyStops(tc.Lat, tc.Lon, tc.Limit, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.Expected, stops, tc.Msg)
	}
}

func testNearbyStopsWithParentStations(t *testing.T, sb StorageBuilder) {
	reader := readerFromFiles(t, sb, map[string][]string{
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station",
			// these are type 1 "stations"
			"p1,p1,40.0,40.0,1,",
			"p2,p2,40.5,40.5,1,",
			"p3,p3,41.0,41.0,1,",
			"p4,p4,41.5,41.5,1,",
			// these are type 0 "stops" or "platforms"
			"s5,s5,42.0,42.0,,",
			"s6,s6,42.5,42.5,,",
			// these are type 0 too, but have parent stations
			"s2a,s2a,40.1,40.1,,p1",
			"s2b,s2b,40.2,40.2,,p1",
			"s3a,s3a,41.1,41.1,,p3",
			"s3b,s3b,41.2,41.2,,p3",
			"s3c,s3c,41.3,41.3,,p3",
		},
	})

	// Queries for nearby stops should return the parent stations
	// and the parent-less stops only.
	stops, err := reader.NearbyStops(40.0, 40.0, 10, nil)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(stops))
	assert.Equal(t, "p1", stops[0].ID)
	assert.Equal(t, "p2", stops[1].ID)
	assert.Equal(t, "p3", stops[2].ID)
	assert.Equal(t, "p4", stops[3].ID)
	assert.Equal(t, "s5", stops[4].ID)
	assert.Equal(t, "s6", stops[5].ID)
}

func testFeedMetadataReadWrite(t *testing.T, sb StorageBuilder) {
	s, err := sb()
	require.NoError(t, err)

	// No feeds initially
	feeds, err := s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 0, len(feeds))

	// Write two feeds
	err = s.WriteFeedMetadata(&storage.FeedMetadata{
		SHA256:            "feed1",
		URL:               "https://gtfs/feed1",
		RetrievedAt:       time.Date(2018, 1, 2, 3, 4, 5, 0, time.UTC),
		UpdatedAt:         time.Date(2018, 7, 2, 3, 4, 5, 0, time.UTC),
		CalendarStartDate: "20190201",
		CalendarEndDate:   "20191131",
		FeedStartDate:     "20190101",
		FeedEndDate:       "20191231",
		MaxArrival:        "123456",
		MaxDeparture:      "654321",
	})
	assert.NoError(t, err)

	err = s.WriteFeedMetadata(&storage.FeedMetadata{
		SHA256:            "feed2",
		URL:               "https://gtfs/feed2",
		RetrievedAt:       time.Date(2018, 2, 3, 4, 5, 6, 0, time.UTC),
		UpdatedAt:         time.Date(2018, 8, 3, 4, 5, 6, 0, time.UTC),
		CalendarStartDate: "20190202",
		CalendarEndDate:   "20191130",
		FeedStartDate:     "20190102",
		FeedEndDate:       "20191230",
		MaxArrival:        "123457",
		MaxDeparture:      "754321",
	})
	assert.NoError(t, err)

	// Read them back
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 2, len(feeds))
	assert.Equal(t, "feed2", feeds[0].SHA256)
	assert.Equal(t, "https://gtfs/feed2", feeds[0].URL)
	assert.True(t, time.Date(2018, 2, 3, 4, 5, 6, 0, time.UTC).Equal(feeds[0].RetrievedAt))
	assert.True(t, time.Date(2018, 8, 3, 4, 5, 6, 0, time.UTC).Equal(feeds[0].UpdatedAt))
	assert.Equal(t, "20190202", feeds[0].CalendarStartDate)
	assert.Equal(t, "20191130", feeds[0].CalendarEndDate)
	assert.Equal(t, "20190102", feeds[0].FeedStartDate)
	assert.Equal(t, "20191230", feeds[0].FeedEndDate)
	assert.Equal(t, "123457", feeds[0].MaxArrival)
	assert.Equal(t, "754321", feeds[0].MaxDeparture)
	assert.Equal(t, "feed1", feeds[1].SHA256)
	assert.Equal(t, "https://gtfs/feed1", feeds[1].URL)
	assert.True(t, time.Date(2018, 1, 2, 3, 4, 5, 0, time.UTC).Equal(feeds[1].RetrievedAt))
	assert.True(t, time.Date(2018, 7, 2, 3, 4, 5, 0, time.UTC).Equal(feeds[1].UpdatedAt))
	assert.Equal(t, "20190201", feeds[1].CalendarStartDate)
	assert.Equal(t, "20191131", feeds[1].CalendarEndDate)
	assert.Equal(t, "20190101", feeds[1].FeedStartDate)
	assert.Equal(t, "20191231", feeds[1].FeedEndDate)
	assert.Equal(t, "123456", feeds[1].MaxArrival)
	assert.Equal(t, "654321", feeds[1].MaxDeparture)

	// Overwrite one of the feeds
	err = s.WriteFeedMetadata(&storage.FeedMetadata{
		SHA256:            "feed2",
		URL:               "https://gtfs/feed2",
		RetrievedAt:       time.Date(2019, 2, 3, 4, 5, 6, 0, time.UTC),
		UpdatedAt:         time.Date(2019, 8, 3, 4, 5, 6, 0, time.UTC),
		CalendarStartDate: "20200202",
		CalendarEndDate:   "20201130",
		FeedStartDate:     "20200102",
		FeedEndDate:       "20201230",
		MaxArrival:        "123458",
		MaxDeparture:      "854321",
	})
	assert.NoError(t, err)

	// And read it back
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 2, len(feeds))
	assert.Equal(t, "feed2", feeds[0].SHA256)
	assert.Equal(t, "https://gtfs/feed2", feeds[0].URL)
	assert.True(t, time.Date(2019, 2, 3, 4, 5, 6, 0, time.UTC).Equal(feeds[0].RetrievedAt))
	assert.True(t, time.Date(2019, 8, 3, 4, 5, 6, 0, time.UTC).Equal(feeds[0].UpdatedAt))
	assert.Equal(t, "20200202", feeds[0].CalendarStartDate)
	assert.Equal(t, "20201130", feeds[0].CalendarEndDate)
	assert.Equal(t, "20200102", feeds[0].FeedStartDate)
	assert.Equal(t, "20201230", feeds[0].FeedEndDate)
	assert.Equal(t, "123458", feeds[0].MaxArrival)
	assert.Equal(t, "854321", feeds[0].MaxDeparture)
}

// FeedMetadata in storage is keyed on URL and SHA256
func testFeedMetadataFiltering(t *testing.T, sb StorageBuilder) {
	s, err := sb()
	require.NoError(t, err)

	// Write some feeds
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed1",
		SHA256: "deadbeef",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed2",
		SHA256: "cafed00d",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed3",
		SHA256: "1337ca7",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed4",
		SHA256: "deadbeef", // same as feed 1
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed4", // second occurrence of feed 4
		SHA256: "feedface",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed5",
		SHA256: "", //blank
	}))

	// Read them all back
	feeds, err := s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 6, len(feeds))
	sort.Slice(feeds, func(i, j int) bool {
		if feeds[i].URL != feeds[j].URL {
			return feeds[i].URL < feeds[j].URL
		}
		return feeds[i].SHA256 < feeds[j].SHA256
	})
	assert.Equal(t, "https://gtfs/feed1", feeds[0].URL)
	assert.Equal(t, "deadbeef", feeds[0].SHA256)
	assert.Equal(t, "https://gtfs/feed2", feeds[1].URL)
	assert.Equal(t, "cafed00d", feeds[1].SHA256)
	assert.Equal(t, "https://gtfs/feed3", feeds[2].URL)
	assert.Equal(t, "1337ca7", feeds[2].SHA256)
	assert.Equal(t, "https://gtfs/feed4", feeds[3].URL)
	assert.Equal(t, "deadbeef", feeds[3].SHA256)
	assert.Equal(t, "https://gtfs/feed4", feeds[4].URL)
	assert.Equal(t, "feedface", feeds[4].SHA256)
	assert.Equal(t, "https://gtfs/feed5", feeds[5].URL)
	assert.Equal(t, "", feeds[5].SHA256)

	// Filter by URL
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{
		URL: "https://gtfs/feed1",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))
	assert.Equal(t, "https://gtfs/feed1", feeds[0].URL)
	assert.Equal(t, "deadbeef", feeds[0].SHA256)

	feeds, err = s.ListFeeds(storage.ListFeedsFilter{
		URL: "https://gtfs/feed5",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))
	assert.Equal(t, "https://gtfs/feed5", feeds[0].URL)
	assert.Equal(t, "", feeds[0].SHA256)

	// Filter by SHA256
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{
		SHA256: "deadbeef",
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(feeds))
	sort.Slice(feeds, func(i, j int) bool {
		return feeds[i].URL < feeds[j].URL
	})
	assert.Equal(t, "https://gtfs/feed1", feeds[0].URL)
	assert.Equal(t, "deadbeef", feeds[0].SHA256)
	assert.Equal(t, "https://gtfs/feed4", feeds[1].URL)
	assert.Equal(t, "deadbeef", feeds[1].SHA256)
}

func testFeedMetadataDeletion(t *testing.T, sb StorageBuilder) {
	s, err := sb()
	require.NoError(t, err)

	// Write some feeds
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed1",
		SHA256: "deadbeef",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed2",
		SHA256: "cafed00d",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed3",
		SHA256: "1337ca7",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed4",
		SHA256: "deadbeef", // same as feed 1
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed4", // second occurrence of feed 4
		SHA256: "feedface",
	}))
	require.NoError(t, s.WriteFeedMetadata(&storage.FeedMetadata{
		URL:    "https://gtfs/feed5",
		SHA256: "", //blank
	}))

	// Delete some feeds
	assert.NoError(t, s.DeleteFeedMetadata("https://gtfs/feed1", "deadbeef"))
	assert.NoError(t, s.DeleteFeedMetadata("https://gtfs/feed4", "feedface"))
	assert.NoError(t, s.DeleteFeedMetadata("https://gtfs/feed5", ""))

	// They're now gone
	feeds, err := s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 3, len(feeds))
	sort.Slice(feeds, func(i, j int) bool {
		return feeds[i].URL < feeds[j].URL
	})
	assert.Equal(t, "https://gtfs/feed2", feeds[0].URL)
	assert.Equal(t, "cafed00d", feeds[0].SHA256)
	assert.Equal(t, "https://gtfs/feed3", feeds[1].URL)
	assert.Equal(t, "1337ca7", feeds[1].SHA256)
	assert.Equal(t, "https://gtfs/feed4", feeds[2].URL)
	assert.Equal(t, "deadbeef", feeds[2].SHA256)

}

func testFeedOverwrite(t *testing.T, sb StorageBuilder) {
	// Two completely different feeds
	feed1 := map[string][]string{
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station",
			"s1,S1,40.0,40.0,1,",
		},
		"routes.txt": {
			"route_id,route_short_name,route_long_name,route_type",
			"r1,R1,one,1",
		},
		"trips.txt": {
			"route_id,service_id,trip_id,trip_headsign,direction_id,shape_id",
			"r1,mondays,t1,one,0,",
		},
		"stop_times.txt": {
			"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
			"t1,00:00:00,00:00:00,s1,1",
		},
		"calendar.txt": {
			"service_id,monday,start_date,end_date",
			"mondays,1,20190101,20191231",
		},
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"mondays,20190101,2",
		},
	}

	feed2 := map[string][]string{
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station",
			"s2,S2,40.0,40.0,1,",
		},
		"routes.txt": {
			"route_id,route_short_name,route_long_name,route_type",
			"r2,R2,two,2",
		},
		"trips.txt": {
			"route_id,service_id,trip_id,trip_headsign,direction_id,shape_id",
			"r2,mondays,t2,two,0,",
		},
		"stop_times.txt": {
			"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
			"t2,00:00:00,00:00:00,s2,1",
		},
		"calendar.txt": {
			"service_id,monday,start_date,end_date",
			"mondays,1,20190101,20190202",
		},
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"mondays,20190102,2",
		},
	}

	// Use a single storage object to make sure it gets reused.
	s, err := sb()
	require.NoError(t, err)
	reuse := func() (storage.Storage, error) {
		return s, nil
	}

	// Write the first feed
	reader := readerFromFiles(t, reuse, feed1)

	// Reader provides the expected values
	stops, err := reader.Stops()
	require.NoError(t, err)
	assert.Equal(t, 1, len(stops))
	assert.Equal(t, "s1", stops[0].ID)
	routes, err := reader.Routes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(routes))
	assert.Equal(t, "r1", routes[0].ID)
	trips, err := reader.Trips()
	require.NoError(t, err)
	assert.Equal(t, 1, len(trips))
	assert.Equal(t, "t1", trips[0].ID)
	stopTimes, err := reader.StopTimes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(stopTimes))
	assert.Equal(t, "t1", stopTimes[0].TripID)
	assert.Equal(t, "s1", stopTimes[0].StopID)
	calendar, err := reader.Calendars()
	require.NoError(t, err)
	assert.Equal(t, 1, len(calendar))
	assert.Equal(t, "mondays", calendar[0].ServiceID)
	assert.Equal(t, "20191231", calendar[0].EndDate)
	calendarDates, err := reader.CalendarDates()
	require.NoError(t, err)
	assert.Equal(t, 1, len(calendarDates))
	assert.Equal(t, "mondays", calendarDates[0].ServiceID)
	assert.Equal(t, "20190101", calendarDates[0].Date)

	// Write the second feed
	reader = readerFromFiles(t, reuse, feed2)

	// All values are now updated
	stops, err = reader.Stops()
	require.NoError(t, err)
	assert.Equal(t, 1, len(stops))
	assert.Equal(t, "s2", stops[0].ID)
	routes, err = reader.Routes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(routes))
	assert.Equal(t, "r2", routes[0].ID)
	trips, err = reader.Trips()
	require.NoError(t, err)
	assert.Equal(t, 1, len(trips))
	assert.Equal(t, "t2", trips[0].ID)
	stopTimes, err = reader.StopTimes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(stopTimes))
	assert.Equal(t, "t2", stopTimes[0].TripID)
	assert.Equal(t, "s2", stopTimes[0].StopID)
	calendar, err = reader.Calendars()
	require.NoError(t, err)
	assert.Equal(t, 1, len(calendar))
	assert.Equal(t, "mondays", calendar[0].ServiceID)
	assert.Equal(t, "20190202", calendar[0].EndDate)
	calendarDates, err = reader.CalendarDates()
	require.NoError(t, err)
	assert.Equal(t, 1, len(calendarDates))
	assert.Equal(t, "mondays", calendarDates[0].ServiceID)
	assert.Equal(t, "20190102", calendarDates[0].Date)
}

func testFeedRequest(t *testing.T, sb StorageBuilder) {
	s, err := sb()
	require.NoError(t, err)

	// No requests at first
	requests, err := s.ListFeedRequests("")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(requests))
	requests, err = s.ListFeedRequests("a-not-yet-added-url")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(requests))

	// Add 1 request for google, and 3 for microsoft
	assert.NoError(t, s.WriteFeedRequest(storage.FeedRequest{
		URL:       "https://google.com",
		Consumer:  "mario",
		Headers:   "mario-headers",
		CreatedAt: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
	}))
	assert.NoError(t, s.WriteFeedRequest(storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "luigi",
		Headers:   "luigi-headers",
		CreatedAt: time.Date(2019, 1, 3, 0, 0, 0, 0, time.UTC),
	}))
	assert.NoError(t, s.WriteFeedRequest(storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "peach",
		Headers:   "peach-headers",
		CreatedAt: time.Date(2019, 1, 5, 0, 0, 0, 0, time.UTC),
	}))
	assert.NoError(t, s.WriteFeedRequest(storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "bowser",
		Headers:   "bowser-headers",
		CreatedAt: time.Date(2019, 1, 7, 0, 0, 0, 0, time.UTC),
	}))

	// All requests are added
	requests, err = s.ListFeedRequests("")
	assert.NoError(t, err)
	assert.Equal(t, 4, len(requests))
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].Consumer < requests[j].Consumer
	})
	assert.Equal(t, storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "bowser",
		Headers:   "bowser-headers",
		CreatedAt: time.Date(2019, 1, 7, 0, 0, 0, 0, time.UTC),
	}, requests[0])
	assert.Equal(t, storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "luigi",
		Headers:   "luigi-headers",
		CreatedAt: time.Date(2019, 1, 3, 0, 0, 0, 0, time.UTC),
	}, requests[1])

	// And can be requested by URL
	requests, err = s.ListFeedRequests("https://google.com")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(requests))
	assert.Equal(t, storage.FeedRequest{
		URL:       "https://google.com",
		Consumer:  "mario",
		Headers:   "mario-headers",
		CreatedAt: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
	}, requests[0])

	requests, err = s.ListFeedRequests("https://microsoft.com")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(requests))
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].Consumer < requests[j].Consumer
	})
	assert.Equal(t, storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "bowser",
		Headers:   "bowser-headers",
		CreatedAt: time.Date(2019, 1, 7, 0, 0, 0, 0, time.UTC),
	}, requests[0])
	assert.Equal(t, storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "luigi",
		Headers:   "luigi-headers",
		CreatedAt: time.Date(2019, 1, 3, 0, 0, 0, 0, time.UTC),
	}, requests[1])
	assert.Equal(t, storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "peach",
		Headers:   "peach-headers",
		CreatedAt: time.Date(2019, 1, 5, 0, 0, 0, 0, time.UTC),
	}, requests[2])

	// Overwriting a request (same URL and consumer)
	assert.NoError(t, s.WriteFeedRequest(storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "luigi",
		Headers:   "luigi-headers-updated",
		CreatedAt: time.Date(2019, 1, 3, 0, 0, 0, 0, time.UTC),
	}))
	requests, err = s.ListFeedRequests("https://microsoft.com")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(requests))
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].Consumer < requests[j].Consumer
	})
	assert.Equal(t, storage.FeedRequest{
		URL:       "https://microsoft.com",
		Consumer:  "luigi",
		Headers:   "luigi-headers-updated",
		CreatedAt: time.Date(2019, 1, 3, 0, 0, 0, 0, time.UTC),
	}, requests[1])
}

func TestStorage(t *testing.T) {
	for _, test := range []struct {
		Name string
		Test func(t *testing.T, sb StorageBuilder)
	}{
		{"InitiallyEmpty", testInitiallyEmpty},
		{"BasicReadingAndWriting", testBasicReadingAndWriting},
		{"ActiveServicesCalendarOnly", testActiveServicesCalendarOnly},
		{"ActiveServicesServiceAdded", testActiveServicesServiceAdded},
		{"ActiveServicesServiceRemoved", testActiveServicesServiceRemoved},
		{"ActiveServicesServiceAddedOutsideDateRange", testActiveServicesServiceAddedOutsideDateRange},
		{"ActiveServicesCalendarDatesOnly", testActiveServicesCalendarDatesOnly},
		{"ActiveServicesCalendarDatesOnlyRemoved", testActiveServicesCalendarDatesOnlyRemoved},
		{"ActiveServicesLotsOfRecords", testActiveServicesLotsOfRecords},
		{"ActiveServicesNoCalendar", testActiveServicesNoCalendar},
		{"StopTimeEventFilter_TimeStopdirection", testStopTimeEventFilter_TimeStopdirection},
		{"StopTimeEventFilter_RouteAndRouteType", testStopTimeEventFilter_RouteAndRouteType},
		{"StopTimeEventFilter_Service", testStopTimeEventFilter_Service},
		{"StopTimeEvent_AllTheFields", testStopTimeEvent_AllTheFields},
		{"StopTimeEvent_ParentStations", testStopTimeEvent_ParentStations},
		{"RouteDirections", testRouteDirections},
		{"NearbyStops", testNearbyStops},
		{"NearbyStopsWithParentStations", testNearbyStopsWithParentStations},
		{"FeedMetadataReadWrite", testFeedMetadataReadWrite},
		{"FeedMetadataFiltering", testFeedMetadataFiltering},
		{"FeedMetadataDeletion", testFeedMetadataDeletion},
		{"FeedOverwrite", testFeedOverwrite},
		{"FeedRequest", testFeedRequest},
	} {
		t.Run(fmt.Sprintf("%s memory", test.Name), func(t *testing.T) {
			test.Test(t, func() (storage.Storage, error) {
				return storage.NewMemoryStorage(), nil
			})
		})
		t.Run(fmt.Sprintf("%s SQLiteMemory", test.Name), func(t *testing.T) {
			test.Test(t, func() (storage.Storage, error) {
				return storage.NewSQLiteStorage()
			})
		})
		t.Run(fmt.Sprintf("%s SQLiteFile", test.Name), func(t *testing.T) {
			dir, err := ioutil.TempDir("", "gtfs_storage_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			test.Test(t, func() (storage.Storage, error) {
				return storage.NewSQLiteStorage(storage.SQLiteConfig{OnDisk: true, Directory: dir})
			})
		})
		if PostgresConnStr != "" {
			t.Run(fmt.Sprintf("%s Postgres", test.Name), func(t *testing.T) {
				test.Test(t, func() (storage.Storage, error) {
					return storage.NewPSQLStorage(PostgresConnStr, true)
				})
			})
		}
	}
}
