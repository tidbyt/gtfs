package parse

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/storage"
)

func buildZip(t *testing.T, files map[string][]string) []byte {
	buf := &bytes.Buffer{}
	w := zip.NewWriter(buf)
	for filename, content := range files {
		f, err := w.Create(filename)
		require.NoError(t, err)
		_, err = f.Write([]byte(strings.Join(content, "\n")))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	return buf.Bytes()
}

// A simple GTFS feed with all required data
func fixtureSimple() map[string][]string {
	return map[string][]string{
		"agency.txt": []string{
			"agency_timezone,agency_name,agency_url",
			"America/Los_Angeles,Fake Agency,http://agency/index.html",
		},
		"routes.txt": []string{
			"route_id,route_short_name,route_type",
			"r,R,3",
		},
		"calendar.txt": []string{
			"service_id,monday,start_date,end_date",
			"mondays,1,20190101,20190301",
		},
		"calendar_dates.txt": []string{
			"service_id,date,exception_type",
			"mondays,20190302,1",
		},
		"trips.txt": []string{
			"route_id,service_id,trip_id",
			"r,mondays,t",
		},
		"stops.txt": []string{
			"stop_id,stop_name,stop_lat,stop_lon",
			"s,S,12,34",
		},
		"stop_times.txt": []string{
			"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
			"t,12:00:00,12:00:00,s,1",
		},
	}
}

func TestParseValidFeed(t *testing.T) {
	s, err := storage.NewSQLiteStorage()
	require.NoError(t, err)
	writer, err := s.GetWriter("test")
	require.NoError(t, err)

	metadata, err := ParseStatic(writer, buildZip(t, fixtureSimple()))
	assert.NoError(t, err)
	assert.Equal(t, "America/Los_Angeles", metadata.Timezone)
	assert.Equal(t, "20190101", metadata.CalendarStartDate)
	assert.Equal(t, "20190302", metadata.CalendarEndDate)
	assert.Equal(t, "120000", metadata.MaxArrival)
	assert.Equal(t, "120000", metadata.MaxDeparture)

	reader, err := s.GetReader("test")
	require.NoError(t, err)

	agencies, err := reader.Agencies()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.Agency{&storage.Agency{
		Timezone: "America/Los_Angeles",
		Name:     "Fake Agency",
		URL:      "http://agency/index.html",
	}}, agencies)

	routes, err := reader.Routes()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.Route{&storage.Route{
		ID:        "r",
		ShortName: "R",
		Type:      3,
		Color:     "FFFFFF",
		TextColor: "000000",
	}}, routes)

	calendar, err := reader.Calendars()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.Calendar{&storage.Calendar{
		ServiceID: "mondays",
		Weekday:   1 << time.Monday,
		StartDate: "20190101",
		EndDate:   "20190301",
	}}, calendar)

	calendarDates, err := reader.CalendarDates()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.CalendarDate{&storage.CalendarDate{
		ServiceID:     "mondays",
		Date:          "20190302",
		ExceptionType: 1,
	}}, calendarDates)

	trips, err := reader.Trips()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.Trip{&storage.Trip{
		ID:        "t",
		RouteID:   "r",
		ServiceID: "mondays",
	}}, trips)

	stops, err := reader.Stops()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.Stop{&storage.Stop{
		ID:   "s",
		Name: "S",
		Lat:  12,
		Lon:  34,
	}}, stops)

	stopTimes, err := reader.StopTimes()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.StopTime{&storage.StopTime{
		TripID:       "t",
		Arrival:      "120000",
		Departure:    "120000",
		StopID:       "s",
		StopSequence: 1,
	}}, stopTimes)
}

func TestParseMissingRequiredFile(t *testing.T) {

	for _, file := range []string{
		"agency.txt",
		"routes.txt",
		"trips.txt",
		"stops.txt",
		"stop_times.txt",
	} {
		s, err := storage.NewSQLiteStorage()
		require.NoError(t, err)
		writer, err := s.GetWriter("test")
		require.NoError(t, err)

		files := fixtureSimple()
		delete(files, file)
		_, err = ParseStatic(writer, buildZip(t, files))
		assert.Error(t, err, "missing "+file)
	}

	// Ok for calendar.txt to be missing
	s, err := storage.NewSQLiteStorage()
	require.NoError(t, err)
	writer, err := s.GetWriter("test")
	require.NoError(t, err)
	files := fixtureSimple()
	delete(files, "calendar.txt")
	metadata, err := ParseStatic(writer, buildZip(t, files))
	assert.NoError(t, err)
	assert.Equal(t, metadata.CalendarStartDate, "20190302")
	assert.Equal(t, metadata.CalendarEndDate, "20190302")
	assert.Equal(t, metadata.MaxArrival, "120000")
	assert.Equal(t, metadata.MaxDeparture, "120000")

	// Ok for calendar_dates.txt to be missing
	s, err = storage.NewSQLiteStorage()
	require.NoError(t, err)
	writer, err = s.GetWriter("test")
	require.NoError(t, err)
	files = fixtureSimple()
	delete(files, "calendar_dates.txt")
	metadata, err = ParseStatic(writer, buildZip(t, files))
	assert.NoError(t, err)
	assert.Equal(t, metadata.CalendarStartDate, "20190101")
	assert.Equal(t, metadata.CalendarEndDate, "20190301")
	assert.Equal(t, metadata.MaxArrival, "120000")
	assert.Equal(t, metadata.MaxDeparture, "120000")

	// But not OK for both to be missing
	s, err = storage.NewSQLiteStorage()
	require.NoError(t, err)
	writer, err = s.GetWriter("test")
	require.NoError(t, err)
	files = fixtureSimple()
	delete(files, "calendar.txt")
	delete(files, "calendar_dates.txt")
	_, err = ParseStatic(writer, buildZip(t, files))
	assert.Error(t, err)
}

func TestParseBrokenFile(t *testing.T) {
	// Individual files in the feed broken.
	for _, file := range []string{
		"agency.txt",
		"routes.txt",
		"calendar.txt",
		"calendar_dates.txt",
		"trips.txt",
		"stops.txt",
		"stop_times.txt",
	} {
		s, err := storage.NewSQLiteStorage()
		require.NoError(t, err)
		writer, err := s.GetWriter("test")
		require.NoError(t, err)

		files := fixtureSimple()
		files[file][1] = "malformed"

		_, err = ParseStatic(writer, buildZip(t, files))
		assert.Error(t, err, "malformed "+file)
	}

	// Zip file broken.
	s, err := storage.NewSQLiteStorage()
	require.NoError(t, err)
	writer, err := s.GetWriter("test")
	require.NoError(t, err)

	_, err = ParseStatic(writer, []byte("malformed"))
	assert.Error(t, err, "malformed zip file")
}

// Some agencies place files in subdirectories. They shouldn't, but
// they do. Make sure we can handle that.
func TestParseUnorthodoxArchiveStructure(t *testing.T) {
	goodFiles := fixtureSimple()
	badFiles := map[string][]string{}
	for name, contents := range goodFiles {
		badFiles["bad/agency/"+name] = contents
	}
	sillyZip := buildZip(t, badFiles)

	s, err := storage.NewSQLiteStorage()
	require.NoError(t, err)
	writer, err := s.GetWriter("test")
	require.NoError(t, err)

	metadata, err := ParseStatic(writer, sillyZip)
	assert.NoError(t, err)
	assert.Equal(t, "America/Los_Angeles", metadata.Timezone)
	assert.Equal(t, "20190101", metadata.CalendarStartDate)
	assert.Equal(t, "20190302", metadata.CalendarEndDate)
	assert.Equal(t, "120000", metadata.MaxArrival)
	assert.Equal(t, "120000", metadata.MaxDeparture)

	reader, err := s.GetReader("test")
	require.NoError(t, err)

	agency, err := reader.Agencies()
	assert.NoError(t, err)
	assert.Equal(t, []*storage.Agency{&storage.Agency{
		Timezone: "America/Los_Angeles",
		Name:     "Fake Agency",
		URL:      "http://agency/index.html",
	}}, agency)
}
