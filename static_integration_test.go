package gtfs

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/parse"
	"tidbyt.dev/gtfs/storage"
)

func loadFeed2(t *testing.T, backend string, filename string) *Static {
	var s storage.Storage
	var err error
	if backend == "memory" {
		s = storage.NewMemoryStorage()
	} else if backend == "sqlite" {
		s, err = storage.NewSQLiteStorage()
		require.NoError(t, err)
	} else if backend == "postgres" {
		s, err = storage.NewPSQLStorage(storage.PSQLConfig{
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "mysecretpassword",
			DBName:   "gtfs",
			ClearDB:  true,
		})
		require.NoError(t, err)
	} else {
		t.Fatalf("unknown backend %q", backend)
	}

	content, err := ioutil.ReadFile(filename)
	require.NoError(t, err)

	writer, err := s.GetWriter("benchmarking")
	require.NoError(t, err)
	metadata, err := parse.ParseStatic(writer, content)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := s.GetReader("benchmarking")
	require.NoError(t, err)

	static, err := NewStatic(reader, metadata)
	require.NoError(t, err)

	return static
}

func testGTFSStaticIntegrationNearbyStops(t *testing.T, backend string) {
	if testing.Short() {
		t.Skip("loading MTA dump is slow")
	}

	// This is a giant GTFS file from the MTA
	g := loadFeed2(t, backend, "testdata/mta_static.zip")

	// The 4 nearest stops for 544 Park Ave, BK. There are other
	// stops with the same coordinates, but they all have
	// location_type==0 and reference one of these 4 as parent
	// station.
	stops, err := g.NearbyStops(40.6968986, -73.955555, 4, nil)
	assert.NoError(t, err)
	stopMap := make(map[string]storage.Stop)
	for _, s := range stops {
		stopMap[s.ID] = s
	}
	assert.Equal(t, map[string]storage.Stop{
		"G31": {
			ID:            "G31",
			Lat:           40.700377,
			Lon:           -73.950234,
			Name:          "Flushing Av",
			LocationType:  1,
			ParentStation: "",
		},
		"G32": {
			ID:            "G32",
			Lat:           40.694568,
			Lon:           -73.949046,
			Name:          "Myrtle - Willoughby Avs",
			LocationType:  1,
			ParentStation: "",
		},
		"G33": {
			ID:            "G33",
			Lat:           40.689627,
			Lon:           -73.953522,
			Name:          "Bedford - Nostrand Avs",
			LocationType:  1,
			ParentStation: "",
		},
		"G34": {
			ID:            "G34",
			Name:          "Classon Av",
			Lat:           40.688873,
			Lon:           -73.96007,
			LocationType:  1,
			ParentStation: "",
		},
	}, stopMap)
}

func testGTFSStaticIntegrationDepartures(t *testing.T, backend string) {
	if testing.Short() {
		t.Skip("loading MTA dump is slow")
	}

	// This is a giant GTFS file from the MTA
	g := loadFeed2(t, backend, "testdata/mta_static.zip")

	// Let's look at the G33S stop, also known as "Bedford -
	// Nostrand Avs". Between 22:50 and 23:10 there are are 6
	// stop_time records:
	//
	//   trip_id,arrival_time,departure_time,stop_id,stop_sequence
	//   BFA19GEN-G035-Saturday-00_136750_G..S16R,23:02:00,23:02:30,G33S,9
	//   BFA19GEN-G035-Saturday-00_135750_G..S16R,22:52:00,22:52:00,G33S,9
	//   BFA19GEN-G036-Sunday-00_136550_G..S16R,23:00:00,23:00:00,G33S,9
	//   BFA19GEN-G051-Weekday-00_135800_G..S14R,22:50:30,22:50:30,G33S,9
	//   BFA19GEN-G051-Weekday-00_136700_G..S14R,22:59:30,22:59:30,G33S,9
	//   BFA19GEN-G051-Weekday-00_137600_G..S14R,23:08:30,23:08:30,G33S,9
	//
	// The first two share a prefix in their trip_id, as do the
	// last final three. Inspecting the corresponding trip records
	// reveals that these share service_id. These three have
	// calendar records as follows:
	//
	//   service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
	//   BFA19GEN-G035-Saturday-00,0,0,0,0,0,1,0,20200104,20200502
	//   BFA19GEN-G036-Sunday-00,0,0,0,0,0,0,1,20200105,20200426
	//   BFA19GEN-G051-Weekday-00,1,1,1,1,1,0,0,20200102,20200501
	//
	// So during most of Jan - Apr, these stop_times should
	// apply. However, there are also calendar_date records for
	// two of these services:
	//
	//   BFA19GEN-G035-Saturday-00,20200217,1
	//   BFA19GEN-G051-Weekday-00,20200217,2
	//
	// Together, these remove the Weekday schedule and add the
	// Saturday schedule for February 17th (President's Day).
	//
	// The 6 trips all share a single headsign: "Church Av".
	//
	// Armed with this knowledge, we can now run some test
	// queries.

	// Feb 3rd is a Monday
	departures, _ := g.Departures("G33S", time.Date(2020, 2, 3, 22, 50, 0, 0, g.location), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, []Departure{
		{
			StopID:       "G33S",
			RouteID:      "G",
			TripID:       "BFA19GEN-G051-Weekday-00_135800_G..S14R",
			StopSequence: 9,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 3, 22, 50, 30, 0, g.location),
			Headsign:     "Church Av",
		},
		{
			StopID:       "G33S",
			RouteID:      "G",
			TripID:       "BFA19GEN-G051-Weekday-00_136700_G..S14R",
			StopSequence: 9,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 3, 22, 59, 30, 0, g.location),
			Headsign:     "Church Av",
		},
	}, departures)

	// Feb 17 is also a Monday, but President's Day
	departures, _ = g.Departures("G33S", time.Date(2020, 2, 17, 22, 50, 0, 0, g.location), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, []Departure{
		{
			StopID:       "G33S",
			RouteID:      "G",
			TripID:       "BFA19GEN-G035-Saturday-00_135750_G..S16R",
			StopSequence: 9,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 17, 22, 52, 0, 0, g.location),
			Headsign:     "Church Av",
		},
	}, departures)

	// So to get 2 stops w need a larger window. These appear in
	// reverse order in stop_times.txt, but will be still be
	// returned ordered by departure time.
	departures, _ = g.Departures("G33S", time.Date(2020, 2, 17, 22, 50, 0, 0, g.location), 13*time.Minute, -1, "", -1, nil)
	assert.Equal(t, []Departure{
		{
			StopID:       "G33S",
			RouteID:      "G",
			TripID:       "BFA19GEN-G035-Saturday-00_135750_G..S16R",
			StopSequence: 9,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 17, 22, 52, 0, 0, g.location),
			Headsign:     "Church Av",
		},
		{
			StopID:       "G33S",
			RouteID:      "G",
			TripID:       "BFA19GEN-G035-Saturday-00_136750_G..S16R",
			StopSequence: 9,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 17, 23, 2, 30, 0, g.location),
			Headsign:     "Church Av",
		},
	}, departures)

	// Feb 16 is a Sunday
	departures, _ = g.Departures("G33S", time.Date(2020, 2, 16, 22, 50, 0, 0, g.location), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, []Departure{
		{
			StopID:       "G33S",
			RouteID:      "G",
			TripID:       "BFA19GEN-G036-Sunday-00_136550_G..S16R",
			StopSequence: 9,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 16, 23, 0, 0, 0, g.location),
			Headsign:     "Church Av",
		},
	}, departures)
}

func TestStaticIntegration(t *testing.T) {
	for _, test := range []struct {
		Name string
		Test func(*testing.T, string)
	}{
		{"GTFSStaticIntegrationNearbyStops", testGTFSStaticIntegrationNearbyStops},
		{"GTFSStaticIntegrationDepartures", testGTFSStaticIntegrationDepartures},
	} {
		t.Run(test.Name+"_SQLite", func(t *testing.T) {
			test.Test(t, "sqlite")
		})
		t.Run(test.Name+"_memory", func(t *testing.T) {
			test.Test(t, "memory")
		})
		t.Run(test.Name+"_postgres", func(t *testing.T) {
			test.Test(t, "postgres")
		})
	}
}
