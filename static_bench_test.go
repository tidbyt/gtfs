package gtfs_test

import (
	"testing"
	"time"

	"tidbyt.dev/gtfs/testutil"
)

func benchNearbyStops(b *testing.B, backend string) {
	static := testutil.LoadStaticFile(b, backend, "testdata/caltrain_20160406.zip")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// The 20 nearest stops for 544 Park Ave, BK
		_, err := static.NearbyStops(40.6968986, -73.955555, 20, nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func benchDepartures(b *testing.B, backend string) {
	static := testutil.LoadStaticFile(b, backend, "testdata/caltrain_20160406.zip")

	tz, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		b.Error(err)
	}

	when := time.Date(2020, 2, 3, 8, 0, 0, 0, tz)
	window := 1 * time.Hour

	stops, err := static.NearbyStops(40.734673, -73.989951, 0, nil)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stopID := stops[i%len(stops)].ID
		// All departures from (one of) the Union Square
		// stop(s) between 8AM and 9AM on a weekday
		_, err := static.Departures(stopID, when, window, -1, "", -1, nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkGTFSStatic(b *testing.B) {
	for _, test := range []struct {
		Name  string
		Bench func(b *testing.B, storage string)
	}{
		{"NearbyStops", benchNearbyStops},
		{"Departures", benchDepartures},
	} {
		b.Run(test.Name+"_sqlite", func(b *testing.B) {
			test.Bench(b, "sqlite")
		})
		if testutil.PostgresConnStr != "" {
			b.Run(test.Name+"_postgres", func(b *testing.B) {
				test.Bench(b, "postgres")
			})
		}
	}
}
