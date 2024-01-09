package gtfs_test

import (
	"context"
	"os"
	"testing"
	"time"

	"tidbyt.dev/gtfs"
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

func benchRouteDirections(b *testing.B, backend string) {
	static := testutil.LoadStaticFile(b, backend, "testdata/caltrain_20160406.zip")

	stops, err := static.NearbyStops(40.734673, -73.989951, 0, nil)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := static.RouteDirections(stops[i%len(stops)].ID)
		if err != nil {
			b.Error(err)
		}
	}
}

func benchStaticDepartures(b *testing.B, backend string) {
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
		_, err := static.Departures(stopID, when, window, -1, "", -1, nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func benchRealtimeLoad(b *testing.B, backend string) {
	static := testutil.LoadStaticFile(b, backend, "testdata/bart_static.zip")

	buf, err := os.ReadFile("testdata/bart_20240104T142509.pb")
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := gtfs.NewRealtime(context.Background(), static, [][]byte{buf})
		if err != nil {
			b.Error(err)
		}
	}
}

func benchRealtimeDepartures(b *testing.B, backend string) {
	static := testutil.LoadStaticFile(b, backend, "testdata/bart_static.zip")

	buf, err := os.ReadFile("testdata/bart_20240104T142509.pb")
	if err != nil {
		b.Error(err)
	}
	rt, err := gtfs.NewRealtime(context.Background(), static, [][]byte{buf})
	if err != nil {
		b.Error(err)
	}

	stops, err := static.NearbyStops(40.734673, -73.989951, 0, nil)
	if err != nil {
		b.Error(err)
	}

	tzSF, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		b.Error(err)
	}

	when := time.Date(2024, 1, 4, 11, 26, 42, 0, tzSF)
	window := 30 * time.Minute

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := rt.Departures(stops[i%len(stops)].ID, when, window, -1, "", -1, nil)
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
		{"RouteDirections", benchRouteDirections},
		{"StaticDepartures", benchStaticDepartures},
		{"RealtimeLoad", benchRealtimeLoad},
		{"RealtimeDepartures", benchRealtimeDepartures},
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
