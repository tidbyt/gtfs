package gtfs_test

import (
	"io/ioutil"
	"testing"
	"time"

	"tidbyt.dev/gtfs"
	"tidbyt.dev/gtfs/parse"
	"tidbyt.dev/gtfs/storage"
)

func loadFeed(b *testing.B, backend string, filename string) *gtfs.Static {
	var s storage.Storage
	var err error
	if backend == "memory" {
		s = storage.NewMemoryStorage()
	} else if backend == "sqlite" {
		s, err = storage.NewSQLiteStorage()
		if err != nil {
			b.Error(err)
		}
	} else if backend == "postgres" {
		s, err = storage.NewPSQLStorage(PostgresConnStr, true)
		if err != nil {
			b.Error(err)
		}
	} else {
		b.Error("unknown backend")
	}

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		b.Error(err)
	}

	writer, err := s.GetWriter("benchmarking")
	if err != nil {
		b.Error(err)
	}
	metadata, err := parse.ParseStatic(writer, content)
	if err != nil {
		b.Error(err)
	}
	err = writer.Close()
	if err != nil {
		b.Error(err)
	}

	reader, err := s.GetReader("benchmarking")
	if err != nil {
		b.Error(err)
	}

	static, err := gtfs.NewStatic(reader, metadata)
	if err != nil {
		b.Error(err)
	}

	return static
}

func benchNearbyStops(b *testing.B, backend string) {
	static := loadFeed(b, backend, "testdata/caltrain_20160406.zip")

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
	static := loadFeed(b, backend, "testdata/caltrain_20160406.zip")

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
		b.Run(test.Name+"_memory", func(b *testing.B) {
			test.Bench(b, "memory")
		})
		b.Run(test.Name+"_sqlite", func(b *testing.B) {
			test.Bench(b, "sqlite")
		})
		if PostgresConnStr != "" {
			b.Run(test.Name+"_postgres", func(b *testing.B) {
				test.Bench(b, "postgres")
			})
		}
	}
}
