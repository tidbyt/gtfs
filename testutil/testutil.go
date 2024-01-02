package testutil

// Helpers and configuration for tests.

import (
	"archive/zip"
	"bytes"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs"
	"tidbyt.dev/gtfs/parse"
	"tidbyt.dev/gtfs/storage"
)

const (
	PostgresConnStr = "postgres://postgres:mysecretpassword@localhost:5432/gtfs?sslmode=disable"
)

func BuildStorage(t testing.TB, backend string) storage.Storage {
	var s storage.Storage
	var err error
	if backend == "sqlite" {
		s, err = storage.NewSQLiteStorage()
		require.NoError(t, err)
	} else if backend == "postgres" {
		s, err = storage.NewPSQLStorage(PostgresConnStr, true)
		require.NoError(t, err)
	}
	require.NotEqual(t, nil, s, "unknown backend %q", backend)

	return s
}

func LoadStatic(t testing.TB, backend string, buf []byte) *gtfs.Static {
	s := BuildStorage(t, backend)

	// Parse buf into storage
	feedWriter, err := s.GetWriter("test")
	require.NoError(t, err)

	metadata, err := parse.ParseStatic(feedWriter, buf)
	require.NoError(t, err)

	require.NoError(t, feedWriter.Close())

	// Create Static
	reader, err := s.GetReader("test")
	require.NoError(t, err)

	static, err := gtfs.NewStatic(reader, metadata)
	require.NoError(t, err)

	return static
}

func LoadStaticFile(t testing.TB, backend string, filename string) *gtfs.Static {
	buf, err := ioutil.ReadFile(filename)
	require.NoError(t, err)

	return LoadStatic(t, backend, buf)
}

func BuildStatic(
	t testing.TB,
	backend string,
	files map[string][]string,
) *gtfs.Static {

	// Fill in missing files with (mostly blank) dummy data.
	if files["agency.txt"] == nil {
		files["agency.txt"] = []string{"agency_timezone,agency_name,agency_url", "UTC,FooAgency,http://example.com"}
	}
	if files["calendar.txt"] == nil && files["calendar_dates.txt"] == nil {
		files["calendar.txt"] = []string{"service_id"}
	}
	if files["routes.txt"] == nil {
		files["routes.txt"] = []string{"route_id"}
	}
	if files["trips.txt"] == nil {
		files["trips.txt"] = []string{"trip_id"}
	}
	if files["stops.txt"] == nil {
		files["stops.txt"] = []string{"stop_id"}
	}
	if files["stop_times.txt"] == nil {
		files["stop_times.txt"] = []string{"stop_id"}
	}

	buf := BuildZip(t, files)

	return LoadStatic(t, backend, buf)
}

func BuildZip(
	t testing.TB,
	files map[string][]string,
) []byte {

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
