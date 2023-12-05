package gtfs

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/storage"
)

type MockGTFSServer struct {
	Feeds    map[string][]byte
	Requests []string
	Server   *httptest.Server
}

func (m *MockGTFSServer) handler(w http.ResponseWriter, r *http.Request) {
	m.Requests = append(m.Requests, r.URL.Path)
	if feed, found := m.Feeds[r.URL.Path]; found {
		w.Write(feed)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func managerFixture() *MockGTFSServer {
	m := &MockGTFSServer{
		Feeds:    map[string][]byte{},
		Requests: []string{},
	}

	m.Server = httptest.NewServer(http.HandlerFunc(m.handler))

	return m
}

func validFeed() map[string][]string {
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

func TestManagerLoadSingleFeed(t *testing.T) {
	server := managerFixture()
	defer server.Server.Close()

	server.Feeds["/static.zip"] = buildZip(t, validFeed())

	storage := storage.NewMemoryStorage()
	m := NewManager(storage)

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	s, err := m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.NoError(t, err)

	// static loaded and serves data
	stops, err := s.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S", stops[0].Name)
}

func TestManagerLoadMultipleURLs(t *testing.T) {
	server := managerFixture()
	defer server.Server.Close()

	// Two different static feeds. Identical except for
	// stops.txt. Served on different URLs.
	files := validFeed()
	server.Feeds["/static1.zip"] = buildZip(t, validFeed())
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s2,S2,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s2,1",
	}
	server.Feeds["/static2.zip"] = buildZip(t, files)

	// Both feeds can be loaded
	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)
	storage := storage.NewMemoryStorage()
	m := NewManager(storage)
	s1, err := m.LoadStatic(server.Server.URL+"/static1.zip", when)
	require.NoError(t, err)
	s2, err := m.LoadStatic(server.Server.URL+"/static2.zip", when)
	require.NoError(t, err)

	// And can be read simultaneously
	stops, err := s1.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S", stops[0].Name)

	stops, err = s2.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S2", stops[0].Name)
}

func TestManagerLoadWithRefresh(t *testing.T) {
	server := managerFixture()
	defer server.Server.Close()

	// Three versions of a feed. Each has different headsigns in
	// trips.txt.
	files := validFeed()
	feed1Zip := buildZip(t, files)
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s2,S,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s2,1",
	}
	feed2Zip := buildZip(t, files)
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s3,S,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s3,1",
	}
	feed3Zip := buildZip(t, files)

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// Load the first version of the feed
	s := storage.NewMemoryStorage()
	m := NewManager(s)
	server.Feeds["/static.zip"] = feed1Zip
	s1, err := m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.NoError(t, err)

	// It got added to storage
	feeds, err := s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))

	// And data served is from feed 1
	stops, err := s1.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s", stops[0].ID)

	// Replace the feed data served. Refresh the manager. We'll
	// still see the first feed's data served, as too little time
	// has passed for the refresh to actually go out and retrieve
	// the new data.
	server.Feeds["/static.zip"] = feed2Zip
	assert.NoError(t, m.Refresh(context.Background()))
	s2, err := m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.NoError(t, err)

	stops, err = s2.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s", stops[0].ID)
	assert.Equal(t, []string{"/static.zip"}, server.Requests)

	// No new feed added to storage either
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))

	// Set a very low refresh interval, and manager will consider
	// existing data stale. Refreshi, and we'll see the feed 2
	// data served.
	m.RefreshInterval = time.Duration(0)
	require.NoError(t, m.Refresh(context.Background()))
	s2, err = m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.NoError(t, err)
	stops, err = s2.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s2", stops[0].ID)
	assert.Equal(t, []string{"/static.zip", "/static.zip"}, server.Requests)

	// Second feed added to storage
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 2, len(feeds))

	// Serve a third feed, and refresh. But now laod with time for
	// which no active feed exists.
	when = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	server.Feeds["/static.zip"] = feed3Zip
	assert.NoError(t, m.Refresh(context.Background()))
	s3, err := m.LoadStatic(server.Server.URL+"/static.zip", when)

	// No feed for the requested time means we get a
	// ErrNoActiveFeed
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoActiveFeed))
	assert.Nil(t, s3)

	// But the latest feed will still have been loaded into
	// storage. It needed refreshing, so it got refreshed.
	assert.Equal(t, []string{"/static.zip", "/static.zip", "/static.zip"}, server.Requests)
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 3, len(feeds))

	// Set a high refresh interval, and refresh
	m.RefreshInterval = time.Hour
	assert.NoError(t, m.Refresh(context.Background()))

	// Load with time where the feeds are active and feed 3 should
	// be served up, without hitting the server
	when = time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)
	s3, err = m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.NoError(t, err)
	stops, err = s3.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s3", stops[0].ID)

	// No new feed added to storage
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 3, len(feeds))

	// No new request to server
	assert.Equal(t, []string{"/static.zip", "/static.zip", "/static.zip"}, server.Requests)
}

// In the case where a feed is completely broken, manager should
// not continue refreshing it until refresh interval has passed.
func TestManagerBrokenData(t *testing.T) {
	server := managerFixture()
	defer server.Server.Close()

	goodZip := buildZip(t, validFeed())
	badZip := buildZip(t, map[string][]string{"parse": []string{"fail"}})

	server.Feeds["/static.zip"] = badZip

	s, _ := storage.NewSQLiteStorage()
	m := NewManager(s)

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// With a malformed feed, manager returns error
	_, err := m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.Error(t, err)
	_, err = m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.Error(t, err)

	// Each attempt results in a request, even though refresh
	// interval is high
	assert.Equal(t, []string{"/static.zip", "/static.zip"}, server.Requests)

	// But no feed is added to storage
	feeds, err := s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 0, len(feeds))

	// Serve valid data and it gets loaded
	server.Feeds["/static.zip"] = goodZip
	static, err := m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.NoError(t, err)
	stops, err := static.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s", stops[0].ID)

	// And feed is added to storage
	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))
	goodUpdatedAt := feeds[0].UpdatedAt

	// Serve bad data again. Refresh will fail.
	m.RefreshInterval = time.Duration(0) // to trigger fetch
	server.Feeds["/static.zip"] = badZip
	require.Error(t, m.Refresh(context.Background()))

	// But we can still load it, as the old feed is still good.
	_, err = m.LoadStatic(server.Server.URL+"/static.zip", when)
	require.NoError(t, err)

	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))
	assert.NotEqual(t, goodUpdatedAt, feeds[0].UpdatedAt)
}

// Requesting a new URL with LoadStaticAsync() should return
// ErrNoActiveFeed. It should also place a record in storage
// signalling that the feed has been requested.
func TestManagerAsyncLoad(t *testing.T) {
	server := managerFixture()
	defer server.Server.Close()

	server.Feeds["/static.zip"] = buildZip(t, validFeed())

	s, _ := storage.NewSQLiteStorage()
	m := NewManager(s)

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// Async request a feed for the first time
	static, err := m.LoadStaticAsync(server.Server.URL+"/static.zip", when)
	assert.True(t, errors.Is(err, ErrNoActiveFeed))
	assert.Nil(t, static)

	// Record w URL only in DB
	feeds, err := s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))
	assert.Equal(t, &storage.FeedMetadata{
		URL: server.Server.URL + "/static.zip",
	}, feeds[0])

	// Additional requests for the feed doesn't add new records
	_, err = m.LoadStaticAsync(server.Server.URL+"/static.zip", when)
	assert.True(t, errors.Is(err, ErrNoActiveFeed))
	_, err = m.LoadStaticAsync(server.Server.URL+"/static.zip", when)
	assert.True(t, errors.Is(err, ErrNoActiveFeed))

	feeds, err = s.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))
	assert.Equal(t, &storage.FeedMetadata{
		URL: server.Server.URL + "/static.zip",
	}, feeds[0])

	// Processing async requests will retrieve the feed
	err = m.Refresh(context.Background())
	assert.NoError(t, err)

	// Subsequent async requests will return the feed
	static, err = m.LoadStaticAsync(server.Server.URL+"/static.zip", when)
	assert.NoError(t, err)
	stops, err := static.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s", stops[0].ID)
}

// Verifies that Manager respects agency timezone when determining if
// a feed is active.
func TestManagerRespectTimezones(t *testing.T) {
	// TODO: write me
}

// Verifies that manager can refresh a bunch of feeds according to the
// RefreshInterval.
func TestManagerRefreshFeeds(t *testing.T) {
	// TODO: write me
}
