package gtfs_test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"

	"tidbyt.dev/gtfs"
	"tidbyt.dev/gtfs/downloader"
	p "tidbyt.dev/gtfs/proto"
	"tidbyt.dev/gtfs/storage"
	"tidbyt.dev/gtfs/testutil"
)

type MockGTFSServer struct {
	Feeds           map[string][]byte
	RequiredHeaders map[string]map[string]string
	Requests        []string
	Server          *httptest.Server
}

func (m *MockGTFSServer) handler(w http.ResponseWriter, r *http.Request) {
	m.Requests = append(m.Requests, r.URL.Path)
	feed, found := m.Feeds[r.URL.Path]
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	required := m.RequiredHeaders[r.URL.Path]
	if len(required) == 0 {
		w.Write(feed)
		return
	}

	for k, v := range required {
		if r.Header.Get(k) != v {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	w.Write(feed)
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

func validRealtimeFeed(t *testing.T, timestamp time.Time) []byte {
	incrementality := p.FeedHeader_FULL_DATASET
	tripScheduleRelationship := p.TripDescriptor_SCHEDULED
	data, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Incrementality:      &incrementality,
			Timestamp:           proto.Uint64(uint64(timestamp.Unix())),
		},
		Entity: []*p.FeedEntity{
			{
				Id: proto.String("t"),
				TripUpdate: &p.TripUpdate{
					Trip: &p.TripDescriptor{
						TripId:               proto.String("t"),
						ScheduleRelationship: &tripScheduleRelationship,
					},
					StopTimeUpdate: []*p.TripUpdate_StopTimeUpdate{
						{
							StopId: proto.String("s"),
							Arrival: &p.TripUpdate_StopTimeEvent{
								Delay: proto.Int32(12),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	return data
}

func testManagerLoadSingleFeed(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	server.Feeds["/static.zip"] = testutil.BuildZip(t, validFeed())

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// First Load will fail, coz feed is new.
	s, err := m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// Refresh will load the feed
	require.NoError(t, m.Refresh(context.Background()))

	// So next Load will succeed
	s, err = m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)

	// Static is now loaded and serves data
	stops, err := s.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S", stops[0].Name)
}

func testManagerLoadMultipleURLs(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	// Two different static feeds, served on separate URLs.
	files := validFeed()
	server.Feeds["/static1.zip"] = testutil.BuildZip(t, validFeed())
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s2,S2,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s2,1",
	}
	server.Feeds["/static2.zip"] = testutil.BuildZip(t, files)

	// First request for each will fail, but create requests.
	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)
	s1, err := m.LoadStaticAsync("a", server.Server.URL+"/static1.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	assert.Nil(t, s1)
	s2, err := m.LoadStaticAsync("a", server.Server.URL+"/static2.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	assert.Nil(t, s2)

	// Refresh will load both feeds
	require.NoError(t, m.Refresh(context.Background()))

	// Both can now be loaded
	s1, err = m.LoadStaticAsync("a", server.Server.URL+"/static1.zip", nil, when)
	require.NoError(t, err)
	s2, err = m.LoadStaticAsync("a", server.Server.URL+"/static2.zip", nil, when)
	require.NoError(t, err)

	// And can be read
	stops, err := s1.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S", stops[0].Name)

	stops, err = s2.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S2", stops[0].Name)
}

func testManagerLoadWithHeaders(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	// Three feeds, on separate URLs.
	files := validFeed()
	server.Feeds["/static1.zip"] = testutil.BuildZip(t, validFeed())
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s2,S2,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s2,1",
	}
	server.Feeds["/static2.zip"] = testutil.BuildZip(t, files)
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s3,S3,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s3,1",
	}
	server.Feeds["/static3.zip"] = testutil.BuildZip(t, files)

	// First and second requires different headers. Third requires
	// no headers.
	server.RequiredHeaders = map[string]map[string]string{
		"/static1.zip": {
			"X-Header": "1",
		},
		"/static2.zip": {
			"X-Header": "2",
		},
	}

	// Attempt to load without headers.
	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err := m.LoadStaticAsync("a", server.Server.URL+"/static1.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	_, err = m.LoadStaticAsync("a", server.Server.URL+"/static2.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	_, err = m.LoadStaticAsync("a", server.Server.URL+"/static3.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// Refresh will attempt to download all three, as only the
	// third will succeed there'll be an error.
	require.Error(t, m.Refresh(context.Background()))

	// First two fails to load, but third is ok.
	_, err = m.LoadStaticAsync("a", server.Server.URL+"/static1.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	_, err = m.LoadStaticAsync("a", server.Server.URL+"/static2.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	s3, err := m.LoadStaticAsync("a", server.Server.URL+"/static3.zip", nil, when)
	require.NoError(t, err)
	stops, err := s3.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S3", stops[0].Name)

	// Re-request the first two with correct headers.
	_, err = m.LoadStaticAsync("a", server.Server.URL+"/static1.zip", map[string]string{
		"X-Header": "1",
	}, when)
	require.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	_, err = m.LoadStaticAsync("a", server.Server.URL+"/static2.zip", map[string]string{
		"X-Header": "2",
	}, when)
	require.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// And refresh should now be able to download them
	require.NoError(t, m.Refresh(context.Background()))
	assert.Len(t, server.Requests, 5)

	// And can be read
	s2, err := m.LoadStaticAsync("a", server.Server.URL+"/static2.zip", nil, when)
	require.NoError(t, err)
	stops, err = s2.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S2", stops[0].Name)

	s3, err = m.LoadStaticAsync("a", server.Server.URL+"/static3.zip", nil, when)
	require.NoError(t, err)
	stops, err = s3.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S3", stops[0].Name)
}

func testManagerMultipleConsumers(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	// Three feeds, on separate URLs.
	files := validFeed()
	server.Feeds["/static1.zip"] = testutil.BuildZip(t, validFeed())
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s2,S2,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s2,1",
	}
	server.Feeds["/static2.zip"] = testutil.BuildZip(t, files)
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s3,S3,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s3,1",
	}
	server.Feeds["/static3.zip"] = testutil.BuildZip(t, files)

	// First and second requires different headers. Third requires
	// no headers.
	server.RequiredHeaders = map[string]map[string]string{
		"/static1.zip": {
			"X-Header": "1",
		},
		"/static2.zip": {
			"X-Header": "2",
		},
	}

	// We'll use three "consumers": A, B and C. Each of these can
	// request any feed, and they can do so using their own
	// headers.

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// A requests static1.zip with correct headers.
	_, err := m.LoadStaticAsync("A", server.Server.URL+"/static1.zip", map[string]string{
		"X-Header": "1",
	}, when)
	require.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// B requests static2.zip with _incorrect_ header
	_, err = m.LoadStaticAsync("B", server.Server.URL+"/static2.zip", map[string]string{
		"X-Header": "bad header!",
	}, when)
	require.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// C requests static3.zip
	_, err = m.LoadStaticAsync("C", server.Server.URL+"/static3.zip", nil, when)
	require.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// No requests to server yet, but refresh will request all
	// three feeds. Since the request for static2.zip fails (bad
	// headers), Refresh will return an error.
	assert.Equal(t, 0, len(server.Requests))
	assert.Error(t, m.Refresh(context.Background()))
	assert.Len(t, server.Requests, 3)

	// A and C should now be able to read their feeds.
	a, err := m.LoadStaticAsync("A", server.Server.URL+"/static1.zip", map[string]string{
		"X-Header": "1",
	}, when)
	require.NoError(t, err)
	stops, err := a.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S", stops[0].Name)

	c, err := m.LoadStaticAsync("C", server.Server.URL+"/static3.zip", nil, when)
	require.NoError(t, err)
	stops, err = c.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S3", stops[0].Name)

	// B makes a request with correct headers for static2.zip.
	_, err = m.LoadStaticAsync("B", server.Server.URL+"/static2.zip", map[string]string{
		"X-Header": "2",
	}, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// Refresh should now succeed, and static2.zip should be
	// loadable.
	assert.NoError(t, m.Refresh(context.Background()))
	assert.Len(t, server.Requests, 4)
	b, err := m.LoadStaticAsync("B", server.Server.URL+"/static2.zip", map[string]string{
		"X-Header": "2",
	}, when)
	require.NoError(t, err)
	stops, err = b.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S2", stops[0].Name)

	// With all feeds in storage, Load will succeed for all, even
	// with incorrect headers.
	a, err = m.LoadStaticAsync("A", server.Server.URL+"/static1.zip", map[string]string{
		"X-Header": "bad header!",
	}, when)
	require.NoError(t, err)
	stops, err = a.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S", stops[0].Name)

	a, err = m.LoadStaticAsync("A", server.Server.URL+"/static2.zip", map[string]string{
		"X-Header": "bad header!",
	}, when)
	require.NoError(t, err)
	stops, err = a.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S2", stops[0].Name)

	a, err = m.LoadStaticAsync("A", server.Server.URL+"/static3.zip", nil, when)
	require.NoError(t, err)
	stops, err = a.NearbyStops(1.0, -2.0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, stops, 1)
	assert.Equal(t, "S3", stops[0].Name)

	// At this point, we should have 3 requests recorded in
	// storage, with A as consumer for all, and B/C for 1 each.
	requests, err := strg.ListFeedRequests("")
	require.NoError(t, err)
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].URL < requests[j].URL
	})
	assert.Equal(t, 3, len(requests))
	assert.Equal(t, server.Server.URL+"/static1.zip", requests[0].URL)
	assert.Equal(t, server.Server.URL+"/static2.zip", requests[1].URL)
	assert.Equal(t, server.Server.URL+"/static3.zip", requests[2].URL)
	assert.Equal(t, 1, len(requests[0].Consumers))
	assert.Equal(t, 2, len(requests[1].Consumers))
	assert.Equal(t, 2, len(requests[2].Consumers))

	assert.Equal(t, "A", requests[0].Consumers[0].Name)
	con2 := []string{requests[1].Consumers[0].Name, requests[1].Consumers[1].Name}
	con3 := []string{requests[2].Consumers[0].Name, requests[2].Consumers[1].Name}
	sort.Strings(con2)
	sort.Strings(con3)
	assert.Equal(t, []string{"A", "B"}, con2)
	assert.Equal(t, []string{"A", "C"}, con3)
}

func testManagerLoadWithRefresh(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	// Three versions of a feed, each differing in stops.txt.
	files := validFeed()
	feed1Zip := testutil.BuildZip(t, files)
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s2,S,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s2,1",
	}
	feed2Zip := testutil.BuildZip(t, files)
	files["stops.txt"] = []string{
		"stop_id,stop_name,stop_lat,stop_lon",
		"s3,S,12,34",
	}
	files["stop_times.txt"] = []string{
		"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
		"t,12:00:00,12:00:00,s3,1",
	}
	feed3Zip := testutil.BuildZip(t, files)

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// Attempting to load will fail, but adds a request for it to
	// be downloaded by a later Refresh()
	server.Feeds["/static.zip"] = feed1Zip
	s1, err := m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	assert.Nil(t, s1)

	// Call Refresh and it'll be retrieved
	assert.NoError(t, m.Refresh(context.Background()))

	// It got added to storage
	feeds, err := strg.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))

	// It can be loaded and serves the correct data
	s1, err = m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)
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
	s2, err := m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)

	stops, err = s2.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s", stops[0].ID)
	assert.Equal(t, []string{"/static.zip"}, server.Requests)

	// No new feed added to storage either
	feeds, err = strg.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))

	// Set a very low refresh interval, and manager will consider
	// existing data stale. Refresh, and we'll see the feed 2
	// data served.
	m.StaticRefreshInterval = time.Duration(0)
	require.NoError(t, m.Refresh(context.Background()))
	s2, err = m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)
	stops, err = s2.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s2", stops[0].ID) // s2 instead of s
	assert.Equal(t, []string{"/static.zip", "/static.zip"}, server.Requests)

	// Second feed added to storage
	feeds, err = strg.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 2, len(feeds))

	// Serve a third feed, and refresh.
	server.Feeds["/static.zip"] = feed3Zip
	assert.NoError(t, m.Refresh(context.Background()))

	// The refesh will haved downloaded the third feed to storage
	feeds, err = strg.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 3, len(feeds))
	assert.Equal(t, []string{"/static.zip", "/static.zip", "/static.zip"}, server.Requests)

	// This time, load with a time for which no feed is
	// active. It'll error out.
	when = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s3, err := m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	assert.Nil(t, s3)

	// Set a high refresh interval, and refresh. Server should not
	// be hit.
	m.StaticRefreshInterval = time.Hour
	assert.NoError(t, m.Refresh(context.Background()))
	assert.Equal(t, []string{"/static.zip", "/static.zip", "/static.zip"}, server.Requests)

	// Load with a time for which feed 3 is active.
	when = time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)
	s3, err = m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)
	stops, err = s3.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s3", stops[0].ID)

	// No new feed added to storage
	feeds, err = strg.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 3, len(feeds))

	// No new request to server
	assert.Equal(t, []string{"/static.zip", "/static.zip", "/static.zip"}, server.Requests)
}

// In the case where a feed is completely broken, manager should
// not continue refreshing it until refresh interval has passed.
func testManagerBrokenData(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	goodZip := testutil.BuildZip(t, validFeed())
	badZip := testutil.BuildZip(t, map[string][]string{"parse": []string{"fail"}})

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// First attempt to load creates request for feed
	_, err := m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	require.ErrorIs(t, err, gtfs.ErrNoActiveFeed)

	// Refresh will attempt to load the feed, but fail, as server
	// will give 404
	assert.Error(t, m.Refresh(context.Background()))
	assert.Equal(t, []string{"/static.zip"}, server.Requests)

	// Attempting again will fail again
	assert.Error(t, m.Refresh(context.Background()))
	assert.Equal(t, []string{"/static.zip", "/static.zip"}, server.Requests)

	// Now make the broken zip available
	server.Feeds["/static.zip"] = badZip

	// Refresh will try and fail again.
	assert.Error(t, m.Refresh(context.Background()))
	assert.Equal(t, []string{"/static.zip", "/static.zip", "/static.zip"}, server.Requests)

	// Since the last refresh failed due to a parse error (bad
	// data), the manager will wait for the refresh interval
	// before new requests are made.
	assert.NoError(t, m.Refresh(context.Background()))
	assert.NoError(t, m.Refresh(context.Background()))
	assert.NoError(t, m.Refresh(context.Background()))
	assert.NoError(t, m.Refresh(context.Background()))
	assert.NoError(t, m.Refresh(context.Background()))
	assert.Equal(t, 3, len(server.Requests))

	// Lower StaticRefreshInterval and make the good zip
	// available. Refresh will download the new data.
	server.Feeds["/static.zip"] = goodZip
	m.StaticRefreshInterval = time.Duration(0)
	assert.NoError(t, m.Refresh(context.Background()))
	assert.Equal(t, 4, len(server.Requests))

	// Data can be loaded
	s, err := m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)
	stops, err := s.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))

	// With StaticRefreshInterval at 0, refresh will do a request each
	// time.
	assert.NoError(t, m.Refresh(context.Background()))
	assert.Equal(t, 5, len(server.Requests))
	assert.NoError(t, m.Refresh(context.Background()))
	assert.Equal(t, 6, len(server.Requests))
	assert.NoError(t, m.Refresh(context.Background()))
	assert.Equal(t, 7, len(server.Requests))

	// But there's still only 1 feed in storage, as the data
	// didn't change between requests.
	feeds, err := strg.ListFeeds(storage.ListFeedsFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, len(feeds))

	// Serve bad data again. Refresh will fail.
	server.Feeds["/static.zip"] = badZip
	require.Error(t, m.Refresh(context.Background()))
	assert.Equal(t, 8, len(server.Requests))

	// But we can still load it, as the old feed is still there.
	s, err = m.LoadStaticAsync("a", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)
	stops, err = s.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
}

// Requesting a new URL with LoadStaticAsync() should return
// ErrNoActiveFeed. It should also place a record in storage
// signalling that the feed has been requested.
func testManagerAsyncLoad(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	server.Feeds["/static.zip"] = testutil.BuildZip(t, validFeed())

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	// Async request a feed for the first time
	static, err := m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	assert.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	assert.Nil(t, static)

	// Record w URL only in DB
	// A FeedRequest should be in DB
	reqs, err := strg.ListFeedRequests("")
	require.NoError(t, err)
	assert.Equal(t, 1, len(reqs))
	assert.Equal(t, server.Server.URL+"/static.zip", reqs[0].URL)
	assert.Equal(t, 1, len(reqs[0].Consumers))
	assert.Equal(t, "app1", reqs[0].Consumers[0].Name)
	assert.Equal(t, "", reqs[0].Consumers[0].Headers)

	// Additional requests for the feed doesn't add new
	// records. Existing record is exactly as before.
	prevReq := reqs[0]
	_, err = m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	assert.True(t, errors.Is(err, gtfs.ErrNoActiveFeed))
	_, err = m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	assert.True(t, errors.Is(err, gtfs.ErrNoActiveFeed))
	reqs, err = strg.ListFeedRequests("")
	require.NoError(t, err)
	assert.Equal(t, 1, len(reqs))
	assert.Equal(t, prevReq, reqs[0])

	// Processing async requests will retrieve the feed
	err = m.Refresh(context.Background())
	assert.NoError(t, err)

	// Subsequent async requests will return the feed
	static, err = m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	assert.NoError(t, err)
	stops, err := static.NearbyStops(1, 1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(stops))
	assert.Equal(t, "s", stops[0].ID)
}

func testManagerLoadRealtime(t *testing.T, strg storage.Storage) {
	m := gtfs.NewManager(strg)

	server := managerFixture()
	defer server.Server.Close()

	server.Feeds["/realtime.pb"] = validRealtimeFeed(t, time.Unix(12345, 0))
	server.Feeds["/static.zip"] = testutil.BuildZip(t, validFeed())

	when := time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)

	_, err := m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	require.ErrorIs(t, err, gtfs.ErrNoActiveFeed)
	require.NoError(t, m.Refresh(context.Background()))
	static, err := m.LoadStaticAsync("app1", server.Server.URL+"/static.zip", nil, when)
	require.NoError(t, err)

	// Mock clock on the downloader to control caching
	now := time.Now()
	dl := downloader.NewMemory()
	dl.TimeNow = func() time.Time {
		return now
	}
	m.Downloader = dl

	// Realtime feed can now be loaded
	realtime, err := m.LoadRealtime(
		"app1", static,
		server.Server.URL+"/realtime.pb", nil,
		when,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), realtime.Timestamp)

	// Publish a new realtime feed
	server.Feeds["/realtime.pb"] = validRealtimeFeed(t, time.Unix(12346, 0))

	// Old is still served from cache
	realtime, err = m.LoadRealtime(
		"app1", static,
		server.Server.URL+"/realtime.pb", nil,
		when,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), realtime.Timestamp)

	// Fast forward time to invalidate cached feed, and the new
	// will be retrieved
	now = now.Add(3 * time.Minute)
	realtime, err = m.LoadRealtime(
		"app1", static,
		server.Server.URL+"/realtime.pb", nil,
		when,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(12346), realtime.Timestamp)

	// Bad data results in error
	server.Feeds["/bad.pb"] = []byte("this isn't protobuf")
	_, err = m.LoadRealtime(
		"app1", static,
		server.Server.URL+"/bad.pb", nil,
		when,
	)
	assert.Error(t, err, "umarshaling protobuf")

	// Missing data is also error
	_, err = m.LoadRealtime(
		"app1", static,
		server.Server.URL+"/missing.pb", nil,
		when,
	)
	assert.ErrorContains(t, err, "404")

	// 404 isn't cached
	server.Feeds["/missing.pb"] = validRealtimeFeed(t, time.Unix(12348, 0))
	realtime, err = m.LoadRealtime(
		"app1", static,
		server.Server.URL+"/missing.pb", nil,
		when,
	)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12348), realtime.Timestamp)

}

// Verifies that Manager respects agency timezone when determining if
// a feed is active.
func testManagerRespectTimezones(t *testing.T, strg storage.Storage) {
	// TODO: write me
}

// Verifies that manager can refresh a bunch of feeds according to the
// StaticRefreshInterval.
func testManagerRefreshFeeds(t *testing.T, strg storage.Storage) {
	// TODO: write me
}

func TestManager(t *testing.T) {
	for _, test := range []struct {
		Name string
		Test func(*testing.T, storage.Storage)
	}{
		{"LoadSingleFeed", testManagerLoadSingleFeed},
		{"LoadMultipleURLs", testManagerLoadMultipleURLs},
		{"LoadWithHeaders", testManagerLoadWithHeaders},
		{"MultipleConsumers", testManagerMultipleConsumers},
		{"LoadWithRefresh", testManagerLoadWithRefresh},
		{"ManagerBrokenData", testManagerBrokenData},
		{"AsyncLoad", testManagerAsyncLoad},
		{"LoadRealtime", testManagerLoadRealtime},
		{"RespectTimezones", testManagerRespectTimezones},
		{"RefreshFeeds", testManagerRefreshFeeds},
	} {
		t.Run(fmt.Sprintf("%s_SQLiteMemory", test.Name), func(t *testing.T) {
			s, err := storage.NewSQLiteStorage(storage.SQLiteConfig{OnDisk: false})
			require.NoError(t, err)
			test.Test(t, s)
		})
		t.Run(fmt.Sprintf("%s_SQLiteFile", test.Name), func(t *testing.T) {
			dir, err := ioutil.TempDir("", "gtfs_storage_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			s, err := storage.NewSQLiteStorage(storage.SQLiteConfig{OnDisk: true, Directory: dir})
			require.NoError(t, err)
			test.Test(t, s)

		})
		if testutil.PostgresConnStr != "" {
			t.Run(fmt.Sprintf("%s_Postgres", test.Name), func(t *testing.T) {
				s, err := storage.NewPSQLStorage(testutil.PostgresConnStr, true)
				require.NoError(t, err)
				test.Test(t, s)
			})
		}
	}
}
