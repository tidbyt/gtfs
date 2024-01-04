package gtfs_test

// The NYC Ferry tests below are built by manually inspecting the NYC
// Ferry website's (https://www.ferry.nyc/) realtime schedule. When a
// particular state was identified (e.g. a delay), a snapshot of the
// realtime feed was downloaded. The tests verify that our library
// interprets the realtime data the same way as their website.

// The WMATA and BART tests are built by manually inspecting WMATA realtime
// feeds and constructing test cases.

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs"
	"tidbyt.dev/gtfs/testutil"
)

// Here we spotted a delay on the ER ferry. At the time of the
// snapshot, the southbound ER ferry was near South Williamsburg
// (stop_id 8) and had a 6 minute delay reported. This delay had been
// propagated to the following two stops, i.e. Dumbo (stop_id 20) and
// Wall St/Pier 11 (stop_id 87). Since Wall St is the final stop on
// the trip, there's only a departure time for Dumbo.
func TestRealtimeIntegrationNYCFerryDelaysOnER(t *testing.T) {
	if testing.Short() {
		t.Skip("loading nycferry dump is slow")
	}

	rt := testutil.LoadRealtimeFile(
		t, "sqlite", "testdata/nycferry_static.zip", "testdata/nycferry_realtime_20200302T110730",
	)

	tz, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// Check that the South Williamsburg stop is delayed. The
	// static schedule puts this trip 321 departure at 11:01:00,
	// but the realtime feed put its arrival/departure at
	// 11:06:01/11:07:31. That means a departure delay of 6
	// minutes and 31 seconds. This is the only trip passing
	// through South Wburg around this time, according to the
	// static schedule.
	departures, err := rt.Departures(
		"8",
		time.Date(2020, 3, 2, 10, 55, 0, 0, tz),
		20*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []gtfs.Departure{
		{ // delayed 391s
			RouteID:      "ER",
			TripID:       "321",
			StopID:       "8",
			StopSequence: 5,
			DirectionID:  0,
			Time:         time.Date(2020, 3, 2, 11, 7, 31, 0, tz),
			Headsign:     "Wall St./Pier 11",
			Delay:        391 * time.Second,
		},
	}, departures)

	// Makes sure delay got propagated to Dumbo.
	//
	// Minor note: the original schedule has this departing 11:13,
	// while query is from 11:15. This is to verify that a
	// departured pushed into time window by delay is included in
	// result.
	departures, err = rt.Departures(
		"20",
		time.Date(2020, 3, 2, 11, 15, 0, 0, tz),
		6*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	require.Equal(t, 1, len(departures))
	assert.Equal(t, []gtfs.Departure{
		{ // delayed 391s
			RouteID:      "ER",
			TripID:       "321",
			StopID:       "20",
			StopSequence: 6,
			DirectionID:  0,
			Time:         time.Date(2020, 3, 2, 11, 19, 31, 0, tz),
			Headsign:     "Wall St./Pier 11",
			Delay:        391 * time.Second,
		},
	}, departures)

	// And there's no 321 departure from Wall St
	departures, err = rt.Departures(
		"87",
		time.Date(2020, 3, 2, 11, 15, 0, 0, tz),
		500*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	for _, d := range departures {
		assert.NotEqual(t, "321", d.TripID)
	}
}

// This is a fun one. Trip 526 of the SB route should depart Bay Ridge
// at 15:50:00, and then Sunset Park at 15:59:00. The NYC Ferry app
// reports a delay of ~1 minute for the Bay Ridge departure, but 0
// delay for Sunset Park. In the realtime feed, this is communicated
// as Bay Ridge departing at 15:51:27, and an arrival at Sunset Park
// at 15:55:40. No departure data is provided for Sunset Park. The
// arrival happening ahead of schedule signifies a return to the
// original schedule.
func TestRealtimeIntegrationNYCFerryDelayWithRecoveryOnSB(t *testing.T) {
	if testing.Short() {
		t.Skip("loading nycferry dump is slow")
	}

	rt := testutil.LoadRealtimeFile(
		t, "sqlite", "testdata/nycferry_static.zip", "testdata/nycferry_realtime_20200302T155200",
	)

	tz, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// The delay at Bay Ridge (stop_id 23)
	departures, err := rt.Departures(
		"23",
		time.Date(2020, 3, 2, 15, 49, 0, 0, tz),
		5*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []gtfs.Departure{
		{
			RouteID:      "SB",
			TripID:       "526",
			StopID:       "23",
			StopSequence: 1,
			DirectionID:  1,
			Time:         time.Date(2020, 3, 2, 15, 51, 27, 0, tz),
			Headsign:     "Wall St./Pier 11",
			Delay:        87 * time.Second,
		},
	}, departures)

	// No delay for SB at Sunset Park (stop_id 118).
	//
	// We use a very narrow window here to only catch SB. There's
	// also a RW trip passing by around then that has the exact
	// same type of delay recovery (delayed departure at first
	// stop, arrival ahead of schedule at Sunset Park. I didn't
	// spot this particular case in NYC Ferry's own delay
	// calculations, so I won't include it here.
	departures, err = rt.Departures(
		"118",
		time.Date(2020, 3, 2, 15, 58, 0, 0, tz),
		2*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []gtfs.Departure{
		{
			RouteID:      "SB",
			TripID:       "526",
			StopID:       "118",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 3, 2, 15, 59, 0, 0, tz),
			Headsign:     "Wall St./Pier 11",
		},
	}, departures)
}

// The WMATA realtime feed has 4 canceled trips on 2024-01-03. One of
// them is trip 5068308_19722. It should appear in a static lookup,
// but not in a realtime lookup.
func TestRealtimeIntegrationWMATACanceledTrips(t *testing.T) {
	if testing.Short() {
		t.Skip("loading wmata dump is slow")
	}

	rt := testutil.LoadRealtimeFile(t, "sqlite", "testdata/wmata_static.zip", "testdata/wmata_20240103T131500.pb")
	static := testutil.LoadStaticFile(t, "sqlite", "testdata/wmata_static.zip")

	tzET, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// Verify trip passes through PF_C14_1 in static data
	deps, err := static.Departures("PF_C14_1", time.Date(2024, 1, 3, 13, 0, 0, 0, tzET), 30*time.Minute, -1, "", -1, nil)
	require.NoError(t, err)
	found := false
	for _, dep := range deps {
		if dep.TripID == "5068308_19722" {
			found = true
			assert.Equal(t, "YELLOW", dep.RouteID)
			assert.Equal(t, time.Date(2024, 1, 3, 13, 8, 0, 0, tzET), dep.Time)
		}
	}
	assert.True(t, found)

	// Verify it's not there when querying realtime data
	deps, err = rt.Departures("PF_C14_1", time.Date(2024, 1, 3, 13, 0, 0, 0, tzET), 30*time.Minute, -1, "", -1, nil)
	require.NoError(t, err)
	found = false
	for _, dep := range deps {
		found = found || dep.TripID == "5068308_19722"
	}
	assert.False(t, found)
}

// WMATA shows delays on the BLUE line along trip 5068249_19722. This
// trip has 28 stops in total. The feed gives departure time for the
// first stop, and arrival times for all others. Lacking departure
// information, any delays should be inferred from the arrival data.
func TestRealtimeIntegrationWMATADelaysAlongATrip(t *testing.T) {

	if testing.Short() {
		t.Skip("loading wmata dump is slow")
	}

	rt := testutil.LoadRealtimeFile(t, "sqlite", "testdata/wmata_static.zip", "testdata/wmata_20240103T131500.pb")

	tzET, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// These have been manually extracted from the static and
	// realtime feeds for this trip.
	for i, tc := range []struct {
		StopID        string
		StaticTime    string
		RealtimeDelay string
	}{
		{"PF_J03_C", "12:53:00", "-15s"},  // 12:52:45
		{"PF_J02_C", "12:57:00", "54s"},   // 12:57:54
		{"PF_C13_C", "13:03:00", "5m18s"}, // 13:08:18
		{"PF_C12_C", "13:05:00", "9m55s"}, // 13:14:55
		{"PF_C11_1", "13:08:00", "9m9s"},  // 13:17:09
		{"PF_C10_1", "13:11:00", "9m16s"}, // 13:20:16
		{"PF_C09_1", "13:13:00", "9m"},    // 13:22:00
		{"PF_C08_1", "13:15:00", "8m51s"}, // 13:23:51
		{"PF_C07_1", "13:17:00", "8m37s"}, // 13:25:37
		{"PF_C06_1", "13:20:00", "8m32s"}, // 13:28:32
		{"PF_C05_1", "13:22:00", "8m54s"}, // 13:30:54
		{"PF_C04_C", "13:25:00", "8m41s"}, // 13:33:41
		{"PF_C03_1", "13:27:00", "8m23s"}, // 13:35:23
		{"PF_C02_1", "13:29:00", "7m54s"}, // 13:36:54
		{"PF_C01_C", "13:31:00", "7m30s"}, // 13:38:30
		{"PF_D01_C", "13:32:00", "7m59s"}, // 13:39:59
		{"PF_D02_1", "13:34:00", "7m29s"}, // 13:41:29
		{"PF_D03_C", "13:36:00", "7m4s"},  // 13:43:04
		{"PF_D04_C", "13:37:00", "7m34s"}, // 13:44:34
		{"PF_D05_C", "13:39:00", "7m13s"}, // 13:46:13
		{"PF_D06_C", "13:41:00", "6m54s"}, // 13:47:54
		{"PF_D07_C", "13:43:00", "6m39s"}, // 13:49:39
		{"PF_D08_C", "13:45:00", "6m43s"}, // 13:51:43
		{"PF_G01_C", "13:50:00", "6m12s"}, // 13:56:12
		{"PF_G02_C", "13:53:00", "6m6s"},  // 13:59:06
		{"PF_G03_C", "13:55:00", "6m18s"}, // 14:01:18
		{"PF_G04_C", "13:58:00", "6m9s"},  // 14:04:09
	} {

		st := strings.Split(tc.StaticTime, ":")
		sH, err := strconv.Atoi(st[0])
		require.NoError(t, err)
		sM, err := strconv.Atoi(st[1])
		require.NoError(t, err)
		staticTime := time.Date(2024, 1, 3, sH, sM, 0, 0, tzET)

		realtimeDelay, err := time.ParseDuration(tc.RealtimeDelay)
		require.NoError(t, err)

		expectedTime := staticTime.Add(realtimeDelay)

		var d *gtfs.Departure
		deps, err := rt.Departures(tc.StopID, expectedTime.Add(-1*time.Minute), 2*time.Minute, -1, "", -1, nil)
		require.NoError(t, err)
		for _, dep := range deps {
			if dep.TripID == "5068249_19722" {
				d2 := dep
				d = &d2
				break
			}
		}

		assert.NotNil(t, d)
		assert.Equal(t, expectedTime, d.Time)
		assert.Equal(t, realtimeDelay, d.Delay)
		assert.Equal(t, tc.StopID, d.StopID)
		assert.Equal(t, "BLUE", d.RouteID)
		assert.Equal(t, uint32(i+1), d.StopSequence)
		assert.Equal(t, "LARGO", d.Headsign)
		assert.Equal(t, int8(0), d.DirectionID)
	}

	// There is also a final record in the realtime data. Since
	// this refers to the final stop along the trip, there should be no departure for it.
	//
	// {"PF_G05_C", "14:01:00", "6m6s"}, // 14:07:06
	deps, err := rt.Departures("PF_G05_C", time.Date(2024, 1, 3, 13, 30, 0, 0, tzET), 60*time.Minute, -1, "", -1, nil)
	require.NoError(t, err)
	found := false
	for _, dep := range deps {
		if dep.TripID == "5068249_19722" {
			found = true
			break
		}
	}
	assert.False(t, found)
}

// Along SILVER route's trip 5068636_19722 toward ASHBURN, 2nd, 3rd
// and 4th stops are skipped. There are delays on all stops.
func TestRealtimeIntegrationWMATASkippedStops(t *testing.T) {

	if testing.Short() {
		t.Skip("loading wmata dump is slow")
	}

	rt := testutil.LoadRealtimeFile(t, "sqlite", "testdata/wmata_static.zip", "testdata/wmata_20240103T163116.pb")

	tzET, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// These are the first few stops along the trip, according to
	// realtime data. The ones without an expected time are
	// skipped.
	for i, tc := range []struct {
		StopID       string
		ExpectedTime string
	}{
		{"PF_G05_C", "2024-01-03 15:52:08 -0500 EST"},
		{"PF_G04_C", ""},
		{"PF_G03_C", ""},
		{"PF_G02_C", ""},
		{"PF_G01_C", "2024-01-03 15:54:32 -0500 EST"},
		{"PF_D08_C", "2024-01-03 16:00:30 -0500 EST"},
		{"PF_D07_C", "2024-01-03 16:02:15 -0500 EST"},
		{"PF_D06_C", "2024-01-03 16:04:05 -0500 EST"},
		{"PF_D05_C", "2024-01-03 16:05:39 -0500 EST"},
	} {
		var d *gtfs.Departure
		deps, err := rt.Departures(tc.StopID, time.Date(2024, 1, 3, 15, 50, 0, 0, tzET), 80*time.Minute, -1, "", -1, nil)
		require.NoError(t, err)
		for _, dep := range deps {
			if dep.TripID == "5068636_19722" {
				d2 := dep
				d = &d2
				break
			}
		}

		if tc.ExpectedTime == "" {
			assert.Nil(t, d, "stop %s should be skipped", tc.StopID)
			continue
		}

		require.NotNil(t, d, "stop %s should not be skipped", tc.StopID)

		expTime, err := time.Parse("2006-01-02 15:04:05 -0700 MST", tc.ExpectedTime)
		require.NoError(t, err)
		expTime = expTime.In(tzET)

		assert.Equal(t, expTime, d.Time)
		assert.Equal(t, tc.StopID, d.StopID)
		assert.Equal(t, "SILVER", d.RouteID)
		assert.Equal(t, uint32(i+1), d.StopSequence)
		assert.Equal(t, "ASHBURN", d.Headsign)
		assert.Equal(t, int8(1), d.DirectionID)
		assert.NotEqual(t, 0, d.Delay)
	}
}

// BART's realtime feed shows delays along trip 1461820, with some
// kinda confusing data.
func TestRealtimeIntegrationBART(t *testing.T) {
	if testing.Short() {
		t.Skip("loading bart dump is slow")
	}

	// According to static schedule, trip 1461820 ends with these 5 stops:
	//
	// stop_id, arrival_time, departure_time
	// CONC,11:21:00,11:21:00
	// NCON,11:24:00,11:24:00
	// PITT,11:30:00,11:31:00
	// PCTR,11:44:00,11:44:00
	// ANTC,11:52:00,11:52:00
	//
	// Realtime feed updates stop time at NCON and PITT. Both
	// updates hold arrival and departure data, so the departure
	// data should take precedence.
	//
	// The weird stuff:
	//  - The NCON delay is 49s, but timestamp is 11:26:42, which
	//    implies a 2m42s delay.
	//  - The PITT delay is 0 (suggesting return to schedule), but
	//    timestamp is 11:32:29 EST, which implies a 1m29s delay.
	//
	// Since spec says timestamps take precedence over delays, the
	// correct departure times should be as per the following test
	// cases.
	//
	// That said, I sort of wonder if this is what BART intended.

	rt := testutil.LoadRealtimeFile(t, "sqlite", "testdata/bart_static.zip", "testdata/bart_20240104T142509.pb")

	tzSF, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	for _, tc := range []struct {
		StopID        string
		ExpectedTime  time.Time
		ExpectedDelay string
	}{
		{"CONC", time.Date(2024, 1, 4, 11, 21, 0, 0, tzSF), "0s"},
		{"NCON", time.Date(2024, 1, 4, 11, 26, 42, 0, tzSF), "2m42s"},
		{"PITT", time.Date(2024, 1, 4, 11, 32, 29, 0, tzSF), "1m29s"},
		{"PCTR", time.Date(2024, 1, 4, 11, 45, 29, 0, tzSF), "1m29s"}, // propagated
		// No departure from ANTC, as it's the final stop on the trip
	} {
		var d *gtfs.Departure
		deps, err := rt.Departures(tc.StopID, tc.ExpectedTime.Add(-time.Minute), 2*time.Minute, -1, "", -1, nil)
		require.NoError(t, err)
		for _, dep := range deps {
			if dep.TripID == "1461820" {
				d2 := dep
				d = &d2
				break
			}
		}
		require.NotNil(t, d, "stop %s should not be skipped", tc.StopID)
		assert.Equal(t, tc.ExpectedTime, d.Time)
		assert.Equal(t, tc.ExpectedDelay, d.Delay.String())
		assert.Equal(t, tc.StopID, d.StopID)
	}
}
