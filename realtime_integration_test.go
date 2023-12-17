package gtfs_test

// These tests are built by manually inspecting the NYC Ferry
// website's (https://www.ferry.nyc/) realtime schedule. When a
// particular state was identified (e.g. a delay), a snapshot of the
// realtime feed was downloaded. The tests verify that our library
// interprets the realtime data the same way as their website.

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs"
)

func loadNYCFerryRealtime(t *testing.T, suffix string) *gtfs.Realtime {
	static, reader := loadFeed2(t, "sqlite", "testdata/nycferry_static.zip")

	buf, err := ioutil.ReadFile(fmt.Sprintf("testdata/nycferry_realtime_%s", suffix))
	require.NoError(t, err)

	rt, err := gtfs.NewRealtime(context.Background(), static, reader, [][]byte{buf})
	require.NoError(t, err)

	return rt
}

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

	rt := loadNYCFerryRealtime(t, "20200302T110730")

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
		},
	}, departures)
	return

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

	rt := loadNYCFerryRealtime(t, "20200302T155200")

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
