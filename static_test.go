package gtfs_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/model"
	"tidbyt.dev/gtfs/testutil"
)

func testStaticDeparturesWindowing(t *testing.T, backend string) {
	duration := func(h, m, s int) time.Duration {
		return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second
	}

	g := testutil.BuildStatic(t, backend, map[string][]string{
		// A weekdays only schedule
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"weekday,20200101,20201231,1,1,1,1,1,0,0",
		},
		// Two routes: L and F
		"routes.txt": {"route_id,route_short_name,route_type", "L,l,0", "F,f,0"},
		// A bunch of stops
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"3a,3a,1,1",
			"14,14,2,2",
			"6a,6a,3,3",
			"w4,w4,4,4",
			"23,23,5,5",
		},
		// The L has three trips running east, two running west. F runs north then south.
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"LE1,L,weekday,0",
			"LW1,L,weekday,1",
			"LE2,L,weekday,0",
			"LW2,L,weekday,1",
			"LE3,L,weekday,0",
			"FN1,F,weekday,1",
			"FS1,F,weekday,0",
		},
		// The L trips run 3rd ave - 14th st - 6th ave. F runs W4 - 14th - 23rd.
		"stop_times.txt": {
			"trip_id,stop_id,departure_time,arrival_time,stop_sequence",
			"LW1,3a,6:10:0,6:10:0,1",
			"LW1,14,6:12:0,6:12:0,2",
			"LW1,6a,6:14:0,6:14:0,3",
			"LE2,6a,6:22:0,6:22:0,100",
			"LE2,14,6:24:0,6:24:0,102",
			"LE2,3a,6:26:0,6:26:0,104",
			"LW2,3a,6:30:0,6:30:0,1",
			"LW2,14,6:32:0,6:32:0,2",
			"LW2,6a,6:34:0,6:34:0,3",
			"LE3,6a,6:42:0,6:42:0,1",
			"LE3,14,6:44:0,6:44:0,2",
			"LE3,3a,6:46:0,6:46:0,3",
			"FN1,w4,6:30:0,6:30:0,1",
			"FN1,14,6:35:0,6:35:0,2",
			"FN1,23,6:40:0,6:40:0,3",
			"FS1,23,6:45:0,6:45:0,10",
			"FS1,14,6:50:0,6:50:0,11",
			"FS1,w4,6:55:0,6:55:0,15",
		},
	})

	// Feb 4th is a Tuesday, so the weekday schedule
	// applies. Within 30 minutes of 6 AM, the 14th street
	// station should have 2 L train departures
	departures, err := g.Departures("14", time.Date(2020, 2, 4, 6, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LW1",
			DirectionID:  1,
			StopSequence: 2,
			Time:         time.Date(2020, 2, 4, 6, 12, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LE2",
			StopSequence: 102,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 4, 6, 24, 0, 0, time.UTC)},
	}, departures)

	// Extend the window to 50 minutes and we capture 2 extra L
	// stops, and two F train stops. The last one is right on the
	// boundary of the window.
	departures, err = g.Departures("14", time.Date(2020, 2, 4, 6, 10, 0, 0, time.UTC), 50*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LW1",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 4, 6, 12, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LE2",
			StopSequence: 102,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 4, 6, 24, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LW2",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 4, 6, 32, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "F",
			TripID:       "FN1",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 4, 6, 35, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LE3",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 4, 6, 44, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "F",
			TripID:       "FS1",
			StopSequence: 11,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 4, 6, 50, 0, 0, time.UTC)},
	}, departures)

	// Start window at 6:30 and earlier departures are cut
	departures, err = g.Departures("14", time.Date(2020, 2, 4, 6, 30, 0, 0, time.UTC), 50*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LW2",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 4, 6, 32, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "F",
			TripID:       "FN1",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 4, 6, 35, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LE3",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 4, 6, 44, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "F",
			TripID:       "FS1",
			StopSequence: 11,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 4, 6, 50, 0, 0, time.UTC)},
	}, departures)

	// Push window past last departure and we get nothing
	departures, err = g.Departures("14", time.Date(2020, 2, 4, 6, 51, 0, 0, time.UTC), 50*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)

	// Non-existent stop also gives us nothing
	departures, err = g.Departures("FOO", time.Date(2020, 2, 4, 6, 30, 0, 0, time.UTC), 50*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)

	// But a large enough window reaches next day's departures.
	departures, err = g.Departures("14", time.Date(2020, 2, 4, 6, 51, 0, 0, time.UTC), duration(23, 50, 0), -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LW1",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 5, 6, 12, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LE2",
			StopSequence: 102,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 5, 6, 24, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LW2",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 5, 6, 32, 0, 0, time.UTC)},
		{
			StopID:       "14",
			RouteID:      "F",
			TripID:       "FN1",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 5, 6, 35, 0, 0, time.UTC)},
	}, departures)

	// Outside of calendar, we get nothing (Jan 1st 2021 was a Friday)
	departures, err = g.Departures("14", time.Date(2021, 1, 1, 6, 30, 0, 0, time.UTC), 50*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)
}

func testStaticDeparturesWeekendSchedule(t *testing.T, backend string) {
	g := testutil.BuildStatic(t, backend, map[string][]string{
		// A weekend and a weekday schedules
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"weekday,20200101,20201231,1,1,1,1,1,0,0",
			"weekend,20200101,20201231,0,0,0,0,0,1,1",
		},
		// Only 1 route: the L
		"routes.txt": {"route_id,route_long_name,route_type", "L,The ELL,3"},
		// Stops on Manhattan only
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"8a,8a,1,1",
			"6a,6a,2,2",
			"14,14,3,3",
			"3a,3a,4,4",
		},
		// The L runs east then west on weekdays. Only east on weekends.
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"LE1,L,weekday,0",
			"LW1,L,weekday,1",
			"LE2,L,weekend,0",
		},
		// The trips stop at all 4 stations
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"LE1,8a,1,9:0:0,9:0:0",
			"LE1,6a,2,9:5:0,9:5:0",
			"LE1,14,3,9:10:0,9:10:0",
			"LE1,3a,4,9:15:0,9:15:0",
			"LW1,3a,1,9:20:0,9:20:0",
			"LW1,14,2,9:25:0,9:25:0",
			"LW1,6a,3,9:30:0,9:30:0",
			"LW1,8a,4,9:35:0,9:35:0",
			"LE2,8a,1,9:1:0,9:1:0",
			"LE2,6a,2,9:6:0,9:6:0",
			"LE2,14,3,9:11:0,9:11:0",
			"LE2,3a,4,9:16:0,9:16:0",
		},
	})

	// Feb 14th is a Friday, so weekday schedule applies.
	departures, err := g.Departures("6a", time.Date(2020, 2, 14, 9, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "6a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 14, 9, 5, 0, 0, time.UTC),
		},
	}, departures)

	// Feb 15th will be on weekend schedule
	departures, err = g.Departures("6a", time.Date(2020, 2, 15, 9, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "6a",
			RouteID:      "L",
			TripID:       "LE2",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 15, 9, 6, 0, 0, time.UTC),
		},
	}, departures)

	// Window spanning from 14th into 15th can capture stops from both days
	departures, err = g.Departures("6a", time.Date(2020, 2, 14, 9, 29, 0, 0, time.UTC), 24*time.Hour-1*time.Second, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "6a",
			RouteID:      "L",
			TripID:       "LW1",
			StopSequence: 3,
			DirectionID:  1,
			Time:         time.Date(2020, 2, 14, 9, 30, 0, 0, time.UTC),
		},
		{
			StopID:       "6a",
			RouteID:      "L",
			TripID:       "LE2",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 15, 9, 6, 0, 0, time.UTC),
		},
	}, departures)

}

func testStaticDeparturesTimezones(t *testing.T, backend string) {
	g := testutil.BuildStatic(t, backend, map[string][]string{
		// Eastern Time
		"agency.txt": {"agency_timezone,agency_name,agency_url", "America/New_York,MTA,http://example.com"},
		// Mondays only!
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"mondays,20200101,20201231,1,0,0,0,0,0,0",
		},
		"routes.txt": {"route_id,route_short_name,route_type", "L,l,1"},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"8a,8a,1,1",
			"6a,6a,2,2",
			"14,14,3,3",
			"3a,3a,4,4",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"LE1,L,mondays,0",
		},
		// The trips stop at all 4 stations
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"LE1,8a,1,9:0:0,9:0:0",
			"LE1,6a,2,9:5:0,9:5:0",
			"LE1,14,3,9:10:0,9:10:0",
			"LE1,3a,4,9:15:0,9:15:0",
		},
	})

	tzNYC, _ := time.LoadLocation("America/New_York")

	// Querying using the transit agency's time zone
	departures, err := g.Departures("6a", time.Date(2020, 2, 3, 9, 0, 0, 0, tzNYC), 20*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "6a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 3, 9, 5, 0, 0, tzNYC),
		},
	}, departures)

	// Querying using UTC, which in February 2020 is NYC+5
	departures, err = g.Departures("6a", time.Date(2020, 2, 3, 14, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "6a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 3, 14, 5, 0, 0, time.UTC),
		},
	}, departures)

	// This also works if we query for the preceding day, with a
	// large enough window
	departures, err = g.Departures("6a", time.Date(2020, 2, 2, 22, 0, 0, 0, time.UTC), 20*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "6a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 3, 14, 5, 0, 0, time.UTC),
		},
	}, departures)
}

func testStaticDeparturesOvernightTrip(t *testing.T, backend string) {
	g := testutil.BuildStatic(t, backend, map[string][]string{
		"agency.txt": {"agency_timezone,agency_name,agency_url", "America/New_York,MTA,http://example.com"},
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"weekend,20200101,20201231,0,0,0,0,0,1,1",
		},
		"routes.txt": {"route_id,route_short_name,route_type", "L,l,0"},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"8a,8a,1,2",
			"6a,6a,1,2",
			"14,14,1,2",
			"3a,3a,1,2",
			"1a,1a,1,2",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"LE1,L,weekend,0",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"LE1,8a,1,23:00:0,23:00:0",
			"LE1,6a,2,23:30:0,23:30:0",
			"LE1,14,3,24:00:0,24:00:0",
			"LE1,3a,4,24:30:0,24:30:0",
			"LE1,1a,5,24:35:0,24:35:0",
		},
	})

	tzNYC, _ := time.LoadLocation("America/New_York")

	// Feb 9th is a Sunday. 3rd ave stop falls 00:30 on the 10th,
	// but is still part of the feb 9 trip.
	departures, err := g.Departures("3a", time.Date(2020, 2, 9, 23, 30, 0, 0, tzNYC), 2*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "3a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 4,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 10, 0, 30, 0, 0, tzNYC),
		},
	}, departures)

	// It's also there if we query for departures on the 10th
	departures, err = g.Departures("3a", time.Date(2020, 2, 10, 0, 15, 0, 0, tzNYC), 20*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "3a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 4,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 10, 0, 30, 0, 0, tzNYC),
		},
	}, departures)

	// This works when we query with different timezone (UTC is
	// NYC+5)
	departures, err = g.Departures("3a", time.Date(2020, 2, 10, 4, 30, 0, 0, time.UTC), 2*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "3a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 4,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 10, 5, 30, 0, 0, time.UTC),
		},
	}, departures)
	departures, err = g.Departures("3a", time.Date(2020, 2, 10, 5, 15, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "3a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 4,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 10, 5, 30, 0, 0, time.UTC),
		},
	}, departures)

}

func testStaticDeparturesCalendarDateOverride(t *testing.T, backend string) {
	g := testutil.BuildStatic(t, backend, map[string][]string{
		"agency.txt": {"agency_timezone,agency_name,agency_url", "America/New_York,MTA,http://example.com"},
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"weekend,20200101,20201231,0,0,0,0,0,1,1",
		},
		"routes.txt": {"route_id,route_short_name,route_type", "L,L,4"},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"8a,8a,1,1",
			"6a,6a,2,2",
			"14,14,3,3",
			"3a,3a,4,4",
			"1a,1a,5,5",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"LE1,L,weekend,0",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"LE1,8a,1,23:00:0,23:00:0",
			"LE1,6a,2,23:30:0,23:30:0",
			"LE1,14,3,24:00:0,24:00:0",
			"LE1,3a,4,24:30:0,24:30:0",
			"LE1,1a,5,24:35:0,24:35:0",
		},
		// This removes service from Saturday the 8th and
		// Sunday the 16th. It adds service on Monday the
		// 24th.
		"calendar_dates.txt": {
			"service_id,date,exception_type",
			"weekend,20200208,2",
			"weekend,20200216,2",
			"weekend,20200224,1",
		},
	})

	tzNYC, _ := time.LoadLocation("America/New_York")

	// The 9th is still running, but the trips from the 8th
	// (including the ones spilling over into the 9th) are
	// disabled.
	departures, err := g.Departures("8a", time.Date(2020, 2, 9, 22, 0, 0, 0, tzNYC), 2*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "8a",
			RouteID:      "L",
			TripID:       "LE1",
			DirectionID:  0,
			StopSequence: 1,
			Time:         time.Date(2020, 2, 9, 23, 0, 0, 0, tzNYC)},
	}, departures)
	departures, err = g.Departures("8a", time.Date(2020, 2, 8, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = g.Departures("3a", time.Date(2020, 2, 8, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)

	// The trips from the 16th are also disabled, including spill
	// over into the 17th. The 15th is still up though, including
	// spill over into the 16th.
	departures, err = g.Departures("8a", time.Date(2020, 2, 16, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = g.Departures("3a", time.Date(2020, 2, 16, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)

	departures, err = g.Departures("8a", time.Date(2020, 2, 15, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "8a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 1,
			Time:         time.Date(2020, 2, 15, 23, 0, 0, 0, tzNYC),
		},
	}, departures)
	departures, err = g.Departures("3a", time.Date(2020, 2, 15, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "3a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 4,
			Time:         time.Date(2020, 2, 16, 0, 30, 0, 0, tzNYC),
		},
	}, departures)

	// The added Monday the 24th is enabled, including spill over
	// into the the 25th. 25th remains disabled.
	departures, err = g.Departures("8a", time.Date(2020, 2, 24, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "8a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 1,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 24, 23, 0, 0, 0, tzNYC)},
	}, departures)
	departures, err = g.Departures("3a", time.Date(2020, 2, 24, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "3a",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 4,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 25, 0, 30, 0, 0, tzNYC)},
	}, departures)
	departures, err = g.Departures("8a", time.Date(2020, 2, 25, 22, 0, 0, 0, tzNYC), 5*time.Hour, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)
}

// Real world schedules seem to provide departure_time for all stops,
// including the final stop on a trip. The spec doesn't seem to say
// much about this, but we have to make sure we're not interpreting
// these as actual departures. If the vehicle leaves it'll be on a
// different trip, possibly with the same block_id (which we're not
// using at the time of this writing).
func testStaticDeparturesNoDepartureFromFinalStop(t *testing.T, backend string) {

	g := testutil.BuildStatic(t, backend, map[string][]string{
		"agency.txt": {"agency_timezone,agency_name,agency_url", "America/New_York,MTA,http://example.com"},
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"everyday,20200101,20201231,1,1,1,1,1,1,1",
		},
		"routes.txt": {"route_id,route_long_name,route_type", "L,The L,1"},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"8a,8a,1,1",
			"6a,6a,2,2",
			"14,14,3,3",
			"3a,3a,4,4",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"LE1,L,everyday,0",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"LE1,8a,1,23:00:0,23:00:0",
			"LE1,6a,2,23:30:0,23:30:0",
			"LE1,14,3,24:00:0,24:00:0",
			"LE1,3a,4,24:30:0,24:30:0",
		},
	})

	tzNYC, _ := time.LoadLocation("America/New_York")

	departures, err := g.Departures("14", time.Date(2020, 2, 9, 23, 0, 0, 0, tzNYC), 2*time.Hour, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(departures))

	assert.Equal(t, []model.Departure{
		{
			StopID:       "14",
			RouteID:      "L",
			TripID:       "LE1",
			StopSequence: 3,
			DirectionID:  0,
			Time:         time.Date(2020, 2, 10, 0, 0, 0, 0, tzNYC)},
	}, departures)

	departures, err = g.Departures("3a", time.Date(2020, 2, 9, 23, 0, 0, 0, tzNYC), 2*time.Hour, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)
}

func testStaticDeparturesFiltering(t *testing.T, backend string) {
	// This weekend schedule has RouteA running alpha-beta-gamma
	// and gamma-beta-alpha a few times per day. Route B does a
	// single run beta-epsilon-gamma.
	g := testutil.BuildStatic(t, backend, map[string][]string{
		"agency.txt": {"agency_timezone,agency_name,agency_url", "America/New_York,MTA,http://example.com"},
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"weekend,20200101,20201231,0,0,0,0,0,1,1",
		},
		"routes.txt": {"route_id,route_short_name,route_type", "RouteA,a,0", "RouteB,a,0"},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"alpha,alpha,1,1",
			"beta,beta,2,2",
			"gamma,gamma,3,3",
			"delta,delta,4,4",
			"epsilon,epsilon,5,5",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id",
			"A1,RouteA,weekend,0",
			"A2,RouteA,weekend,0",
			"A3,RouteA,weekend,0",
			"a1,RouteA,weekend,1",
			"a2,RouteA,weekend,1",
			"a3,RouteA,weekend,1",
			"B1,RouteB,weekend,0",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"A1,alpha,1,5:30:0,5:30:0",
			"A1,beta,2,6:0:0,6:0:0",
			"A1,gamma,3,6:30:0,6:30:0",
			"A2,alpha,1,12:30:0,12:30:0",
			"A2,beta,2,13:0:0,13:0:0",
			"A2,gamma,3,13:30:0,13:30:0",
			"A3,alpha,1,23:30:0,23:30:0",
			"A3,beta,2,24:0:0,24:0:0",
			"A3,gamma,3,24:30:0,24:30:0",
			"a1,gamma,1,5:31:0,5:31:0",
			"a1,beta,2,6:1:0,6:1:0",
			"a1,alpha,3,6:31:0,6:31:0",
			"a2,gamma,1,12:31:0,12:31:0",
			"a2,beta,2,13:1:0,13:1:0",
			"a2,alpha,3,13:31:0,13:31:0",
			"a3,gamma,1,23:31:0,23:31:0",
			"a3,beta,2,24:1:0,24:1:0",
			"a3,alpha,3,24:31:0,24:31:0",
			"B1,beta,1,11:0:0,11:0:0",
			"B1,epsilon,2,11:30:0,11:30:0",
			"B1,gamma,3,12:0:0,12:0:0",
		},
	})

	tzNYC, _ := time.LoadLocation("America/New_York")
	longDuration := 100 * 24 * time.Hour

	// March 14th was a Saturday
	// Departures from alpha, in any direction, on any route
	departures, err := g.Departures("alpha", time.Date(2020, 3, 14, 0, 0, 0, 0, tzNYC), longDuration, 1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(departures))
	assert.Equal(t, []model.Departure{
		{
			StopID:       "alpha",
			RouteID:      "RouteA",
			TripID:       "A1",
			StopSequence: 1,
			DirectionID:  0,
			Time:         time.Date(2020, 3, 14, 5, 30, 0, 0, tzNYC)},
	}, departures)

	// Specifying non-existent route and/or direction -> no results
	departures, err = g.Departures("alpha", time.Date(2020, 3, 14, 0, 0, 0, 0, tzNYC), longDuration, 1, "", 1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = g.Departures("alpha", time.Date(2020, 3, 14, 0, 0, 0, 0, tzNYC), longDuration, 1, "RouteC", 0, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = g.Departures("alpha", time.Date(2020, 3, 14, 0, 0, 0, 0, tzNYC), longDuration, 1, "RouteC", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)

	// The beta stop has departures in 2 direction
	departures, err = g.Departures("beta", time.Date(2020, 3, 14, 0, 0, 0, 0, tzNYC), longDuration, 1, "", 0, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "beta",
			RouteID:      "RouteA",
			TripID:       "A1",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 3, 14, 6, 0, 0, 0, tzNYC)},
	}, departures)

	departures, err = g.Departures("beta", time.Date(2020, 3, 14, 0, 0, 0, 0, tzNYC), longDuration, 1, "", 1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "beta",
			RouteID:      "RouteA",
			TripID:       "a1",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 3, 14, 6, 1, 0, 0, tzNYC)},
	}, departures)

	// Pushing start time back discards earlier departures
	departures, err = g.Departures("beta", time.Date(2020, 3, 14, 12, 0, 0, 0, tzNYC), longDuration, 1, "", 0, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "beta",
			RouteID:      "RouteA",
			TripID:       "A2",
			StopSequence: 2,
			DirectionID:  0,
			Time:         time.Date(2020, 3, 14, 13, 0, 0, 0, tzNYC)},
	}, departures)
	departures, err = g.Departures("beta", time.Date(2020, 3, 14, 12, 0, 0, 0, tzNYC), longDuration, 1, "RouteA", 1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			StopID:       "beta",
			RouteID:      "RouteA",
			TripID:       "a2",
			StopSequence: 2,
			DirectionID:  1,
			Time:         time.Date(2020, 3, 14, 13, 1, 0, 0, tzNYC)},
	}, departures)

	// Requesting a whole lot of departures results in a whole lot of departures
	departures, err = g.Departures("alpha", time.Date(2020, 3, 14, 0, 0, 0, 0, tzNYC), longDuration, 9, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 9, len(departures))
	assert.Equal(t, time.Date(2020, 3, 14, 5, 30, 0, 0, tzNYC), departures[0].Time)
	assert.Equal(t, time.Date(2020, 3, 14, 12, 30, 0, 0, tzNYC), departures[1].Time)
	assert.Equal(t, time.Date(2020, 3, 14, 23, 30, 0, 0, tzNYC), departures[2].Time)
	assert.Equal(t, time.Date(2020, 3, 15, 5, 30, 0, 0, tzNYC), departures[3].Time)
	assert.Equal(t, time.Date(2020, 3, 15, 12, 30, 0, 0, tzNYC), departures[4].Time)
	assert.Equal(t, time.Date(2020, 3, 15, 23, 30, 0, 0, tzNYC), departures[5].Time)
	assert.Equal(t, time.Date(2020, 3, 21, 5, 30, 0, 0, tzNYC), departures[6].Time)
	assert.Equal(t, time.Date(2020, 3, 21, 12, 30, 0, 0, tzNYC), departures[7].Time)
	assert.Equal(t, time.Date(2020, 3, 21, 23, 30, 0, 0, tzNYC), departures[8].Time)
}

// Headsign can be set on trips, and on stop_times. The latter
// overrides the former.
func testStaticDeparturesStopTimeWithHeadsignOverride(t *testing.T, backend string) {
	// A single trip on Mondays, going through the alphabet
	g := testutil.BuildStatic(t, backend, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"mondays,20200101,20201231,1,0,0,0,0,0,0",
		},
		"routes.txt": {"route_id,route_short_name,route_type", "alpha,alpha,3"},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"A,A,1,2",
			"B,B,1,1",
			"C,C,2,2",
			"D,D,3,3",
			"E,E,4,4",
			"F,F,5,5",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id,trip_headsign",
			"alphabet,alpha,mondays,0,To Z",
		},
		"stop_times.txt": {
			"trip_id,stop_id,departure_time,arrival_time,stop_headsign,stop_sequence",
			"alphabet,A,6:10:0,6:10:0,,1",
			"alphabet,B,6:11:0,6:11:0,,2",
			"alphabet,C,6:12:0,6:12:0,,3",
			"alphabet,D,6:13:0,6:13:0,To F,4",
			"alphabet,E,6:14:0,6:14:0,To F,5",
			"alphabet,F,6:14:0,6:14:0,To nowhere,6",
		},
	})

	// Feb 3rd is a Monday.
	for _, test := range []struct {
		StopID           string
		ExpectedHeadsign string
	}{
		{"A", "To Z"},
		{"B", "To Z"},
		{"C", "To Z"},
		{"D", "To F"},
		{"E", "To F"},
	} {
		departures, err := g.Departures(
			test.StopID,
			time.Date(2020, 2, 3, 6, 0, 0, 0, time.UTC),
			30*time.Minute,
			-1,
			"",
			-1,
			nil,
		)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(departures))
		assert.Equal(t, test.StopID, departures[0].StopID)
		assert.Equal(t, test.ExpectedHeadsign, departures[0].Headsign)
	}

	// And nothing departs from F
	departures, err := g.Departures("F", time.Date(2020, 2, 3, 6, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(departures))
}

// Verifies that departures can be retrieved both for individual stops
// and for their parent stations (if any)
func testStaticDeparturesWithParentStations(t *testing.T, backend string) {
	g := testutil.BuildStatic(t, backend, map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"mondays,20200101,20201231,1,0,0,0,0,0,0",
		},
		"routes.txt": {"route_id,route_short_name,route_type", "alpha,a,0"},
		"stops.txt": {
			"stop_id,stop_name,location_type,parent_station,stop_lat,stop_lon",
			"a,a,1,,1,1",  // a is a station
			"A,A,0,a,2,2", // A is a stop at a
			"B,B,0,,3,3",  // B is a stop, without parent
			"c,c,1,,4,4",  // c is a station
			"C,C,0,c,5,5", // C is a stop at c
			"d,d,1,,6,6",  // d is a station
			"D,D,0,d,7,7", // D is a stop at d
			"E,E,0,,8,8",  // E is a stop, without parent
		},
		"trips.txt": {
			"trip_id,route_id,service_id,direction_id,trip_headsign",
			"alphabet,alpha,mondays,0,To Z",
		},
		"stop_times.txt": {
			"trip_id,stop_id,departure_time,arrival_time,stop_sequence",
			"alphabet,A,6:10:0,6:10:0,1",
			"alphabet,B,6:11:0,6:11:0,2",
			"alphabet,C,6:12:0,6:12:0,3",
			"alphabet,D,6:13:0,6:13:0,4",
			"alphabet,E,6:14:0,6:14:0,5",
		},
	})

	getDeps := func(stopID string) []model.Departure {
		// Feb 3rd is a Monday.
		departures, err := g.Departures(
			stopID,
			time.Date(2020, 2, 3, 6, 0, 0, 0, time.UTC),
			30*time.Minute,
			-1, "", -1, nil,
		)
		assert.NoError(t, err)
		return departures
	}

	// IDentical result hitting parent stations or individual stop
	assert.Equal(t, getDeps("A"), getDeps("a"))
	assert.Equal(t, getDeps("C"), getDeps("c"))
	assert.Equal(t, getDeps("D"), getDeps("d"))

	assert.Equal(t, "A", getDeps("A")[0].StopID)
	assert.Equal(t, "B", getDeps("B")[0].StopID)
	assert.Equal(t, "C", getDeps("C")[0].StopID)
	assert.Equal(t, "D", getDeps("D")[0].StopID)
}

func testStaticDeparturesDaylightsSavings(t *testing.T, backend string) {
	// Two trips, one early in morning, one so late that it
	// overflows into next day. Each on a separate route.
	g := testutil.BuildStatic(t, backend, map[string][]string{
		"agency.txt": {
			"agency_id,agency_name,agency_url,agency_timezone",
			"1,MTA,http://mta.com/,America/New_York",
		},
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"all,20200101,20201231,1,1,1,1,1,1,1",
		},
		"routes.txt": {
			"route_id,route_long_name,route_type",
			"L_early,The L Early,3",
			"L_late,The L Late,3",
		},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"8av,8th Avenue,40.739777,-74.002578",
			"6av,6th Avenue,40.737741,-73.996948",
			"14st,14th Street,40.738228,-73.996209",
			"3av,3rd Avenue,40.732849,-73.989433",
			"1av,1st Avenue,40.730953,-73.981628",
			"bedford,Bedford Avenue,40.717304,-73.956872",
		},
		"trips.txt": {
			"trip_id,route_id,service_id,trip_headsign,direction_id,block_id",
			"L_early,L_early,all,8av,0,1",
			"L_late,L_late,all,8av,0,1",
		},
		"stop_times.txt": {
			"trip_id,arrival_time,departure_time,stop_id,stop_sequence",
			"L_early,00:00:00,00:00:00,8av,1",
			"L_early,01:00:00,01:00:00,6av,2",
			"L_early,02:00:00,02:00:00,14st,3",
			"L_early,03:00:00,03:00:00,3av,4",
			"L_early,04:00:00,04:00:00,1av,5",
			"L_early,05:00:00,05:00:00,bedford,6",
			"L_late,24:00:00,24:00:00,8av,1",
			"L_late,25:00:00,25:00:00,6av,2",
			"L_late,26:00:00,26:00:00,14st,3",
			"L_late,27:00:00,27:00:00,3av,4",
			"L_late,28:00:00,28:00:00,1av,5",
			"L_late,29:00:00,29:00:00,bedford,6",
		},
	})

	// Stuff can get weird around transition to and from daylight
	// savings time. Times gets "pushed" 1h in some direction on
	// day of the switch, as noon-12+time in the new "timezone" is
	// different.

	// In 2020, DST begun March 8 (at 2am, clocks went forward to
	// 3am), and ended November 1 (at 2am, clocks went back to
	// 1am).

	for _, tc := range []struct {
		name    string
		routeID string
		stopID  string
		time    string
	}{
		// Early trip going into DST
		{
			name:    "L_early way before DST begins",
			routeID: "L_early",
			stopID:  "6av",
			time:    "2020-03-08 00:00:00 -0500 EST",
		},
		{
			name:    "L_early before DST begins",
			routeID: "L_early",
			stopID:  "14st",
			time:    "2020-03-08 01:00:00 -0500 EST",
		},
		{
			name:    "L_early as DST begins",
			routeID: "L_early",
			stopID:  "3av",
			time:    "2020-03-08 03:00:00 -0400 EDT",
		},
		{
			name:    "L_early after DST begins",
			routeID: "L_early",
			stopID:  "1av",
			time:    "2020-03-08 04:00:00 -0400 EDT",
		},

		// Late trip going into DST
		{
			name:    "L_late before DST begins",
			routeID: "L_late",
			stopID:  "6av",
			time:    "2020-03-08 01:00:00 -0500 EST",
		},
		{
			name:    "late as DST begins",
			routeID: "L_late",
			stopID:  "14st",
			time:    "2020-03-08 03:00:00 -0400 EDT",
		},
		{
			name:    "late after DST begins",
			routeID: "L_late",
			stopID:  "3av",
			time:    "2020-03-08 04:00:00 -0400 EDT",
		},

		// Early trip going out of DST
		{
			name:    "early before DST ends",
			routeID: "L_early",
			stopID:  "8av",
			time:    "2020-11-01 01:00:00 -0400 EDT",
		},
		{
			name:    "early as DST ends",
			routeID: "L_early",
			stopID:  "6av",
			time:    "2020-11-01 01:00:00 -0500 EST",
		},
		{
			name:    "early after DST ends",
			routeID: "L_early",
			stopID:  "14st",
			time:    "2020-11-01 02:00:00 -0500 EST",
		},

		// Late trip going out of DST
		{
			name:    "late before DST ends",
			routeID: "L_late",
			stopID:  "6av",
			time:    "2020-11-01 01:00:00 -0400 EDT",
		},
		{
			name:    "late as DST ends",
			routeID: "L_late",
			stopID:  "14st",
			time:    "2020-11-01 01:00:00 -0500 EST",
		},
		{
			name:    "late after DST ends",
			routeID: "L_late",
			stopID:  "3av",
			time:    "2020-11-01 02:00:00 -0500 EST",
		},
	} {
		depTime, err := time.Parse("2006-01-02 15:04:05 -0700 MST", tc.time)
		require.NoError(t, err)

		departures, err := g.Departures(
			tc.stopID,
			depTime.Add(-1*time.Minute),
			2*time.Minute, -1, tc.routeID, -1, nil,
		)
		require.NoError(t, err)
		require.Equal(t, 1, len(departures), tc.name)
		require.Equal(t, depTime, departures[0].Time, tc.name)
	}

}

func TestStatic(t *testing.T) {
	for _, test := range []struct {
		Name string
		Test func(t *testing.T, storage string)
	}{
		{"StaticDeparturesWindowing", testStaticDeparturesWindowing},
		{"StaticDeparturesWeekendSchedule", testStaticDeparturesWeekendSchedule},
		{"StaticDeparturesTimezones", testStaticDeparturesTimezones},
		{"StaticDeparturesOvernightTrip", testStaticDeparturesOvernightTrip},
		{"StaticDeparturesCalendarDateOverride", testStaticDeparturesCalendarDateOverride},
		{"StaticDeparturesNoDepartureFromFinalStop", testStaticDeparturesNoDepartureFromFinalStop},
		{"StaticDeparturesFiltering", testStaticDeparturesFiltering},
		{"StaticDeparturesStopTimeWithHeadsignOverride", testStaticDeparturesStopTimeWithHeadsignOverride},
		{"StaticDeparturesWithParentStations", testStaticDeparturesWithParentStations},
		{"StaticDeparturesDaylightsSavings", testStaticDeparturesDaylightsSavings},
	} {
		t.Run(fmt.Sprintf("%s SQLite", test.Name), func(t *testing.T) {
			test.Test(t, "sqlite")
		})
		if testutil.PostgresConnStr != "" {
			t.Run(fmt.Sprintf("%s Postgres", test.Name), func(t *testing.T) {
				test.Test(t, "postgres")
			})
		}
	}
}
