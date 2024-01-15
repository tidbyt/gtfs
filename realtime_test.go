package gtfs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"

	"tidbyt.dev/gtfs"
	"tidbyt.dev/gtfs/model"
	p "tidbyt.dev/gtfs/proto"
	"tidbyt.dev/gtfs/testutil"
)

// Helpers for building gtfs-realtime feeds
type StopUpdate struct {
	ArrivalSet     bool
	ArrivalDelay   int32
	ArrivalTime    time.Time
	DepartureSet   bool
	DepartureDelay int32
	DepartureTime  time.Time
	StopID         string
	StopSequence   uint32
	SchedRel       string
}

type TripUpdate struct {
	TripID      string
	StopUpdates []StopUpdate
	Canceled    bool
}

func buildFeed(t *testing.T, tripUpdates []TripUpdate) [][]byte {
	entity := make([]*p.FeedEntity, 0, len(tripUpdates))

	for _, tripUpdate := range tripUpdates {
		stopTimeUpdate := make([]*p.TripUpdate_StopTimeUpdate, 0, len(tripUpdate.StopUpdates))

		for _, stopUpdate := range tripUpdate.StopUpdates {
			var scheduleRelationship p.TripUpdate_StopTimeUpdate_ScheduleRelationship
			switch stopUpdate.SchedRel {
			case "SKIPPED":
				scheduleRelationship = p.TripUpdate_StopTimeUpdate_SKIPPED
			case "NO_DATA":
				scheduleRelationship = p.TripUpdate_StopTimeUpdate_NO_DATA
			case "":
				fallthrough
			case "SCHEDULED":
				scheduleRelationship = p.TripUpdate_StopTimeUpdate_SCHEDULED
			default:
				t.Fatal(fmt.Sprintf("bad SchedRel: %s", stopUpdate.SchedRel))
			}

			stup := &p.TripUpdate_StopTimeUpdate{
				ScheduleRelationship: &scheduleRelationship,
				StopSequence:         proto.Uint32(stopUpdate.StopSequence),
				StopId:               proto.String(stopUpdate.StopID),
			}
			if stopUpdate.DepartureSet {
				departureTime := int64(0)
				if !stopUpdate.DepartureTime.IsZero() {
					departureTime = stopUpdate.DepartureTime.Unix()
				}
				stup.Departure = &p.TripUpdate_StopTimeEvent{
					Delay: proto.Int32(stopUpdate.DepartureDelay),
					Time:  proto.Int64(departureTime),
				}
			}
			if stopUpdate.ArrivalSet {
				arrivalTime := int64(0)
				if !stopUpdate.ArrivalTime.IsZero() {
					arrivalTime = stopUpdate.ArrivalTime.Unix()
				}
				stup.Arrival = &p.TripUpdate_StopTimeEvent{
					Delay: proto.Int32(stopUpdate.ArrivalDelay),
					Time:  proto.Int64(arrivalTime),
				}
			}

			stopTimeUpdate = append(stopTimeUpdate, stup)
		}

		tripScheduleRelationship := p.TripDescriptor_SCHEDULED
		if tripUpdate.Canceled {
			tripScheduleRelationship = p.TripDescriptor_CANCELED
		}
		entity = append(entity, &p.FeedEntity{
			Id: proto.String(tripUpdate.TripID),
			TripUpdate: &p.TripUpdate{
				Trip: &p.TripDescriptor{
					TripId:               proto.String(tripUpdate.TripID),
					ScheduleRelationship: &tripScheduleRelationship,
				},
				StopTimeUpdate: stopTimeUpdate,
			},
		})
	}

	incrementality := p.FeedHeader_FULL_DATASET
	timestamp := uint64(time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC).Unix())
	header := &p.FeedHeader{
		GtfsRealtimeVersion: proto.String("2.0"),
		Incrementality:      &incrementality,
		Timestamp:           proto.Uint64(timestamp),
	}

	feed := &p.FeedMessage{Header: header, Entity: entity}

	data, err := proto.Marshal(feed)
	require.NoError(t, err)
	if err != nil {
		panic(err)
	}

	return [][]byte{data}
}

// A simple Static fixture. Trips t1 and t2 cover the same three
// stops s1-s3. Trip t3 covers z1-z2. Full service all days of 2020.
func SimpleStaticFixture(t *testing.T) *gtfs.Static {
	static := testutil.BuildStatic(t, "sqlite", map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"everyday,20200101,20210101,1,1,1,1,1,1,1",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"R1,R_1,1",
			"R2,R_2,1",
		},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"s1,S1,1,1",
			"s2,S2,2,3",
			"s3,S3,4,5",
			"s4,S4,6,7",
			"z1,Z1,8,9",
			"z2,Z2,10,11",
			"z3,Z3,12,13",
		},
		"trips.txt": {
			"service_id,trip_id,route_id",
			"everyday,t1,R1",
			"everyday,t2,R1",
			"everyday,t3,R2",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"t1,s1,1,23:0:0,23:0:0",
			"t1,s2,2,23:1:0,23:1:0",
			"t1,s3,3,23:2:0,23:2:0",
			"t1,s4,4,23:3:0,23:3:0",
			"t2,s1,1,23:10:0,23:10:0",
			"t2,s2,2,23:11:0,23:11:0",
			"t2,s3,3,23:12:0,23:12:0",
			"t2,s4,4,23:13:0,23:13:0",
			"t3,z1,1,23:5:0,23:5:0",
			"t3,z2,2,23:6:0,23:6:0",
			"t3,z3,3,23:7:0,23:7:0",
		},
	})

	return static
}

// Test realtime data where updates all have 0 delay.
func TestRealtimeNoChanges(t *testing.T) {

	// This realtime feed has updates for stops on trip t1, but
	// none of them modify the departure time from what's already
	// scheduled.
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s2", // identify by stop_id
					DepartureSet:   true,
					DepartureDelay: 0,
				},
				{
					StopSequence:  3, // identify by stop_sequence
					DepartureSet:  true,
					DepartureTime: time.Date(2020, 1, 15, 23, 2, 0, 0, time.UTC),
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)
	assert.Equal(t, uint64(time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC).Unix()), rt.Timestamp)

	// Check s1
	departures, err := rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 10, 0, 0, time.UTC),
		},
	}, departures)

	// Check s2
	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 11, 0, 0, time.UTC),
		},
	}, departures)

	// Check s3
	departures, err = rt.Departures("s3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 2, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 12, 0, 0, time.UTC),
		},
	}, departures)

	// No departures from s4 since it's the final stop
	departures, err = rt.Departures("s4", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)

	// And z1 for good measure. This one definitely shouldn't have
	// changed.
	departures, err = rt.Departures("z1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R2",
			TripID:       "t3",
			StopID:       "z1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 5, 0, 0, time.UTC),
		},
	}, departures)

	// And no departures from z3 since it's the final stop
	departures, err = rt.Departures("z3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)
}

// Realtime data where updates are expected to propagate along a trip
func TestRealtimeDelayWithPropagation(t *testing.T) {

	// Delays on s2! For trip t1 there are no updates for s3, so
	// s1 delay should propagate. For trip t2, train is expected
	// to catch up fully before s3.
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s2",
					DepartureSet:   true,
					DepartureDelay: 30, // 30 second delay
				},
			},
		},
		{
			TripID: "t2",
			StopUpdates: []StopUpdate{
				{
					SchedRel:      "SCHEDULED",
					StopSequence:  2,
					DepartureSet:  true,
					DepartureTime: time.Date(2020, 1, 15, 23, 11, 45, 0, time.UTC), // 45s delay
				},
				{
					StopID: "s3", // no delay
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	// Check s1
	departures, err := rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 10, 0, 0, time.UTC),
		},
	}, departures)

	// Check s2. Expecting delays on both trips.
	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 11, 45, 0, time.UTC),
			Delay:        45 * time.Second,
		},
	}, departures)

	// Check s3. Expecting delay on t1, but t2 back on schedule.
	departures, err = rt.Departures("s3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 2, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 12, 0, 0, time.UTC),
		},
	}, departures)
}

// Verifies that StopTimeUpdate with NO_DATA are handled correctly.
func TestRealtimeNoData(t *testing.T) {

	// This realtime feed has NO_DATA for t1-s1, which means the
	// provided Time (which is really a violation of spec) is
	// ignored and the static scheduled is used instead. Then at
	// t1-s2, a real delay appears, which should be propagated to
	// t1-s3 since no later update contradicts it.
	// For trip 2 we have a delay at s1
	feed := buildFeed(t, []TripUpdate{
		{
			// For trip t1 we've NO_DATA at s1, which
			// means the provided departure time (which
			// strictly speaking is a violation of spec)
			// should be ignored and the static schedule
			// is used instead. At s2, there is a 30s
			// delay, which should be propagated to s3 as
			// well.
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					SchedRel:      "NO_DATA",
					StopID:        "s1",
					DepartureSet:  true,
					DepartureTime: time.Date(1212, 12, 12, 12, 12, 12, 12, time.UTC),
				},
				{
					SchedRel:       "SCHEDULED",
					StopID:         "s2",
					DepartureSet:   true,
					DepartureDelay: 30,
				},
			},
		},
		{
			// On t2 we have a delay at s1. The NO_DATA
			// record should make s2 and s3 fall back to
			// static schedule, i.e. delay propagation
			// halts.
			TripID: "t2",
			StopUpdates: []StopUpdate{
				{
					StopSequence:  1,
					DepartureSet:  true,
					DepartureTime: time.Date(2020, 1, 15, 23, 10, 45, 0, time.UTC), // 45s delay
				},
				{
					SchedRel:     "NO_DATA",
					StopID:       "s2", // no delay
					DepartureSet: true,
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	// Check s1
	departures, err := rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 10, 45, 0, time.UTC),
			Delay:        45 * time.Second,
		},
	}, departures)

	// Check s2
	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s2",
			StopSequence: 2,
			// delay not propagated due to NO_DATA
			Time: time.Date(2020, 1, 15, 23, 11, 0, 0, time.UTC),
		},
	}, departures)

	// Check s3
	departures, err = rt.Departures("s3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s3",
			StopSequence: 3,
			// delay propagated from s2
			Time:  time.Date(2020, 1, 15, 23, 2, 30, 0, time.UTC),
			Delay: 30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s3",
			StopSequence: 3,
			// and t2 remains on schedule due to NO_DATA at s2
			Time: time.Date(2020, 1, 15, 23, 12, 0, 0, time.UTC),
		},
	}, departures)
}

// Verifies that StopTimeUpdate with SKIPPED stops work.
func TestRealtimeSkippedStop(t *testing.T) {
	feed := buildFeed(t, []TripUpdate{
		{
			// Trip t1 skips stops s1 and s3. A delay is
			// included at s1, but is ignored.
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					SchedRel:       "SKIPPED",
					StopID:         "s1",
					DepartureSet:   true,
					DepartureDelay: 120,
				},
				{
					SchedRel:     "SKIPPED",
					StopSequence: 3,
				},
			},
		},
		{
			// On t2 we have a delay at s1 and a skip at
			// s2. The delay from s1 should be propagated
			// to s3.
			TripID: "t2",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s1",
					DepartureSet:   true,
					DepartureDelay: 30,
				},
				{
					SchedRel: "SKIPPED",
					StopID:   "s2",
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	// Check s1. Expect t1 to skip past. t2 is delayed 30s.
	departures, err := rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 10, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	// Check s2. Expect t2 to skip. t1 is on time.
	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 0, 0, time.UTC),
		},
	}, departures)

	// Check s3. Expect t1 to skip, and t2 to remain 30s delayed.
	departures, err = rt.Departures("s3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 12, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)
}

// Verifies that trip cancellations work.
func TestRealtimeCanceledTrip(t *testing.T) {

	// Trip t1 is canceled, t2 runs with a delay from s2.
	feed := buildFeed(t, []TripUpdate{
		{
			TripID:   "t1",
			Canceled: true,
		},
		{
			TripID: "t2",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s2",
					DepartureSet:   true,
					DepartureDelay: 30,
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	departures, err := rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 10, 0, 0, time.UTC),
		},
	}, departures)

	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 11, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	departures, err = rt.Departures("s3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 12, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	departures, err = rt.Departures("z1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R2",
			TripID:       "t3",
			StopID:       "z1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 5, 0, 0, time.UTC),
		},
	}, departures)

	// All trips are canceled!
	feed = buildFeed(t, []TripUpdate{
		{
			TripID:   "t1",
			Canceled: true,
		},
		{
			TripID:   "t2",
			Canceled: true,
		},
		{
			TripID:   "t3",
			Canceled: true,
		},
	})
	rt, err = gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	departures, err = rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = rt.Departures("s31", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = rt.Departures("z1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)
	departures, err = rt.Departures("z2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)
}

// Check that the time parameters are respected
func TestRealtimeTimeWindowing(t *testing.T) {

	// Delay some stuff, cancel some stuff.
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s2",
					DepartureSet:   true,
					DepartureDelay: 30,
				},
			},
		},
		{
			TripID: "t2",
			StopUpdates: []StopUpdate{
				{
					StopSequence: 1,
					SchedRel:     "SKIPPED",
				},
				{
					StopSequence:   2,
					DepartureSet:   true,
					DepartureDelay: 120,
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	// This should produce the following schedule for t1 and t2:
	// s1 - t1   23:00:00
	// s2 - t1   23:01:30
	// s2 - t2   23:13:00
	// s3 - t1   23:02:30
	// s3 - t2   23:14:00

	// Window exludes t2 stop
	departures, err := rt.Departures(
		"s2",
		time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC),
		12*time.Minute+59*time.Second,
		-1, "", -1, nil,
	)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	// Nudge it forward 1 second and t2 is included
	departures, err = rt.Departures(
		"s2",
		time.Date(2020, 1, 15, 23, 0, 1, 0, time.UTC),
		12*time.Minute+59*time.Second,
		-1, "", -1, nil,
	)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 13, 0, 0, time.UTC),
			Delay:        120 * time.Second,
		},
	}, departures)

	// Move the window past t1-s1 departure and t1 is exluded
	departures, err = rt.Departures(
		"s2",
		time.Date(2020, 1, 15, 23, 1, 30, 1, time.UTC),
		12*time.Minute+59*time.Second,
		-1, "", -1, nil,
	)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 13, 0, 0, time.UTC),
			Delay:        120 * time.Second,
		},
	}, departures)
}

// Make sure we can deal with trips that have loops. In these cases,
// (tripID,stopID) is not a unique departure identifier, but
// (tripID,stopSequence) is.
func TestRealtimeTripWithLoop(t *testing.T) {
	// This static schedule has t1 running from s1 to s2, and then
	// 3 loops s3-s5, and finally end of the trip at s3.
	static := testutil.BuildStatic(t, "sqlite", map[string][]string{
		// A weekdays only schedule
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"everyday,20200101,20210101,1,1,1,1,1,1,1",
		},
		// Two routes: L and F
		"routes.txt": {"route_id,route_short_name,route_type", "R1,R one,3"},
		// A bunch of stops
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"s1,stop one,1,1",
			"s2,stop two,1,1",
			"s3,stop three,1,1",
			"s4,stop four,1,1",
			"s5,stop five,1,1",
		},
		// The L has three trips running east, two running west. F runs north then south.
		"trips.txt": {
			"trip_id,route_id,service_id",
			"t1,R1,everyday",
		},
		// The L trips run 3rd ave - 14th st - 6th ave. F runs W4 - 14th - 23rd.
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"t1,s1,1,23:0:0,23:0:0",
			"t1,s2,2,23:1:0,23:1:0",
			"t1,s3,3,23:2:0,23:2:0",
			"t1,s4,4,23:3:0,23:3:0",
			"t1,s5,5,23:4:0,23:4:0",
			"t1,s3,6,23:5:0,23:5:0",
			"t1,s4,7,23:6:0,23:6:0",
			"t1,s5,8,23:7:0,23:7:0",
			"t1,s3,9,23:8:0,23:8:0",
			"t1,s4,10,23:9:0,23:9:0",
			"t1,s5,11,23:10:0,23:10:0",
			"t1,s3,12,23:11:0,23:11:0",
		},
	})

	// Let's drop in a delay at the 5th stop, skip stop 8 and stop
	// propagating the delay at stop 11.
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s5",
					StopSequence:   5,
					DepartureSet:   true,
					DepartureDelay: 30,
				},
				{
					SchedRel:     "SKIPPED",
					StopID:       "s5",
					StopSequence: 8,
				},
				{
					SchedRel:       "NO_DATA",
					StopSequence:   11,
					DepartureSet:   true,
					DepartureDelay: 0,
				},
			},
		},
	})
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	departures, err := rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC),
		},
	}, departures)

	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 0, 0, time.UTC),
		},
	}, departures)

	departures, err = rt.Departures("s3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 20*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 2, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s3",
			StopSequence: 6,
			Time:         time.Date(2020, 1, 15, 23, 5, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s3",
			StopSequence: 9,
			Time:         time.Date(2020, 1, 15, 23, 8, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	departures, err = rt.Departures("s4", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s4",
			StopSequence: 4,
			Time:         time.Date(2020, 1, 15, 23, 3, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s4",
			StopSequence: 7,
			Time:         time.Date(2020, 1, 15, 23, 6, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s4",
			StopSequence: 10,
			Time:         time.Date(2020, 1, 15, 23, 9, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	departures, err = rt.Departures("s5", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 10*time.Minute, -1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s5",
			StopSequence: 5,
			Time:         time.Date(2020, 1, 15, 23, 4, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s5",
			StopSequence: 11,
			Time:         time.Date(2020, 1, 15, 23, 10, 0, 0, time.UTC),
		},
	}, departures)

}

func TestRealtimeDepartureFiltering(t *testing.T) {
	// Two routes: bus going center to south and rail going center
	// to east. Each route has two trips: one heading out, one
	// heading in.
	static := testutil.BuildStatic(t, "sqlite", map[string][]string{
		// A weekdays only schedule
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"everyday,20200101,20210101,1,1,1,1,1,1,1",
		},
		// Two routes: L and F
		"routes.txt": {
			"route_id,route_long_name,route_type",
			"BusSouth,Bus South,3",
			"RailEast,Rail East,2",
		},
		// A bunch of stops
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"center,Center,20,20",
			"south,South,10,20",
			"east,East,20,30",
		},
		// The L has three trips running east, two running west. F runs north then south.
		"trips.txt": {
			"trip_id,route_id,direction_id,service_id",
			"ts,BusSouth,0,everyday",
			"tn,BusSouth,1,everyday",
			"te,RailEast,0,everyday",
			"tw,RailEast,1,everyday",
		},
		// The L trips run 3rd ave - 14th st - 6th ave. F runs W4 - 14th - 23rd.
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"ts,center,1,1:0:0,1:0:0",
			"ts,south,2,1:5:0,1:5:0",
			"tn,south,1,2:0:0,2:0:0",
			"tn,center,2,2:5:0,2:5:0",
			"te,center,1,3:0:0,3:0:0",
			"te,east,2,3:5:0,3:5:0",
			"tw,east,1,4:0:0,4:0:0",
			"tw,center,2,4:5:0,4:5:0",
		},
	})

	// 1 second delay on the BusSouth route
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "ts",
			StopUpdates: []StopUpdate{
				{
					StopID:         "center",
					StopSequence:   1,
					DepartureSet:   true,
					DepartureDelay: 1,
				},
			},
		},
		{
			TripID: "tn",
			StopUpdates: []StopUpdate{
				{
					StopID:         "south",
					StopSequence:   1,
					DepartureSet:   true,
					DepartureDelay: 1,
				},
			},
		},
	})
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	// From center we have 2 departures on separate routes
	departures, err := rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		-1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "BusSouth",
			TripID:       "ts",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 1, 0, 1, 0, time.UTC),
			Delay:        1 * time.Second,
		},
		{
			RouteID:      "RailEast",
			TripID:       "te",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 3, 0, 0, 0, time.UTC),
		},
	}, departures)

	// We can limit the number of results
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		1, "", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "BusSouth",
			TripID:       "ts",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 1, 0, 1, 0, time.UTC),
			Delay:        1 * time.Second,
		},
	}, departures)

	// We can filter on RouteID
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		1, "RailEast", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "RailEast",
			TripID:       "te",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 3, 0, 0, 0, time.UTC),
		},
	}, departures)
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		-1, "BusSouth", -1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "BusSouth",
			TripID:       "ts",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 1, 0, 1, 0, time.UTC),
			Delay:        1 * time.Second,
		},
	}, departures)

	// We can filter on DirectionID
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		-1, "BusSouth", 0, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "BusSouth",
			TripID:       "ts",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 1, 0, 1, 0, time.UTC),
			Delay:        1 * time.Second,
		},
	}, departures)
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		-1, "BusSouth", 1, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, []model.Departure{}, departures)

	// And we can filter on model.RouteType
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		-1, "", -1, []model.RouteType{model.RouteTypeBus})
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "BusSouth",
			TripID:       "ts",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 1, 0, 1, 0, time.UTC),
			Delay:        1 * time.Second,
		},
	}, departures)
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		-1, "", -1, []model.RouteType{model.RouteTypeRail})
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "RailEast",
			TripID:       "te",
			StopID:       "center",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 16, 3, 0, 0, 0, time.UTC),
		},
	}, departures)
	departures, err = rt.Departures(
		"center",
		time.Date(2020, 1, 16, 0, 0, 0, 0, time.UTC),
		10*time.Hour,
		-1, "", -1, []model.RouteType{model.RouteTypeMonorail})
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{}, departures)

}

func TestRealtimeLoadError(t *testing.T) {
	static := SimpleStaticFixture(t)

	// This one's fine
	incrementality := p.FeedHeader_FULL_DATASET
	data, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Incrementality:      &incrementality,
			Timestamp:           proto.Uint64(1702473763),
		},
	})
	require.NoError(t, err)
	_, err = gtfs.NewRealtime(context.Background(), static, [][]byte{data})
	assert.NoError(t, err)

	// This one is not (bad version)
	data, err = proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("3.0"),
			Incrementality:      &incrementality,
			Timestamp:           proto.Uint64(1702473763),
		},
	})
	require.NoError(t, err)
	_, err = gtfs.NewRealtime(context.Background(), static, [][]byte{data})
	assert.Error(t, err)
}

// Static departures can get pushed into a realtime request window by
// an update with delay.
func TestRealtimeUpdatePushingDepartureIntoWindow(t *testing.T) {
	// Along trip t1, we normally have stops s1,...,s4 at 23:00,
	// ..., 23:03. This update adds a big delay to s3, and has s1
	// departing way early.
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s1",
					DepartureSet:   true,
					DepartureDelay: -3600,
				},
				{
					StopID:        "s3",
					DepartureSet:  true,
					DepartureTime: time.Date(2020, 1, 15, 23, 59, 30, 1, time.UTC),
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	// stop s1 is early, and can be found around 22:00
	departures, err := rt.Departures(
		"s1",
		time.Date(2020, 1, 15, 21, 55, 0, 0, time.UTC),
		10*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(departures))
	assert.Equal(t, "t1", departures[0].TripID)
	assert.Equal(t, "s1", departures[0].StopID)
	assert.Equal(t, time.Date(2020, 1, 15, 22, 0, 0, 0, time.UTC), departures[0].Time)

	// there's no departure from s1 around the original time
	departures, err = rt.Departures(
		"s1",
		time.Date(2020, 1, 15, 22, 55, 0, 0, time.UTC),
		10*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(departures))

	// stop s3 is delayed, so it's not returned around 23:03
	departures, err = rt.Departures(
		"s3",
		time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC),
		10*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(departures))

	// but it is returned around midnight
	departures, err = rt.Departures(
		"s3",
		time.Date(2020, 1, 15, 23, 55, 0, 0, time.UTC),
		10*time.Minute,
		-1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(departures))
	assert.Equal(t, "t1", departures[0].TripID)
	assert.Equal(t, "s3", departures[0].StopID)
	assert.Equal(t, time.Date(2020, 1, 15, 23, 59, 30, 0, time.UTC), departures[0].Time)
}

// The spec doesn't cover this, but I've seen discussions about
// transit agencies consider arrival ahead of schedule to indicate
// recovery from any previous delays. I think this is only true for
// "timepoint" stop_times, but at the time of this writing we're
// assuming all stop_times are exact times.
func TestRealtimeArrivalRecovery(t *testing.T) {
	// Delay each departure by 30s at first stop
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{
					StopID:         "s1",
					DepartureSet:   true,
					DepartureDelay: 30,
				},
				{
					StopSequence: 2,
					ArrivalSet:   true,
					ArrivalDelay: -1,
				},
			},
		},
		{
			TripID: "t2",
			StopUpdates: []StopUpdate{
				{
					StopSequence:  1,
					DepartureSet:  true,
					DepartureTime: time.Date(2020, 1, 15, 23, 10, 30, 0, time.UTC),
				},
				{
					StopID:       "s2",
					ArrivalSet:   true,
					ArrivalDelay: 0,
				},
			},
		},
		{
			TripID: "t3",
			StopUpdates: []StopUpdate{
				{
					StopID:         "z1",
					DepartureSet:   true,
					DepartureDelay: 30,
				},
				{
					StopID:      "z2",
					ArrivalSet:  true,
					ArrivalTime: time.Date(2020, 1, 15, 23, 5, 58, 50, time.UTC),
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)

	// Check the delays on the first stop
	departures, err := rt.Departures("s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 0, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 10, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	departures, err = rt.Departures("z1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R2",
			TripID:       "t3",
			StopID:       "z1",
			StopSequence: 1,
			Time:         time.Date(2020, 1, 15, 23, 5, 30, 0, time.UTC),
			Delay:        30 * time.Second,
		},
	}, departures)

	// And verify they've all recovered on the second stop
	departures, err = rt.Departures("s2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 1, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 11, 0, 0, time.UTC),
		},
	}, departures)

	departures, err = rt.Departures("z2", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R2",
			TripID:       "t3",
			StopID:       "z2",
			StopSequence: 2,
			Time:         time.Date(2020, 1, 15, 23, 6, 0, 0, time.UTC),
		},
	}, departures)

	// And just to be paranoid, check that they remain on schedule
	// for subsequent stops.
	departures, err = rt.Departures("s3", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 30*time.Minute, -1, "", -1, nil)
	assert.NoError(t, err)
	assert.Equal(t, []model.Departure{
		{
			RouteID:      "R1",
			TripID:       "t1",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 2, 0, 0, time.UTC),
		},
		{
			RouteID:      "R1",
			TripID:       "t2",
			StopID:       "s3",
			StopSequence: 3,
			Time:         time.Date(2020, 1, 15, 23, 12, 0, 0, time.UTC),
		},
	}, departures)
}

func TestRealtimeDelayCrossingMidnight(t *testing.T) {
	// A single trip passing through s1...s4 over midnight.
	static := testutil.BuildStatic(t, "sqlite", map[string][]string{
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"everyday,20200101,20210101,1,1,1,1,1,1,1",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"R1,R_1,1",
		},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"s1,S1,1,1",
			"s2,S2,2,3",
			"s3,S3,4,5",
			"s4,S4,6,7",
		},
		"trips.txt": {
			"service_id,trip_id,route_id",
			"everyday,t1,R1",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"t1,s1,1,23:55:0,23:55:0",
			"t1,s2,2,23:59:0,23:59:0",
			"t1,s3,3,24:01:0,24:01:0",
			"t1,s4,4,24:05:0,24:05:0",
		},
	})

	for _, tc := range []struct {
		Name                  string
		StopUpdate            StopUpdate
		ExpectedDepartureTime time.Time
	}{
		{
			Name: "s1 delayed until before midnight via delay",
			StopUpdate: StopUpdate{
				StopID:         "s1",
				DepartureSet:   true,
				DepartureDelay: 180,
			},
			ExpectedDepartureTime: time.Date(2020, 1, 1, 23, 58, 0, 0, time.UTC),
		},
		{
			Name: "s1 delayed until before midnight via time",
			StopUpdate: StopUpdate{
				StopID:        "s1",
				DepartureSet:  true,
				DepartureTime: time.Date(2020, 1, 1, 23, 59, 0, 0, time.UTC),
			},
			ExpectedDepartureTime: time.Date(2020, 1, 1, 23, 59, 0, 0, time.UTC),
		},
		{
			Name: "s2 delayed until after midnight via delay",
			StopUpdate: StopUpdate{
				StopID:         "s2",
				DepartureSet:   true,
				DepartureDelay: 180,
			},
			ExpectedDepartureTime: time.Date(2020, 1, 2, 0, 2, 0, 0, time.UTC),
		},
		{
			Name: "s2 delayed until after midnight via time",
			StopUpdate: StopUpdate{
				StopID:        "s2",
				DepartureSet:  true,
				DepartureTime: time.Date(2020, 1, 2, 0, 2, 0, 0, time.UTC),
			},
			ExpectedDepartureTime: time.Date(2020, 1, 2, 0, 2, 0, 0, time.UTC),
		},
		{
			Name: "s3 delayed via delay",
			StopUpdate: StopUpdate{
				StopID:         "s3",
				DepartureSet:   true,
				DepartureDelay: 180,
			},
			ExpectedDepartureTime: time.Date(2020, 1, 2, 0, 4, 0, 0, time.UTC),
		},
		{
			Name: "s3 departing early but after midnight, via delay",
			StopUpdate: StopUpdate{
				StopID:         "s3",
				DepartureSet:   true,
				DepartureDelay: -30,
			},
			ExpectedDepartureTime: time.Date(2020, 1, 2, 0, 0, 30, 0, time.UTC),
		},
		{
			Name: "s3 departing early, before midnight, via delay",
			StopUpdate: StopUpdate{
				StopID:         "s3",
				DepartureSet:   true,
				DepartureDelay: -180,
			},
			ExpectedDepartureTime: time.Date(2020, 1, 2, 23, 58, 0, 0, time.UTC),
		},
		{
			Name: "s3 departing early, before midnight, via time",
			StopUpdate: StopUpdate{
				StopID:        "s3",
				DepartureSet:  true,
				DepartureTime: time.Date(2020, 1, 2, 23, 57, 0, 0, time.UTC),
			},
			ExpectedDepartureTime: time.Date(2020, 1, 2, 23, 57, 0, 0, time.UTC),
		},
	} {
		feed := buildFeed(t, []TripUpdate{
			{
				TripID:      "t1",
				StopUpdates: []StopUpdate{tc.StopUpdate},
			},
		})
		rt, err := gtfs.NewRealtime(context.Background(), static, feed)
		require.NoError(t, err)

		departures, err := rt.Departures(
			tc.StopUpdate.StopID,
			tc.ExpectedDepartureTime.Add(-10*time.Minute),
			20*time.Minute,
			-1, "", -1, nil,
		)
		require.NoErrorf(t, err, "%s: %v", tc.Name, err)
		require.Equal(t, 1, len(departures), tc.Name)
		require.Equal(t, tc.ExpectedDepartureTime, departures[0].Time, tc.Name)
	}
}

func TestRealtimeDelayCrossingDSTBoundaryOnCurrentDay(t *testing.T) {
	// Delays on trips crossing a DST boundary
	static := testutil.BuildStatic(t, "sqlite", map[string][]string{
		"agency.txt": {
			"agency_id,agency_name,agency_url,agency_timezone",
			"a,A,http://a.com/,America/New_York",
		},
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"everyday,20240101,20250101,1,1,1,1,1,1,1",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"R1,R_1,1",
		},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"s1,S1,1,1",
			"s2,S2,2,3",
			"s3,S3,4,5",
			"s4,S4,6,7",
			"s5,S5,8,9",
			"s6,S6,10,11",
			"s7,S7,12,13",
			"s8,S8,14,15",
		},
		"trips.txt": {
			"service_id,trip_id,route_id",
			"everyday,t1,R1",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"t1,s1,1,00:30:0,00:30:0",
			"t1,s2,2,01:00:0,01:00:0",
			"t1,s3,3,01:30:0,01:30:0",
			"t1,s4,4,02:00:0,02:00:0",
			"t1,s5,5,02:30:0,02:30:0",
			"t1,s6,6,03:00:0,03:00:0",
			"t1,s7,7,03:30:0,03:30:0",
			"t1,s8,8,04:00:0,03:30:0",
		},
	})

	tz, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// In most cases below, times can be given via time.Date(), but
	// when DST ends such times are ambiguous. This helper lets us
	// specify EST vs DST for those cases.
	parseT := func(t *testing.T, s string) time.Time {
		x, err := time.Parse("2006-01-02 15:04:05 -0700 MST", s)
		require.NoError(t, err)
		return x
	}

	for _, tc := range []struct {
		Name                  string
		StopUpdate            StopUpdate
		ExpectedDepartureTime string
	}{
		// DST begins on March 10, 2024 (at 02:00 time moves to 03:00).
		// Schedule times are given as offset from noon-12h on service day.
		{
			Name: "delay _not_ crossing into DST boundary (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s5", // 02:30, i.e. 01:30 past midnight
				DepartureSet:   true,
				DepartureDelay: 180,
			},
			ExpectedDepartureTime: "2024-03-10 01:33:00 -0500 EST",
		},
		{
			Name: "delay _not_ crossing into DST boundary (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s5",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 3, 10, 1, 59, 59, 0, tz),
			},
			ExpectedDepartureTime: "2024-03-10 01:59:59 -0500 EST",
		},
		{
			Name: "delay crossing into DST boundary (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s5",
				DepartureSet:   true,
				DepartureDelay: 30*60 + 1,
			},
			ExpectedDepartureTime: "2024-03-10 03:00:01 -0400 EDT",
		},
		{
			Name: "delay crossing into DST boundary (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s5",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 3, 10, 3, 4, 59, 0, tz),
			},
			ExpectedDepartureTime: "2024-03-10 03:04:59 -0400 EDT",
		},
		{
			Name: "no delay on boundary into DST",
			StopUpdate: StopUpdate{
				StopID: "s6", // 03:00, i.e. 02:00 past midnight
			},
			ExpectedDepartureTime: "2024-03-10 03:00:00 -0400 EDT",
		},
		{
			Name: "delay on boundary into DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s6",
				DepartureSet:   true,
				DepartureDelay: 1,
			},
			ExpectedDepartureTime: "2024-03-10 03:00:01 -0400 EDT",
		},
		{
			Name: "delay on boundary into DST (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s6",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 3, 10, 3, 0, 15, 0, tz),
			},
			ExpectedDepartureTime: "2024-03-10 03:00:15 -0400 EDT",
		},
		{
			Name: "delay after DST began (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s7", // 03:30, i.e. 02:30 past midnight
				DepartureSet:   true,
				DepartureDelay: 1,
			},
			ExpectedDepartureTime: "2024-03-10 03:30:01 -0400 EDT",
		},
		{
			Name: "delay after DST began (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s7",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 3, 10, 3, 30, 15, 0, tz),
			},
			ExpectedDepartureTime: "2024-03-10 03:30:15 -0400 EDT",
		},

		// DST ends on November 3, 2024 (at 02:00 time moves to 01:00).
		{
			Name: "delay _not_ crossing out of DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s1", // 00:30 (-> 01:30 past midnight)
				DepartureSet:   true,
				DepartureDelay: 180,
			},
			ExpectedDepartureTime: "2024-11-03 01:33:00 -0400 EDT",
		},

		{
			Name: "delay _not_ crossing out of DST (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s1",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 11, 3, 1, 59, 59, 0, tz),
			},
			ExpectedDepartureTime: "2024-11-03 01:59:59 -0400 EDT",
		},
		{
			Name: "delay crossing out of DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s1",
				DepartureSet:   true,
				DepartureDelay: 30*60 + 1,
			},
			ExpectedDepartureTime: "2024-11-03 01:00:01 -0500 EST",
		},
		{
			Name: "delay crossing out of DST (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s1",
				DepartureSet:  true,
				DepartureTime: parseT(t, "2024-11-03 01:04:59 -0500 EST"),
			},
			ExpectedDepartureTime: "2024-11-03 01:04:59 -0500 EST",
		},
		{
			Name: "no delay on boundary out of DST",
			StopUpdate: StopUpdate{
				StopID: "s2", // 01:00 (-> 02:00 past midnight)
			},
			ExpectedDepartureTime: "2024-11-03 01:00:00 -0500 EST",
		},
		{
			Name: "delay on boundary out of DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s2",
				DepartureSet:   true,
				DepartureDelay: 1,
			},
			ExpectedDepartureTime: "2024-11-03 01:00:01 -0500 EST",
		},
		{
			Name: "delay after DST ended (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s3", // 01:30 (-> 02:30 past midnight)
				DepartureSet:   true,
				DepartureDelay: 1,
			},
			ExpectedDepartureTime: "2024-11-03 01:30:01 -0500 EST",
		},
		{
			Name: "delay after DST ended (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s3",
				DepartureSet:  true,
				DepartureTime: parseT(t, "2024-11-03 01:30:15 -0500 EST"),
			},
			ExpectedDepartureTime: "2024-11-03 01:30:15 -0500 EST",
		},
	} {
		feed := buildFeed(t, []TripUpdate{
			{
				TripID:      "t1",
				StopUpdates: []StopUpdate{tc.StopUpdate},
			},
		})
		rt, err := gtfs.NewRealtime(context.Background(), static, feed)
		require.NoError(t, err)

		expTime := parseT(t, tc.ExpectedDepartureTime)
		departures, err := rt.Departures(
			tc.StopUpdate.StopID,
			expTime.Add(-10*time.Minute),
			20*time.Minute,
			-1, "", -1, nil,
		)
		require.NoErrorf(t, err, "%s: %v", tc.Name, err)
		require.Equal(t, 1, len(departures), tc.Name)
		require.Equal(t, expTime, departures[0].Time, tc.Name)
	}
}

func TestRealtimeDelayCrossingDSTBoundaryFromPreviousDay(t *testing.T) {
	// Delays on trips crossing a DST boundary on the _following
	// day_ (via scheduled trips running past 24:00).
	static := testutil.BuildStatic(t, "sqlite", map[string][]string{
		"agency.txt": {
			"agency_id,agency_name,agency_url,agency_timezone",
			"a,A,http://a.com/,America/New_York",
		},
		"calendar.txt": {
			"service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday",
			"everyday,20240101,20250101,1,1,1,1,1,1,1",
		},
		"routes.txt": {
			"route_id,route_short_name,route_type",
			"R1,R_1,1",
		},
		"stops.txt": {
			"stop_id,stop_name,stop_lat,stop_lon",
			"s1,S1,1,1",
			"s2,S2,2,3",
			"s3,S3,4,5",
			"s4,S4,6,7",
			"s5,S5,8,9",
			"s6,S6,10,11",
			"s7,S7,12,13",
		},
		"trips.txt": {
			"service_id,trip_id,route_id",
			"everyday,t1,R1",
		},
		"stop_times.txt": {
			"trip_id,stop_id,stop_sequence,departure_time,arrival_time",
			"t1,s1,1,24:30:0,24:30:0",
			"t1,s2,2,25:00:0,25:00:0",
			"t1,s3,3,25:30:0,25:30:0",
			"t1,s4,4,26:00:0,26:00:0",
			"t1,s5,5,26:30:0,26:30:0",
			"t1,s6,6,27:00:0,27:00:0",
			"t1,s7,7,27:30:0,27:30:0",
		},
	})

	tz, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// In most cases below, times can be given via time.Date(), but
	// when DST ends such times are ambiguous. This helper lets us
	// specify EST vs DST for those cases.
	parseT := func(t *testing.T, s string) time.Time {
		x, err := time.Parse("2006-01-02 15:04:05 -0700 MST", s)
		require.NoError(t, err)
		return x
	}

	for _, tc := range []struct {
		Name                  string
		StopUpdate            StopUpdate
		ExpectedDepartureTime time.Time
	}{
		// DST begins on March 10, 2024 (at 02:00 time moves to 03:00).
		{
			Name: "delay _not_ crossing into DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s3", // 25:30, i.e. 01:30 ignoring DST
				DepartureSet:   true,
				DepartureDelay: 29*60 + 59,
			},
			ExpectedDepartureTime: time.Date(2024, 3, 10, 1, 59, 59, 0, tz),
		},
		{
			Name: "delay _not_ crossing into DST (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s3",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 3, 10, 1, 59, 59, 0, tz),
			},
			ExpectedDepartureTime: time.Date(2024, 3, 10, 1, 59, 59, 0, tz),
		},
		{
			Name: "delay crossing into DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s3",
				DepartureSet:   true,
				DepartureDelay: 30*60 + 1,
			},
			ExpectedDepartureTime: time.Date(2024, 3, 10, 3, 0, 1, 0, tz),
		},
		{
			Name: "delay crossing into DST (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s3",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 3, 10, 3, 0, 2, 0, tz),
			},
			ExpectedDepartureTime: time.Date(2024, 3, 10, 3, 0, 2, 0, tz),
		},
		{
			Name: "no delay on boundary of DST switch",
			StopUpdate: StopUpdate{
				StopID: "s4", // 26:00, i.e. 02:00 ignoring DST
			},
			ExpectedDepartureTime: time.Date(2024, 3, 10, 3, 0, 0, 0, tz),
		},
		{
			Name: "delay after DST began (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s5", // 26:30, i.e. 02:30 ignoring DST
				DepartureSet:   true,
				DepartureDelay: 47,
			},
			ExpectedDepartureTime: time.Date(2024, 3, 10, 3, 30, 47, 0, tz),
		},
		{
			Name: "delay after DST began (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s5",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 3, 10, 3, 32, 1, 0, tz),
			},
			ExpectedDepartureTime: time.Date(2024, 3, 10, 3, 32, 1, 0, tz),
		},

		// DST ends on November 3, 2024 (at 02:00 time moves to 01:00)
		{
			Name: "delay _not_ crossing out of DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s3", // 25:30, i.e. 01:30 ignoring DST
				DepartureSet:   true,
				DepartureDelay: 29*60 + 7,
			},
			ExpectedDepartureTime: time.Date(2024, 11, 3, 1, 59, 7, 0, tz),
		},
		{
			Name: "delay _not_ crossing out of DST (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s3",
				DepartureSet:  true,
				DepartureTime: time.Date(2024, 11, 3, 1, 58, 5, 0, tz),
			},
			ExpectedDepartureTime: time.Date(2024, 11, 3, 1, 58, 5, 0, tz),
		},
		{
			Name: "delay crossing out of DST (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s3",
				DepartureSet:   true,
				DepartureDelay: 30*60 + 1,
			},
			ExpectedDepartureTime: parseT(t, "2024-11-03 01:00:01 -0500 EST"),
		},
		{
			Name: "delay crossing out of DST (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s3",
				DepartureSet:  true,
				DepartureTime: parseT(t, "2024-11-03 01:00:02 -0500 EST"),
			},
			ExpectedDepartureTime: parseT(t, "2024-11-03 01:00:02 -0500 EST"),
		},
		{
			Name: "no delay on boundary of DST end",
			StopUpdate: StopUpdate{
				StopID: "s4", // 26:00, i.e. 02:00 ignoring DST
			},
			ExpectedDepartureTime: parseT(t, "2024-11-03 01:00:00 -0500 EST"),
		},
		{
			Name: "early departure pushing back before DST begins",
			StopUpdate: StopUpdate{
				StopID:         "s4",
				DepartureSet:   true,
				DepartureDelay: -1,
			},
			ExpectedDepartureTime: time.Date(2024, 11, 3, 1, 59, 59, 0, tz),
		},
		{
			Name: "delay after DST ended (via delay)",
			StopUpdate: StopUpdate{
				StopID:         "s5", // 26:30, i.e. 02:30 ignoring DST
				DepartureSet:   true,
				DepartureDelay: 47,
			},
			ExpectedDepartureTime: parseT(t, "2024-11-03 01:30:47 -0500 EST"),
		},
		{
			Name: "delay after DST ended (via time)",
			StopUpdate: StopUpdate{
				StopID:        "s5",
				DepartureSet:  true,
				DepartureTime: parseT(t, "2024-11-03 01:32:01 -0500 EST"),
			},
			ExpectedDepartureTime: parseT(t, "2024-11-03 01:32:01 -0500 EST"),
		},
	} {
		feed := buildFeed(t, []TripUpdate{
			{
				TripID:      "t1",
				StopUpdates: []StopUpdate{tc.StopUpdate},
			},
		})
		rt, err := gtfs.NewRealtime(context.Background(), static, feed)
		require.NoError(t, err)

		departures, err := rt.Departures(
			tc.StopUpdate.StopID,
			tc.ExpectedDepartureTime.Add(-3*time.Hour),
			6*time.Hour,
			-1, "", -1, nil,
		)
		require.NoErrorf(t, err, "%s: %v", tc.Name, err)
		require.Equal(t, 1, len(departures), tc.Name)
		require.Equal(t, tc.ExpectedDepartureTime, departures[0].Time, tc.Name)
	}
}

func TestRealtimeArrivalDelayOnly(t *testing.T) {

	// Some agencies (e.g. WMATA) prefer passing Arrival Delay
	// instead of Departure Delay (or dito with Time.) When we
	// encounter a delayed arrival, and no departure information
	// is available, we should assume the departure is similarly
	// delayed.
	feed := buildFeed(t, []TripUpdate{
		{
			TripID: "t1",
			StopUpdates: []StopUpdate{
				{ // 55s delay
					StopID:       "s2",
					ArrivalSet:   true,
					ArrivalDelay: 55,
				},
			},
		},
		{
			TripID: "t2",
			StopUpdates: []StopUpdate{
				{ // arrival delay, but departure delay shows it recovers
					StopID:         "s1",
					ArrivalSet:     true,
					ArrivalDelay:   55,
					DepartureSet:   true,
					DepartureDelay: 0,
				},
				{ // 25s delay (via time)
					StopSequence: 2,
					ArrivalSet:   true,
					ArrivalTime:  time.Date(2020, 1, 15, 23, 11, 25, 0, time.UTC),
				},
			},
		},
		{
			TripID: "t3",
			StopUpdates: []StopUpdate{
				{ // 35s delay (via time)
					StopID:      "z1",
					ArrivalSet:  true,
					ArrivalTime: time.Date(2020, 1, 15, 23, 5, 35, 0, time.UTC),
				},
				{ // recovery via early arrival
					StopID:      "z2",
					ArrivalSet:  true,
					ArrivalTime: time.Date(2020, 1, 15, 23, 5, 55, 0, time.UTC),
				},
			},
		},
	})
	static := SimpleStaticFixture(t)
	rt, err := gtfs.NewRealtime(context.Background(), static, feed)
	require.NoError(t, err)
	assert.Equal(t, uint64(time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC).Unix()), rt.Timestamp)

	for _, tc := range []struct {
		TripID string
		StopID string
		Time   time.Time
		Delay  time.Duration
	}{
		{"t1", "s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 0},
		{"t1", "s2", time.Date(2020, 1, 15, 23, 1, 55, 0, time.UTC), 55 * time.Second},
		{"t1", "s3", time.Date(2020, 1, 15, 23, 2, 55, 0, time.UTC), 55 * time.Second},
		{"t2", "s1", time.Date(2020, 1, 15, 23, 0, 0, 0, time.UTC), 0},
		{"t2", "s2", time.Date(2020, 1, 15, 23, 11, 25, 0, time.UTC), 25 * time.Second},
		{"t2", "s3", time.Date(2020, 1, 15, 23, 12, 25, 0, time.UTC), 25 * time.Second},
		{"t3", "z1", time.Date(2020, 1, 15, 23, 5, 35, 0, time.UTC), 35 * time.Second},
		{"t3", "z2", time.Date(2020, 1, 15, 23, 6, 0, 0, time.UTC), 0},
	} {
		deps, err := rt.Departures(tc.StopID, tc.Time.Add(-time.Minute), 2*time.Minute, -1, "", -1, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(deps))
		assert.Equal(t, tc.Delay, deps[0].Delay, "trip %s stop %s time %s", tc.TripID, tc.StopID, tc.Time)
		assert.Equal(t, tc.Time, deps[0].Time, "trip %s stop %s time %s", tc.TripID, tc.StopID, tc.Time)
	}

}
