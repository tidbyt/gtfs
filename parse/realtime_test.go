package parse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"

	p "tidbyt.dev/gtfs/proto"
)

func TestParseRealtimeBadHeader(t *testing.T) {
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
	_, err = ParseRealtime(context.Background(), [][]byte{data})
	assert.NoError(t, err)

	// Unsupported version
	data, err = proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("3.0"),
			Incrementality:      &incrementality,
			Timestamp:           proto.Uint64(1702473763),
		},
	})
	require.NoError(t, err)
	_, err = ParseRealtime(context.Background(), [][]byte{data})
	assert.Error(t, err)

	// Unsupported incrementality
	incrementality = p.FeedHeader_DIFFERENTIAL
	data, err = proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Incrementality:      &incrementality,
			Timestamp:           proto.Uint64(1702473763),
		},
	})
	require.NoError(t, err)
	_, err = ParseRealtime(context.Background(), [][]byte{data})
	assert.Error(t, err)
}

func TestParseRealtimeNoUpdates(t *testing.T) {
	data, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Incrementality:      p.FeedHeader_FULL_DATASET.Enum(),
			Timestamp:           proto.Uint64(1702473763),
		},
	})
	require.NoError(t, err)

	rt, err := ParseRealtime(context.Background(), [][]byte{data})
	require.NoError(t, err)
	assert.Equal(t, 0, len(rt.SkippedTrips))
	assert.Equal(t, 0, len(rt.Updates))
	assert.Equal(t, uint64(1702473763), rt.Timestamp)
}

func TestParseRealtimeStopTimeUpdates(t *testing.T) {
	data, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
		},
		Entity: []*p.FeedEntity{
			{
				Id: proto.String("entity1"),
				TripUpdate: &p.TripUpdate{
					Trip: &p.TripDescriptor{
						TripId:               proto.String("trip1"),
						RouteId:              proto.String("route1"),
						ScheduleRelationship: p.TripDescriptor_SCHEDULED.Enum(),
					},
					StopTimeUpdate: []*p.TripUpdate_StopTimeUpdate{
						// Both arrival and departure set
						{
							StopSequence: proto.Uint32(4),
							StopId:       proto.String("stop1"),
							Arrival: &p.TripUpdate_StopTimeEvent{
								Time:  proto.Int64(time.Date(2015, 1, 2, 3, 3, 2, 0, time.UTC).Unix()),
								Delay: proto.Int32(47),
							},
							Departure: &p.TripUpdate_StopTimeEvent{
								Time:  proto.Int64(time.Date(2015, 1, 2, 3, 3, 4, 0, time.UTC).Unix()),
								Delay: proto.Int32(48),
							},
						},
						// Only arrival set
						{
							StopSequence: proto.Uint32(5),
							StopId:       proto.String("stop2"),
							Arrival: &p.TripUpdate_StopTimeEvent{
								Time:  proto.Int64(time.Date(2015, 1, 2, 3, 3, 6, 0, time.UTC).Unix()),
								Delay: proto.Int32(49),
							},
						},
						// Only departure set
						{
							StopSequence: proto.Uint32(6),
							StopId:       proto.String("stop3"),
							Departure: &p.TripUpdate_StopTimeEvent{
								Time:  proto.Int64(time.Date(2015, 1, 2, 3, 3, 8, 0, time.UTC).Unix()),
								Delay: proto.Int32(50),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	rt, err := ParseRealtime(context.Background(), [][]byte{data})
	require.NoError(t, err)

	assert.Equal(t, 0, len(rt.SkippedTrips))
	assert.Equal(t, 3, len(rt.Updates))

	assert.Equal(t, "trip1", rt.Updates[0].TripID)
	assert.Equal(t, "stop1", rt.Updates[0].StopID)
	assert.Equal(t, uint32(4), rt.Updates[0].StopSequence)
	assert.True(t, rt.Updates[0].ArrivalIsSet)
	assert.Equal(t, time.Date(2015, 1, 2, 3, 3, 2, 0, time.UTC), rt.Updates[0].ArrivalTime)
	assert.Equal(t, 47*time.Second, rt.Updates[0].ArrivalDelay)
	assert.True(t, rt.Updates[0].DepartureIsSet)
	assert.Equal(t, time.Date(2015, 1, 2, 3, 3, 4, 0, time.UTC), rt.Updates[0].DepartureTime)
	assert.Equal(t, 48*time.Second, rt.Updates[0].DepartureDelay)

	assert.Equal(t, "trip1", rt.Updates[1].TripID)
	assert.Equal(t, "stop2", rt.Updates[1].StopID)
	assert.Equal(t, uint32(5), rt.Updates[1].StopSequence)
	assert.True(t, rt.Updates[1].ArrivalIsSet)
	assert.Equal(t, time.Date(2015, 1, 2, 3, 3, 6, 0, time.UTC), rt.Updates[1].ArrivalTime)
	assert.Equal(t, 49*time.Second, rt.Updates[1].ArrivalDelay)
	assert.False(t, rt.Updates[1].DepartureIsSet)
	assert.Equal(t, time.Time{}, rt.Updates[1].DepartureTime)
	assert.Equal(t, 0*time.Second, rt.Updates[1].DepartureDelay)

	assert.Equal(t, "trip1", rt.Updates[2].TripID)
	assert.Equal(t, "stop3", rt.Updates[2].StopID)
	assert.Equal(t, uint32(6), rt.Updates[2].StopSequence)
	assert.False(t, rt.Updates[2].ArrivalIsSet)
	assert.Equal(t, time.Time{}, rt.Updates[2].ArrivalTime)
	assert.Equal(t, 0*time.Second, rt.Updates[2].ArrivalDelay)
	assert.True(t, rt.Updates[2].DepartureIsSet)
	assert.Equal(t, time.Date(2015, 1, 2, 3, 3, 8, 0, time.UTC), rt.Updates[2].DepartureTime)
	assert.Equal(t, 50*time.Second, rt.Updates[2].DepartureDelay)
}

func TestParseRealtimeCancelledTrip(t *testing.T) {
	data, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
		},
		Entity: []*p.FeedEntity{
			{
				Id: proto.String("entity1"),
				TripUpdate: &p.TripUpdate{
					Trip: &p.TripDescriptor{
						TripId:               proto.String("trip1"),
						RouteId:              proto.String("route1"),
						ScheduleRelationship: p.TripDescriptor_CANCELED.Enum(),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	rt, err := ParseRealtime(context.Background(), [][]byte{data})
	require.NoError(t, err)

	assert.Equal(t, 1, len(rt.SkippedTrips))
	assert.True(t, rt.SkippedTrips["trip1"])
	assert.Equal(t, 0, len(rt.Updates))
}

func TestParseRealtimeMultipleFeeds(t *testing.T) {
	// This one cancels a trip
	data1, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Timestamp:           proto.Uint64(1337),
		},
		Entity: []*p.FeedEntity{
			{
				Id: proto.String("entity1"),
				TripUpdate: &p.TripUpdate{
					Trip: &p.TripDescriptor{
						TripId:               proto.String("trip1"),
						RouteId:              proto.String("route1"),
						ScheduleRelationship: p.TripDescriptor_CANCELED.Enum(),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// This one delays arrival at a stop on another trip
	data2, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Timestamp:           proto.Uint64(1338),
		},
		Entity: []*p.FeedEntity{
			{
				Id: proto.String("entity2"),
				TripUpdate: &p.TripUpdate{
					Trip: &p.TripDescriptor{
						TripId:  proto.String("trip2"),
						RouteId: proto.String("route2"),
					},
					StopTimeUpdate: []*p.TripUpdate_StopTimeUpdate{
						{
							StopSequence: proto.Uint32(1),
							StopId:       proto.String("stop1"),
							Arrival: &p.TripUpdate_StopTimeEvent{
								Delay: proto.Int32(47),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// This one delays dearture at a stop on another trip
	data3, err := proto.Marshal(&p.FeedMessage{
		Header: &p.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Timestamp:           proto.Uint64(1339),
		},
		Entity: []*p.FeedEntity{
			{
				Id: proto.String("entity3"),
				TripUpdate: &p.TripUpdate{
					Trip: &p.TripDescriptor{
						TripId:  proto.String("trip3"),
						RouteId: proto.String("route3"),
					},
					StopTimeUpdate: []*p.TripUpdate_StopTimeUpdate{
						{
							StopSequence: proto.Uint32(1),
							StopId:       proto.String("stop2"),
							Departure: &p.TripUpdate_StopTimeEvent{
								Delay: proto.Int32(48),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// All three are merged into one
	rt, err := ParseRealtime(context.Background(), [][]byte{data1, data2, data3})
	require.NoError(t, err)

	assert.Equal(t, 1, len(rt.SkippedTrips))
	assert.True(t, rt.SkippedTrips["trip1"])
	assert.Equal(t, 2, len(rt.Updates))

	assert.Equal(t, "trip2", rt.Updates[0].TripID)
	assert.Equal(t, "stop1", rt.Updates[0].StopID)
	assert.Equal(t, uint32(1), rt.Updates[0].StopSequence)
	assert.True(t, rt.Updates[0].ArrivalIsSet)
	assert.Equal(t, 47*time.Second, rt.Updates[0].ArrivalDelay)
	assert.False(t, rt.Updates[0].DepartureIsSet)

	assert.Equal(t, "trip3", rt.Updates[1].TripID)
	assert.Equal(t, "stop2", rt.Updates[1].StopID)
	assert.Equal(t, uint32(1), rt.Updates[1].StopSequence)
	assert.False(t, rt.Updates[1].ArrivalIsSet)
	assert.True(t, rt.Updates[1].DepartureIsSet)
	assert.Equal(t, 48*time.Second, rt.Updates[1].DepartureDelay)

	// Timestamp is taken from the last of the three
	assert.Equal(t, uint64(1339), rt.Timestamp)
}
