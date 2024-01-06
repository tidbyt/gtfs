package parse

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/model"
	"tidbyt.dev/gtfs/storage"
)

func TestParseStopTimes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		content   string
		trips     map[string]bool
		stops     map[string]bool
		err       bool
		stopTimes []*model.StopTime
	}{
		{
			"minimal",
			`
trip_id,arrival_time,departure_time,stop_id,stop_sequence
t,10:00:00,10:00:01,s,1`,
			map[string]bool{"t": true},
			map[string]bool{"s": true},
			false,
			[]*model.StopTime{
				&model.StopTime{
					TripID:       "t",
					Arrival:      "100000",
					Departure:    "100001",
					StopID:       "s",
					StopSequence: 1,
				},
			},
		},

		{
			"all_fields_set_and_multiple_records",
			`
trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign
t,10:00:00,10:00:01,s1,1,sh1
t,10:00:02,10:00:03,s2,2,sh2
`,
			map[string]bool{"t": true},
			map[string]bool{"s1": true, "s2": true},
			false,
			[]*model.StopTime{
				&model.StopTime{
					TripID:       "t",
					Arrival:      "100000",
					Departure:    "100001",
					StopID:       "s1",
					StopSequence: 1,
					Headsign:     "sh1",
				},
				&model.StopTime{
					TripID:       "t",
					Arrival:      "100002",
					Departure:    "100003",
					StopID:       "s2",
					StopSequence: 2,
					Headsign:     "sh2",
				},
			},
		},

		{
			"times above 24h",
			`
trip_id,arrival_time,departure_time,stop_id,stop_sequence
t,25:00:00,25:00:01,s,1`,
			map[string]bool{"t": true},
			map[string]bool{"s": true},
			false,
			[]*model.StopTime{
				&model.StopTime{
					TripID:       "t",
					Arrival:      "250000",
					Departure:    "250001",
					StopID:       "s",
					StopSequence: 1,
				},
			},
		},

		{
			"missing trip_id",
			`
arrival_time,departure_time,stop_id,stop_sequence
10:00:00,10:00:01,s,1`,
			nil, nil, true, nil,
		},

		{
			"missing arrival_time",
			`
trip_id,departure_time,stop_id,stop_sequence
t,10:00:01,s,1`,
			nil, nil, true, nil,
		},

		{
			"missing departure_time",
			`
trip_id,arrival_time,stop_id,stop_sequence
t,10:00:00,s,1`,
			nil, nil, true, nil,
		},

		{
			"missing stop_id",
			`
trip_id,arrival_time,departure_time,stop_sequence
t,10:00:00,10:00:01,1`,
			nil, nil, true, nil,
		},

		{
			"missing stop_sequence",
			`
trip_id,arrival_time,departure_time,stop_id
t,10:00:00,10:00:01,s`,
			nil, nil, true, nil,
		},

		{
			"unknown trip",
			`
trip_id,arrival_time,departure_time,stop_id,stop_sequence
t,10:00:00,10:00:01,s,1`,
			map[string]bool{"t2": true},
			map[string]bool{"s": true},
			true,
			nil,
		},

		{
			"unknown stop",
			`
trip_id,arrival_time,departure_time,stop_id,stop_sequence
t,10:00:00,10:00:01,s,1`,
			map[string]bool{"t": true},
			map[string]bool{"s2": true},
			true,
			nil,
		},

		{
			"invalid arrival_time",
			`
trip_id,arrival_time,departure_time,stop_id,stop_sequence
t,10:00:derp,10:00:01,s,1`,
			map[string]bool{"t": true},
			map[string]bool{"s": true},
			true,
			nil,
		},

		{
			"invalid departure_time",
			`
trip_id,arrival_time,departure_time,stop_id,stop_sequence
t,10:00:00,10:00:derp,s,1`,
			map[string]bool{"t": true},
			map[string]bool{"s": true},
			true,
			nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := storage.NewSQLiteStorage()
			require.NoError(t, err)
			writer, err := s.GetWriter("test")
			require.NoError(t, err)

			require.NoError(t, writer.BeginStopTimes())
			maxArrival, maxDeparture, err := ParseStopTimes(
				writer,
				bytes.NewBufferString(tc.content),
				tc.trips,
				tc.stops,
			)
			if tc.err {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
				assert.NoError(t, writer.EndStopTimes())
			}

			expectedMaxArrival := ""
			expectedMaxDeparture := ""
			for _, stopTime := range tc.stopTimes {
				if expectedMaxArrival == "" || stopTime.Arrival > expectedMaxArrival {
					expectedMaxArrival = stopTime.Arrival
				}
				if expectedMaxDeparture == "" || stopTime.Departure > expectedMaxDeparture {
					expectedMaxDeparture = stopTime.Departure
				}
			}

			assert.Equal(t, expectedMaxArrival, maxArrival)
			assert.Equal(t, expectedMaxDeparture, maxDeparture)

			assert.NoError(t, err)

			reader, err := s.GetReader("test")
			require.NoError(t, err)
			stopTimes, err := reader.StopTimes()
			require.NoError(t, err)
			assert.Equal(t, len(tc.stopTimes), len(stopTimes))
			assert.Equal(t, tc.stopTimes, stopTimes)
		})
	}
}
