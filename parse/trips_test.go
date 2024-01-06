package parse

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/model"
	"tidbyt.dev/gtfs/storage"
)

func TestParseTrips(t *testing.T) {
	for _, tc := range []struct {
		name     string
		content  string
		routes   map[string]bool
		services map[string]bool
		trips    []*model.Trip
		err      bool
	}{
		{
			"minimal",
			`
trip_id,route_id,service_id
t,r,s`,
			map[string]bool{"r": true},
			map[string]bool{"s": true},
			[]*model.Trip{&model.Trip{
				ID:        "t",
				RouteID:   "r",
				ServiceID: "s",
			}},
			false,
		},

		{
			"all_fields_set",
			`
trip_id,route_id,service_id,trip_headsign,trip_short_name,direction_id
t,r,s,head,short,1`,
			map[string]bool{"r": true},
			map[string]bool{"s": true},
			[]*model.Trip{&model.Trip{
				ID:          "t",
				RouteID:     "r",
				ServiceID:   "s",
				Headsign:    "head",
				ShortName:   "short",
				DirectionID: 1,
			}},
			false,
		},

		{
			"multiple trips",
			`
trip_id,route_id,service_id,direction_id
t1,r1,s2,0
t2,r2,s1,1`,
			map[string]bool{"r1": true, "r2": true},
			map[string]bool{"s1": true, "s2": true},
			[]*model.Trip{
				&model.Trip{
					ID:          "t1",
					RouteID:     "r1",
					ServiceID:   "s2",
					DirectionID: 0,
				},
				&model.Trip{
					ID:          "t2",
					RouteID:     "r2",
					ServiceID:   "s1",
					DirectionID: 1,
				},
			},
			false,
		},

		{
			"blank trip_id",
			`
route_id,service_id
r,s`,
			map[string]bool{"r": true},
			map[string]bool{"s": true},
			nil,
			true,
		},

		{
			"blank route_id",
			`
trip_id,service_id
t,s`,
			map[string]bool{"r": true},
			map[string]bool{"s": true},
			nil,
			true,
		},

		{
			"blank service_id",
			`
trip_id,route_id
t,r`,
			map[string]bool{"r": true},
			map[string]bool{"s": true},
			nil,
			true,
		},

		{
			"unknown route_id",
			`
trip_id,route_id,service_id
t,r1,s`,
			map[string]bool{"r2": true},
			map[string]bool{"s": true},
			nil,
			true,
		},

		{
			"unknown service_id",
			`
trip_id,route_id,service_id
t,r,s1`,
			map[string]bool{"r": true},
			map[string]bool{"s2": true},
			nil,
			true,
		},

		{
			"repeated trip_id",
			`
trip_id,route_id,service_id
t,r1,s1
t,r2,s2`,
			map[string]bool{"r1": true, "r2": true},
			map[string]bool{"s1": true, "s2": true},
			nil,
			true,
		},

		{
			"invalid direction_id",
			`
trip_id,route_id,service_id,direction_id
t,r,s,2`,
			map[string]bool{"r": true},
			map[string]bool{"s": true},
			nil,
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			s, err := storage.NewSQLiteStorage()
			require.NoError(t, err)
			writer, err := s.GetWriter("test")
			require.NoError(t, err)

			require.NoError(t, writer.BeginTrips())
			tripIDs, err := ParseTrips(writer, bytes.NewBufferString(tc.content), tc.routes, tc.services)
			if tc.err {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			require.NoError(t, writer.EndTrips())

			reader, err := s.GetReader("test")
			require.NoError(t, err)
			trips, err := reader.Trips()
			require.NoError(t, err)
			assert.Equal(t, len(tc.trips), len(trips))
			sort.Slice(trips, func(i, j int) bool {
				return trips[i].ID < trips[j].ID
			})
			assert.Equal(t, tc.trips, trips)

			// IDs of all trips should be returned
			for _, trip := range trips {
				assert.True(t, tripIDs[trip.ID])
			}
		})
	}
}
