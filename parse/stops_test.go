package parse

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/storage"
)

func TestParseStops(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		stops   []*storage.Stop
		err     bool
	}{
		{
			"minimal_stop",
			`
stop_id,stop_name,stop_lat,stop_lon
s,name,1.1,2.2`,
			[]*storage.Stop{&storage.Stop{
				ID:   "s",
				Name: "name",
				Lat:  1.1,
				Lon:  2.2,
			}},
			false,
		},

		{
			"maximal_stop",
			`
location_type,stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,parent_station,platform_code
0,s,code_s,Stop,desc_s,1.1,2.2,url_s,ps,platform
1,ps,code_ps,Station,desc_ps,3.3,4.4,url_ps,,
2,e,code_e,Entrance,desc_e,5.5,6.6,url_se,ps,
3,g,code_g,Generic,desc_g,,,url_g,ps,
4,b,code_b,Boarding,desc_b,,,url_b,ps,
`,
			[]*storage.Stop{
				&storage.Stop{
					ID:            "b",
					Code:          "code_b",
					Name:          "Boarding",
					Desc:          "desc_b",
					URL:           "url_b",
					ParentStation: "ps",
					LocationType:  storage.LocationTypeBoardingArea,
				},
				&storage.Stop{
					ID:            "e",
					Code:          "code_e",
					Name:          "Entrance",
					Desc:          "desc_e",
					Lat:           5.5,
					Lon:           6.6,
					URL:           "url_se",
					ParentStation: "ps",
					LocationType:  storage.LocationTypeEntranceExit,
				},
				&storage.Stop{
					ID:            "g",
					Code:          "code_g",
					Name:          "Generic",
					Desc:          "desc_g",
					URL:           "url_g",
					ParentStation: "ps",
					LocationType:  storage.LocationTypeGenericNode,
				},
				&storage.Stop{
					ID:           "ps",
					Code:         "code_ps",
					Name:         "Station",
					Desc:         "desc_ps",
					Lat:          3.3,
					Lon:          4.4,
					URL:          "url_ps",
					LocationType: storage.LocationTypeStation,
				},
				&storage.Stop{
					ID:            "s",
					Code:          "code_s",
					Name:          "Stop",
					Desc:          "desc_s",
					Lat:           1.1,
					Lon:           2.2,
					URL:           "url_s",
					ParentStation: "ps",
					PlatformCode:  "platform",
					LocationType:  storage.LocationTypeStop,
				},
			},
			false,
		},

		{
			"blank stop_id",
			`
stop_id,stop_name,stop_lat,stop_lon
,name,1.1,2.2`,
			nil,
			true,
		},

		{
			"repeated stop_id",
			`
stop_id,stop_name,stop_lat,stop_lon
s,name_1,1.1,2.2
s,name_2,1.2,2.3`,
			nil,
			true,
		},

		{
			"invalid stop_lat",
			`
stop_id,stop_name,stop_lat,stop_lon
s,name,1.1x,2.2`,
			nil,
			true,
		},

		{
			"invalid stop_lon",
			`
stop_id,stop_name,stop_lat,stop_lon
s,name,1.1,2.2x`,
			nil,
			true,
		},

		{
			"invalid location_type",
			`
stop_id,stop_name,stop_lat,stop_lon,location_type
s,name,1.1,2.2,donkey`,
			nil,
			true,
		},

		{
			"missing parent_station",
			`
stop_id,stop_name,stop_lat,stop_lon,parent_station
s,name,1.1,2.2,ps`,
			nil,
			true,
		},

		{
			"missing lat for stop",
			`
stop_id,stop_name,stop_lon
s,name,2.2`,
			nil,
			true,
		},

		{
			"missing lon for stop",
			`
stop_id,stop_name,stop_lat
s,name,1.1`,
			nil,
			true,
		},

		{
			"missing lat for station",
			`
stop_id,stop_name,stop_lon,location_type
s,name,2.2,1`,
			nil,
			true,
		},

		{
			"missing lon for station",
			`
stop_id,stop_name,stop_lat,location_type
s,name,1.1,1`,
			nil,
			true,
		},

		{
			"missing stop_name for stop",
			`
stop_id,stop_lat,stop_lon
s,1.1,2.2`,
			nil,
			true,
		},

		{
			"missing stop_name for station",
			`
stop_id,stop_lat,stop_lon,location_type
s,1.1,2.2,1`,
			nil,
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := storage.NewMemoryStorage()
			writer, err := s.GetWriter("test")
			require.NoError(t, err)

			stopIDs, err := ParseStops(writer, bytes.NewBufferString(tc.content))
			if tc.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			reader, err := s.GetReader("test")
			require.NoError(t, err)
			stops, err := reader.Stops()
			require.NoError(t, err)
			assert.Equal(t, len(tc.stops), len(stops))
			sort.Slice(stops, func(i, j int) bool {
				return stops[i].ID < stops[j].ID
			})
			assert.Equal(t, tc.stops, stops)
			for _, s := range stops {
				assert.True(t, stopIDs[s.ID])
			}
		})
	}
}
