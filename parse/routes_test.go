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

func TestParseRoutes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		content  string
		agencies map[string]bool
		routes   []*model.Route
		err      bool
	}{
		{
			"minimal_with_short_name",
			`
route_id,route_short_name,route_type
1,1,3`,
			map[string]bool{},
			[]*model.Route{&model.Route{
				ID:        "1",
				ShortName: "1",
				Type:      3,
				Color:     "FFFFFF",
				TextColor: "000000",
			}},
			false,
		},

		{
			"minimal_with_long_name",
			`
route_id,route_long_name,route_type
1,Route One,3`,
			map[string]bool{},
			[]*model.Route{&model.Route{
				ID:        "1",
				LongName:  "Route One",
				Type:      3,
				Color:     "FFFFFF",
				TextColor: "000000",
			}},
			false,
		},

		{
			"all_fields_set",
			`
route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
r1,a1,one,Route One,Description1,3,http://one/,FFFFF0,00000F
r2,a2,two,Route Two,Description2,3,http://two/,FFFFF1,00000E`,
			map[string]bool{"a1": true, "a2": true},
			[]*model.Route{
				&model.Route{
					ID:        "r1",
					AgencyID:  "a1",
					ShortName: "one",
					LongName:  "Route One",
					Desc:      "Description1",
					Type:      model.RouteType(3),
					URL:       "http://one/",
					Color:     "FFFFF0",
					TextColor: "00000F",
				},
				&model.Route{
					ID:        "r2",
					AgencyID:  "a2",
					ShortName: "two",
					LongName:  "Route Two",
					Desc:      "Description2",
					Type:      model.RouteType(3),
					URL:       "http://two/",
					Color:     "FFFFF1",
					TextColor: "00000E",
				},
			},
			false,
		},

		{
			"record with missing route_id",
			`
route_id,route_short_name,route_type
r1,one,3
,two,3`,
			map[string]bool{},
			nil,
			true,
		},

		{
			"record with neither short nor long name",
			`
route_id,route_type
r1,3`,
			map[string]bool{},
			nil,
			true,
		},

		{
			"record without route_type",
			`
route_id,route_short_name
r1,one`,
			map[string]bool{},
			nil,
			true,
		},

		{
			"record with invalid route_type",
			`
route_id,route_short_name,route_type
r1,one,invalid`,
			map[string]bool{},
			nil,
			true,
		},

		{
			"record with invalid route_color",
			`
route_id,route_short_name,route_type,route_color
r1,one,3,invalid`,
			map[string]bool{},
			nil,
			true,
		},

		{
			"record with invalid route_text_color",
			`
route_id,route_short_name,route_type,route_text_color
r1,one,3,invalid`,
			map[string]bool{},
			nil,
			true,
		},

		{
			"repeated route_id",
			`
route_id,route_short_name,route_type
r1,one,3
r1,two,3`,
			map[string]bool{},
			nil,
			true,
		},

		{
			"unknown agency_id",
			`
route_id,agency_id,route_short_name,route_type
r1,a1,one,3`,
			map[string]bool{"b1": true},
			nil,
			true,
		},

		{
			"multiple agencies, one missing id",
			`
route_id,agency_id,route_short_name,route_type
r1,a1,one,3
r2,,two,3`,
			map[string]bool{"a1": true, "": true},
			nil,
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			s, err := storage.NewSQLiteStorage()
			require.NoError(t, err)
			writer, err := s.GetWriter("test")
			require.NoError(t, err)

			routeIDs, err := ParseRoutes(writer, bytes.NewBufferString(tc.content), tc.agencies)
			if tc.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			reader, err := s.GetReader("test")
			require.NoError(t, err)
			routes, err := reader.Routes()
			require.NoError(t, err)
			assert.Equal(t, len(tc.routes), len(routes))
			sort.Slice(routes, func(i, j int) bool {
				return routes[i].ID < routes[j].ID
			})
			assert.Equal(t, tc.routes, routes)

			// all route IDs should be returned
			for _, route := range tc.routes {
				assert.True(t, routeIDs[route.ID])
			}
		})
	}
}
