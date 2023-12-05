package parse

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/storage"
)

func TestParseAgency(t *testing.T) {
	for _, tc := range []struct {
		name      string
		content   string
		agencyIDs map[string]bool
		timezone  string
		agencies  []*storage.Agency
		err       bool
	}{
		{
			"minimal",
			`
agency_name,agency_url,agency_timezone
Agency Name,http://www.example.com,America/New_York`,
			map[string]bool{"": true},
			"America/New_York",
			[]*storage.Agency{&storage.Agency{
				Name:     "Agency Name",
				URL:      "http://www.example.com",
				Timezone: "America/New_York",
			}},
			false,
		},

		{
			"multiple agencies",
			`
agency_id,agency_name,agency_url,agency_timezone
1,Agency One,http://www.example.com/one,America/New_York
2,Agency Two,http://www.example.com/two,America/New_York
3,Agency Three,http://www.example.com/three,America/New_York`,
			map[string]bool{"1": true, "2": true, "3": true},
			"America/New_York",
			[]*storage.Agency{
				&storage.Agency{
					ID:       "1",
					Name:     "Agency One",
					URL:      "http://www.example.com/one",
					Timezone: "America/New_York",
				},
				&storage.Agency{
					ID:       "2",
					Name:     "Agency Two",
					URL:      "http://www.example.com/two",
					Timezone: "America/New_York",
				},
				&storage.Agency{
					ID:       "3",
					Name:     "Agency Three",
					URL:      "http://www.example.com/three",
					Timezone: "America/New_York",
				},
			},
			false,
		},

		{
			"missing agency_name",
			`
agency_id,agency_url,agency_timezone
1,http://www.example.com,America/New_York`,
			nil, "", nil, true,
		},

		{
			"missing agency_url",
			`
agency_id,agency_name,agency_timezone
1,Agency Name,America/New_York`,
			nil, "", nil, true,
		},

		{
			"missing agency_timezone",
			`
agency_id,agency_name,agency_url
1,Agency Name,http://www.example.com`,
			nil, "", nil, true,
		},

		{
			"multiple agencies, with duplicate IDs",
			`
agency_id,agency_name,agency_url,agency_timezone
1,Agency One,http://www.example.com/one,America/New_York
2,Agency Two,http://www.example.com/two,America/New_York
1,Agency Three,http://www.example.com/three,America/New_York`,
			nil, "", nil, true,
		},

		{
			"multiple agencies, without IDs",
			`
agency_name,agency_url,agency_timezone
Agency One,http://www.example.com/one,America/New_York
Agency Two,http://www.example.com/two,America/New_York`,
			nil, "", nil, true,
		},

		{
			"csv without records",
			`
agency_id,agency_name,agency_url,agency_timezone`,
			nil, "", nil, true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := storage.NewMemoryStorage()
			writer, err := storage.GetWriter("test")
			require.NoError(t, err)

			agency, tz, err := ParseAgency(writer, bytes.NewBufferString(tc.content))
			if tc.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.agencyIDs, agency)
			assert.Equal(t, tc.timezone, tz)

			reader, err := storage.GetReader("test")
			require.NoError(t, err)
			agencies, err := reader.Agencies()
			require.NoError(t, err)
			assert.Equal(t, len(tc.agencies), len(agencies))
			sort.Slice(agencies, func(i, j int) bool {
				return agencies[i].ID < agencies[j].ID
			})
			assert.Equal(t, tc.agencies, agencies)
		})
	}
}
