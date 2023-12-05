package parse

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/storage"
)

func TestCalendarDates(t *testing.T) {
	for _, tc := range []struct {
		name     string
		content  string
		expected []*storage.CalendarDate
		minDate  string
		maxDate  string
		err      bool
	}{
		{
			"minimal",
			`
service_id,date,exception_type
s1,20170101,1`,
			[]*storage.CalendarDate{
				&storage.CalendarDate{
					ServiceID:     "s1",
					Date:          "20170101",
					ExceptionType: 1,
				},
			},
			"20170101",
			"20170101",
			false,
		},

		{
			"several",
			`
service_id,date,exception_type
s1,20170101,1
s1,20170102,2
s2,20170103,1`,
			[]*storage.CalendarDate{
				&storage.CalendarDate{
					ServiceID:     "s1",
					Date:          "20170101",
					ExceptionType: 1,
				},
				&storage.CalendarDate{
					ServiceID:     "s1",
					Date:          "20170102",
					ExceptionType: 2,
				},
				&storage.CalendarDate{
					ServiceID:     "s2",
					Date:          "20170103",
					ExceptionType: 1,
				},
			},
			"20170101",
			"20170103",
			false,
		},

		{
			"invalid date",
			`
service_id,date,exception_type
s1,20170141,1`,
			nil,
			"",
			"",
			true,
		},

		{
			"invalid exception type",
			`
service_id,date,exception_type
s1,20170101,3`,
			nil,
			"",
			"",
			true,
		},

		{
			"repeated service id and date",
			`
service_id,date,exception_type
s1,20170101,1
s1,20170101,2`,
			nil,
			"",
			"",
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := storage.NewMemoryStorage()
			writer, err := storage.GetWriter("test")
			require.NoError(t, err)

			serviceIDs, minDate, maxDate, err := ParseCalendarDates(writer, bytes.NewBufferString(tc.content))
			if tc.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			reader, err := storage.GetReader("test")
			require.NoError(t, err)
			cals, err := reader.CalendarDates()
			require.NoError(t, err)

			assert.Equal(t, len(tc.expected), len(cals))
			sort.Slice(cals, func(i, j int) bool {
				return cals[i].ServiceID < cals[j].ServiceID
			})
			assert.Equal(t, tc.expected, cals)
			for _, c := range cals {
				assert.True(t, serviceIDs[c.ServiceID])
			}

			assert.Equal(t, tc.minDate, minDate)
			assert.Equal(t, tc.maxDate, maxDate)
		})
	}
}
