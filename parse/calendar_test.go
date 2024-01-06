package parse

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidbyt.dev/gtfs/model"
	"tidbyt.dev/gtfs/storage"
)

func TestCalendar(t *testing.T) {
	for _, tc := range []struct {
		name     string
		content  string
		expected []model.Calendar
		minDate  string
		maxDate  string
		err      bool
	}{
		{
			"minimal",
			`
service_id,start_date,end_date
s,20170101,20170131`,

			[]model.Calendar{
				{
					ServiceID: "s",
					Weekday:   0,
					StartDate: "20170101",
					EndDate:   "20170131",
				},
			},
			"20170101",
			"20170131",
			false,
		},

		{
			"maximal",
			`
service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
s,1,1,1,1,1,1,1,20170101,20170131`,
			[]model.Calendar{
				{
					ServiceID: "s",
					Weekday:   127,
					StartDate: "20170101",
					EndDate:   "20170131",
				},
			},
			"20170101",
			"20170131",
			false,
		},

		{
			"multiple services",
			`
service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
s1,1,1,1,1,1,1,1,20170101,20170131
s2,1,1,1,1,1,0,0,20171001,20180201
s3,1,1,0,1,1,0,1,20161225,20170202`,
			[]model.Calendar{
				{
					ServiceID: "s1",
					Weekday:   127,
					StartDate: "20170101",
					EndDate:   "20170131",
				},
				{
					ServiceID: "s2",
					Weekday:   127 ^ (1 << time.Saturday) ^ (1 << time.Sunday),
					StartDate: "20171001",
					EndDate:   "20180201",
				},
				{
					ServiceID: "s3",
					Weekday:   127 ^ (1 << time.Wednesday) ^ (1 << time.Saturday),
					StartDate: "20161225",
					EndDate:   "20170202",
				},
			},
			"20161225",
			"20180201",
			false,
		},

		{
			"invalid weekday",
			`
service_id,monday,tuesday,start_date,end_date
s,1,3,20170101,20170131`,
			nil, "", "", true,
		},

		{
			"malformed weekday",
			`
service_id,thursday,start_date,end_date
s,X,20170101,20170131`,
			nil, "", "", true,
		},

		{
			"invalid date",
			`
service_id,monday,tuesday,start_date,end_date
s,1,1,20170101,20170132`,
			nil, "", "", true,
		},

		{
			"repeated service_id",
			`
service_id,monday,tuesday,start_date,end_date
s,1,1,20170101,20170131
s,1,1,20170101,20170131`,
			nil, "", "", true,
		},

		{
			"missing service_id",
			`
monday,tuesday,start_date,end_date
1,1,20170101,20170131`,
			nil, "", "", true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage, err := storage.NewSQLiteStorage()
			require.NoError(t, err)
			writer, err := storage.GetWriter("test")
			require.NoError(t, err)

			serviceIDs, minDate, maxDate, err := ParseCalendar(writer, bytes.NewBufferString(tc.content))
			if tc.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			reader, err := storage.GetReader("test")
			require.NoError(t, err)
			cals, err := reader.Calendars()
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
