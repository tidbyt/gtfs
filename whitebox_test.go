package gtfs

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Don't love this, but some internal functions are finicky and need
// testing.

func TestWhiteboxRangePerDate(t *testing.T) {
	tzET, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// Eastern daylight savings started March 12th, 2023. At 2PM
	// it became 3PM.

	// Eastern standard time started November 5th, 2023. At 2AM
	// it became 1AM.

	for _, tc := range []struct {
		Name     string
		Start    time.Time
		Window   time.Duration
		Max      time.Duration
		Expected []span
	}{
		{
			"simple",
			time.Date(2023, 2, 3, 6, 0, 0, 0, tzET),
			30 * time.Minute,
			1 * time.Hour,
			[]span{{"20230203", "060000", "063000"}},
		},

		{
			"past midnight",
			time.Date(2023, 2, 3, 6, 0, 0, 0, tzET),
			30 * time.Hour,
			1 * time.Hour,
			[]span{
				{"20230203", "060000", ""},
				{"20230204", "", "120000"},
			},
		},

		{
			"past midnight, with change to daylight savings time",
			time.Date(2023, 3, 11, 6, 0, 0, 0, tzET),
			30 * time.Hour,
			1 * time.Hour,
			[]span{
				{"20230311", "060000", ""},
				{"20230312", "", "130000"},
			},
		},

		{
			"past midnight, with change to standard time",
			time.Date(2023, 11, 4, 6, 0, 0, 0, tzET),
			30 * time.Hour,
			1 * time.Hour,
			[]span{
				{"20231104", "060000", ""},
				{"20231105", "", "110000"},
			},
		},

		{
			"multiple days",
			time.Date(2023, 2, 3, 6, 0, 0, 0, tzET),
			49 * time.Hour,
			1 * time.Hour,
			[]span{
				{"20230203", "060000", ""},
				{"20230204", "", ""},
				{"20230205", "", "070000"},
			},
		},

		{
			"maxTrip indicating overflow from previous day",
			time.Date(2023, 2, 3, 6, 0, 0, 0, tzET),
			2 * time.Hour,
			(24 + 7) * time.Hour,
			[]span{
				{"20230202", "300000", ""},
				{"20230203", "060000", "080000"},
			},
		},

		{
			"overflow precisely touching range",
			time.Date(2023, 2, 3, 6, 0, 0, 0, tzET),
			2 * time.Hour,
			(24 + 6) * time.Hour,
			[]span{
				{"20230202", "300000", ""},
				{"20230203", "060000", "080000"},
			},
		},

		{
			"multi day with overflow reaching end of range",
			time.Date(2023, 2, 3, 6, 0, 0, 0, tzET),
			(48+18)*time.Hour + 30*time.Minute,
			(24 + 1) * time.Hour,
			[]span{
				{"20230203", "060000", ""},
				{"20230204", "", ""},
				{"20230205", "", "243000"},
				{"20230206", "", "003000"},
			},
		},

		{
			"overflow with change to standard time",
			time.Date(2023, 11, 5, 00, 59, 0, 0, tzET),
			2 * time.Minute,
			29 * time.Hour,
			[]span{
				{"20231104", "245900", "250100"},
				{"20231105", "", "000100"},
			},
		},

		{
			"overflow with change to daylight savings",
			time.Date(2023, 3, 12, 00, 59, 0, 0, tzET),
			2 * time.Minute,
			29 * time.Hour,
			[]span{
				{"20230311", "245900", "250100"},
				{"20230312", "015900", "020100"},
			},
		},

		// NOTE: DST changes are really annoying. Very likely
		// these tests are missing some edge cases.

	} {
		t.Run(tc.Name, func(t *testing.T) {
			spans := rangePerDate(tc.Start, tc.Window, tc.Max)
			assert.Equal(t, tc.Expected, spans)
		})
	}
}

func TestWhiteboxDelayFromOffsetAndTime(t *testing.T) {

	// TODO: Would be great to flesh this out.

	for _, tc := range []struct {
		tz            *time.Location
		eventOffset   string
		updateTime    string
		expectedDelay string
	}{
		{time.UTC, "23h6m", "2020-01-15 23:05:55 +0000 UTC", "-5s"},
		{time.UTC, "23h11m", "2020-01-15 23:11:25 +0000 UTC", "25s"},
		{time.UTC, "24h10m", "2020-01-01 00:09:00 +0000 UTC", "-1m"},
		{time.UTC, "24h10m", "2020-01-01 23:50:00 +0000 UTC", "-20m"},
		{time.UTC, "24h10m", "2020-01-01 00:11:00 +0000 UTC", "1m"},
	} {
		offset, err := time.ParseDuration(tc.eventOffset)
		require.NoError(t, err)
		time_, err := time.Parse("2006-01-02 15:04:05 -0700 MST", tc.updateTime)
		require.NoError(t, err)
		delay, err := time.ParseDuration(tc.expectedDelay)
		require.NoError(t, err)

		actual := delayFromOffsetAndTime(tc.tz, offset, time_)
		assert.Equal(
			t, delay, actual,
			fmt.Sprintf(
				"%s %s, expect %s, got %s",
				tc.eventOffset, tc.updateTime, tc.expectedDelay, actual,
			),
		)
	}
}
