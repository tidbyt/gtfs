package gtfs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Don't love this, but some internal functions are finicky and need
// testing.

func TestStaticRangePerDate(t *testing.T) {
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

		// TODO: write a test of overflow with DST change
	} {
		t.Run(tc.Name, func(t *testing.T) {
			spans := rangePerDate(tc.Start, tc.Window, tc.Max)
			assert.Equal(t, tc.Expected, spans)
		})
	}
}
