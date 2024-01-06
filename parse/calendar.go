package parse

import (
	"fmt"
	"io"
	"time"

	"github.com/gocarina/gocsv"

	"tidbyt.dev/gtfs/model"
	"tidbyt.dev/gtfs/storage"
)

type CalendarCSV struct {
	ServiceID string `csv:"service_id"`
	StartDate string `csv:"start_date"`
	EndDate   string `csv:"end_date"`
	Monday    int8   `csv:"monday"`
	Tuesday   int8   `csv:"tuesday"`
	Wednesday int8   `csv:"wednesday"`
	Thursday  int8   `csv:"thursday"`
	Friday    int8   `csv:"friday"`
	Saturday  int8   `csv:"saturday"`
	Sunday    int8   `csv:"sunday"`
}

// Returns set of all service IDs, min date and max date.
func ParseCalendar(writer storage.FeedWriter, data io.Reader) (map[string]bool, string, string, error) {
	calendarCsv := []*CalendarCSV{}
	if err := gocsv.Unmarshal(data, &calendarCsv); err != nil {
		return nil, "", "", fmt.Errorf("unmarshaling csv: %w", err)
	}

	knownServices := map[string]bool{}

	var minDate, maxDate string

	for _, c := range calendarCsv {
		if knownServices[c.ServiceID] {
			return nil, "", "", fmt.Errorf("repeated service_id '%s'", c.ServiceID)
		}
		knownServices[c.ServiceID] = true

		if c.ServiceID == "" {
			return nil, "", "", fmt.Errorf("empty service_id")
		}

		var weekday int8
		weekday = 0
		if c.Monday == 1 {
			weekday |= 1 << time.Monday
		} else if c.Monday != 0 {
			return nil, "", "", fmt.Errorf("invalid monday value '%d'", c.Monday)
		}
		if c.Tuesday == 1 {
			weekday |= 1 << time.Tuesday
		} else if c.Tuesday != 0 {
			return nil, "", "", fmt.Errorf("invalid tuesday value '%d'", c.Tuesday)
		}
		if c.Wednesday == 1 {
			weekday |= 1 << time.Wednesday
		} else if c.Wednesday != 0 {
			return nil, "", "", fmt.Errorf("invalid wednesday value '%d'", c.Wednesday)
		}
		if c.Thursday == 1 {
			weekday |= 1 << time.Thursday
		} else if c.Thursday != 0 {
			return nil, "", "", fmt.Errorf("invalid thursday value '%d'", c.Thursday)
		}
		if c.Friday == 1 {
			weekday |= 1 << time.Friday
		} else if c.Friday != 0 {
			return nil, "", "", fmt.Errorf("invalid friday value '%d'", c.Friday)
		}
		if c.Saturday == 1 {
			weekday |= 1 << time.Saturday
		} else if c.Saturday != 0 {
			return nil, "", "", fmt.Errorf("invalid saturday value '%d'", c.Saturday)
		}
		if c.Sunday == 1 {
			weekday |= 1 << time.Sunday
		} else if c.Sunday != 0 {
			return nil, "", "", fmt.Errorf("invalid sunday value '%d'", c.Sunday)
		}

		_, err := time.ParseInLocation("20060102", c.StartDate, time.UTC)
		if err != nil {
			return nil, "", "", fmt.Errorf("parsing start_date: %w", err)
		}

		_, err = time.ParseInLocation("20060102", c.EndDate, time.UTC)
		if err != nil {
			return nil, "", "", fmt.Errorf("parsing end_date: %w", err)
		}

		if minDate == "" || c.StartDate < minDate {
			minDate = c.StartDate
		}
		if maxDate == "" || c.EndDate > maxDate {
			maxDate = c.EndDate
		}

		err = writer.WriteCalendar(&model.Calendar{
			ServiceID: c.ServiceID,
			StartDate: c.StartDate,
			EndDate:   c.EndDate,
			Weekday:   weekday,
		})
		if err != nil {
			return nil, "", "", fmt.Errorf("writing calendar: %w", err)
		}
	}

	return knownServices, minDate, maxDate, nil
}
