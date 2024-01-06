package parse

import (
	"fmt"
	"io"
	"time"

	"github.com/gocarina/gocsv"

	"tidbyt.dev/gtfs/model"
	"tidbyt.dev/gtfs/storage"
)

type CalendarDateCSV struct {
	ServiceID     string `csv:"service_id"`
	Date          string `csv:"date"`
	ExceptionType int8   `csv:"exception_type"`
}

func ParseCalendarDates(
	writer storage.FeedWriter,
	data io.Reader,
) (map[string]bool, string, string, error) {

	calendarDateCsv := []*CalendarDateCSV{}
	if err := gocsv.Unmarshal(data, &calendarDateCsv); err != nil {
		return nil, "", "", fmt.Errorf("unmarshaling calendar_dates csv: %w", err)
	}

	knownService := map[string]bool{}
	knownServiceDate := map[string]bool{}
	var minDate, maxDate string

	for _, cd := range calendarDateCsv {
		if cd.ExceptionType < 1 || cd.ExceptionType > 2 {
			return nil, "", "", fmt.Errorf("illegal exception_type: '%d'", cd.ExceptionType)
		}

		_, err := time.ParseInLocation("20060102", cd.Date, time.UTC)
		if err != nil {
			return nil, "", "", fmt.Errorf("parsing date '%s': %w", cd.Date, err)
		}

		serviceDate := fmt.Sprintf("%s-%s", cd.Date, cd.ServiceID)
		if knownServiceDate[serviceDate] {
			return nil, "", "", fmt.Errorf("duplicate service/date: '%s'", serviceDate)
		}
		knownServiceDate[serviceDate] = true
		knownService[cd.ServiceID] = true

		if minDate == "" || cd.Date < minDate {
			minDate = cd.Date
		}
		if maxDate == "" || cd.Date > maxDate {
			maxDate = cd.Date
		}

		writer.WriteCalendarDate(&model.CalendarDate{
			ServiceID:     cd.ServiceID,
			Date:          cd.Date,
			ExceptionType: model.ExceptionType(cd.ExceptionType),
		})
	}

	return knownService, minDate, maxDate, nil
}
