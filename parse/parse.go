package parse

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/spkg/bom"

	"tidbyt.dev/gtfs/storage"
)

func ParseStatic(writer storage.FeedWriter, buf []byte) (*storage.FeedMetadata, error) {
	// These are the files we load for static dumps.
	//
	// TODO: add feed_info.txt
	file := map[string]io.ReadCloser{
		"agency.txt":         nil,
		"routes.txt":         nil,
		"stops.txt":          nil,
		"trips.txt":          nil,
		"stop_times.txt":     nil,
		"calendar.txt":       nil,
		"calendar_dates.txt": nil,
	}

	defer func() {
		for _, rc := range file {
			if rc != nil {
				rc.Close()
			}
		}
	}()

	r, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return nil, fmt.Errorf("unzipping: %w", err)
	}

	for _, f := range r.File {
		// There should not be any subdirectories. But, some
		// agencies don't care.
		if f.FileInfo().IsDir() {
			continue
		}
		path := strings.Split(f.Name, "/")
		fName := path[len(path)-1]

		if _, found := file[fName]; !found {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("opening %s: %w", f.Name, err)
		}

		file[fName] = rc
	}

	if file["calendar.txt"] == nil && file["calendar_dates.txt"] == nil {
		return nil, fmt.Errorf("missing calendar.txt and calendar_dates.txt")
	}

	for _, required := range []string{"agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt"} {
		if file[required] == nil {
			return nil, fmt.Errorf("missing %s", required)
		}
	}

	// LazyCSVReader required (at least) to survive sloppy use of
	// quotes. The BOM reader strips unicode BOMs if present.
	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		return gocsv.LazyCSVReader(bom.NewReader(in))
	})

	// Parse agency.txt. Extract timezone and set of agency IDs in
	// the process.
	agency, timezone, err := ParseAgency(writer, file["agency.txt"])
	if err != nil {
		return nil, fmt.Errorf("parsing agency.txt: %w", err)
	}

	// Parse routes.txt. Extract route IDs in the process.
	routes, err := ParseRoutes(writer, file["routes.txt"], agency)
	if err != nil {
		return nil, fmt.Errorf("parsing routes.txt: %w", err)
	}

	// Parse calendar.txt and calendar_dates.txt. Extract set of
	// all service IDs, and min/max date of services seen.
	var calendarStart, calendarEnd string
	services := map[string]bool{}
	if file["calendar.txt"] != nil {
		services, calendarStart, calendarEnd, err = ParseCalendar(writer, file["calendar.txt"])
		if err != nil {
			return nil, fmt.Errorf("parsing calendar.txt: %w", err)
		}
	}
	if file["calendar_dates.txt"] != nil {
		cdServices, minDate, maxDate, err := ParseCalendarDates(writer, file["calendar_dates.txt"])
		if err != nil {
			return nil, fmt.Errorf("parsing calendar_dates.txt: %w", err)
		}
		for serviceID := range cdServices {
			services[serviceID] = true
		}
		if calendarStart == "" || minDate < calendarStart {
			calendarStart = minDate
		}
		if calendarEnd == "" || maxDate > calendarEnd {
			calendarEnd = maxDate
		}
	}

	// Parse trips.txt. Extract trip IDs in the process.
	err = writer.BeginTrips()
	if err != nil {
		return nil, fmt.Errorf("beginning trips: %w", err)
	}
	trips, err := ParseTrips(writer, file["trips.txt"], routes, services)
	if err != nil {
		return nil, fmt.Errorf("parsing trips.txt: %w", err)
	}
	err = writer.EndTrips()
	if err != nil {
		return nil, fmt.Errorf("ending trips: %w", err)
	}

	// And parse stop_times.txt. Extract stop IDs in the process.
	stops, err := ParseStops(writer, file["stops.txt"])
	if err != nil {
		return nil, fmt.Errorf("parsing stops.txt: %w", err)
	}

	// Parse stop_times.txt.
	err = writer.BeginStopTimes()
	if err != nil {
		return nil, fmt.Errorf("beginning stop_times: %w", err)
	}
	maxArrival, maxDeparture, err := ParseStopTimes(writer, file["stop_times.txt"], trips, stops)
	if err != nil {
		return nil, fmt.Errorf("parsing stop_times.txt: %w", err)
	}
	err = writer.EndStopTimes()
	if err != nil {
		return nil, fmt.Errorf("ending stop_times: %w", err)
	}

	// All files parsed: close the writer.
	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("closing feed writer: %w", err)
	}

	// And return a (partial) metadata holding some key
	// information about the feed.
	return &storage.FeedMetadata{
		CalendarStartDate: calendarStart,
		CalendarEndDate:   calendarEnd,
		Timezone:          timezone,
		MaxArrival:        maxArrival,
		MaxDeparture:      maxDeparture,
	}, nil
}
