package parse

import (
	"fmt"
	"io"

	"github.com/gocarina/gocsv"

	"tidbyt.dev/gtfs/model"
	"tidbyt.dev/gtfs/storage"
)

type TripCSV struct {
	ID          string `csv:"trip_id"`
	RouteID     string `csv:"route_id"`
	ServiceID   string `csv:"service_id"`
	Headsign    string `csv:"trip_headsign"`
	ShortName   string `csv:"trip_short_name"`
	DirectionID int8   `csv:"direction_id"`
	// BlockID              string `csv:"block_id"`
	// ShapeID              string `csv:"shape_id"`
	// WheelchairAccessible int8   `csv:"wheelchair_accessible"`
	// BikesAllowed         int8   `csv:"bikes_allowed"`
}

func ParseTrips(
	writer storage.FeedWriter,
	data io.Reader,
	routes map[string]bool,
	services map[string]bool,
) (map[string]bool, error) {
	tripCsv := []*TripCSV{}
	if err := gocsv.Unmarshal(data, &tripCsv); err != nil {
		return nil, fmt.Errorf("unmarshaling trips csv: %w", err)
	}

	trips := map[string]bool{}
	for _, t := range tripCsv {
		if trips[t.ID] {
			return nil, fmt.Errorf("repeated trip_id '%s'", t.ID)
		}
		trips[t.ID] = true

		if t.ID == "" {
			return nil, fmt.Errorf("empty trip_id")
		}
		if t.RouteID == "" {
			return nil, fmt.Errorf("empty route_id")
		}

		if !routes[t.RouteID] {
			return nil, fmt.Errorf("unknown route_id '%s'", t.RouteID)
		}
		if !services[t.ServiceID] {
			return nil, fmt.Errorf("unknown service_id '%s'", t.ServiceID)
		}

		if t.DirectionID != 0 && t.DirectionID != 1 {
			return nil, fmt.Errorf("invalid direction_id '%d'", t.DirectionID)
		}

		err := writer.WriteTrip(model.Trip{
			ID:          t.ID,
			RouteID:     t.RouteID,
			ServiceID:   t.ServiceID,
			Headsign:    t.Headsign,
			ShortName:   t.ShortName,
			DirectionID: t.DirectionID,
		})
		if err != nil {
			return nil, fmt.Errorf("writing trip: %w", err)
		}
	}

	return trips, nil
}
