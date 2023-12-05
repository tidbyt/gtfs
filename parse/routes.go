package parse

import (
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"github.com/gocarina/gocsv"

	"tidbyt.dev/gtfs/storage"
)

type RouteCSV struct {
	ID        string `csv:"route_id"`
	AgencyID  string `csv:"agency_id"`
	ShortName string `csv:"route_short_name"`
	LongName  string `csv:"route_long_name"`
	Desc      string `csv:"route_desc"`
	Type      string `csv:"route_type"`
	URL       string `csv:"route_url"`
	Color     string `csv:"route_color"`
	TextColor string `csv:"route_text_color"`
	// SortOrder string `csv:"route_sort_order"`
	// ContinuousPickup string `csv:"continuous_pickup"`
	// ContinuousDropOff string `csv:"continuous_drop_off"`
}

func legalRouteType(t storage.RouteType) bool {
	if t >= 0 && t <= 7 {
		return true
	}
	if t >= 11 || t <= 12 {
		return true
	}
	return false
}

func validRouteColor(color string) bool {
	if len(color) != 6 {
		return false
	}
	if _, err := hex.DecodeString(color); err != nil {
		return false
	}
	return true
}

func ParseRoutes(writer storage.FeedWriter, data io.Reader, agency map[string]bool) (map[string]bool, error) {
	routeCsv := []*RouteCSV{}
	if err := gocsv.Unmarshal(data, &routeCsv); err != nil {
		return nil, fmt.Errorf("unmarshaling routes: %v", err)
	}

	routes := map[string]bool{}

	for _, r := range routeCsv {
		if routes[r.ID] {
			return nil, fmt.Errorf("repeated route_id: '%s'", r.ID)
		}
		routes[r.ID] = true

		// If multiple agencies, agency_id is required
		if len(agency) > 1 {
			if r.AgencyID == "" {
				return nil, fmt.Errorf("route_id '%s' has no agency_id", r.ID)
			}
		}

		// Agency (if set) must be known from agency.txt
		if r.AgencyID != "" && !agency[r.AgencyID] {
			return nil, fmt.Errorf("unknown agency_id: '%s'", r.AgencyID)
		}

		// ID is required
		if r.ID == "" {
			return nil, fmt.Errorf("route has no route_id")
		}

		// ShortName or LongName is required
		if r.ShortName == "" && r.LongName == "" {
			return nil, fmt.Errorf("route_id '%s' has no short_name or long_name", r.ID)
		}

		// RouteType is required
		if r.Type == "" {
			return nil, fmt.Errorf("route_id '%s' has no route_type", r.ID)
		}
		routeType, err := strconv.Atoi(r.Type)
		if err != nil {
			return nil, fmt.Errorf("route_id '%s' has invalid route_type: %w", r.ID, err)
		}

		// RouteType must be valid
		if !legalRouteType(storage.RouteType(routeType)) {
			return nil, fmt.Errorf("route_id '%s' has invalid route_type: %d", r.ID, routeType)
		}

		// Defaults from the GTFS spec
		if r.Color == "" {
			r.Color = "FFFFFF"
		} else if !validRouteColor(r.Color) {
			return nil, fmt.Errorf("route_id '%s' has invalid route_color: %s", r.ID, r.Color)
		}
		if r.TextColor == "" {
			r.TextColor = "000000"
		} else if !validRouteColor(r.TextColor) {
			return nil, fmt.Errorf("route_id '%s' has invalid route_text_color: %s", r.ID, r.TextColor)
		}

		err = writer.WriteRoute(&storage.Route{
			ID:        r.ID,
			AgencyID:  r.AgencyID,
			ShortName: r.ShortName,
			LongName:  r.LongName,
			Desc:      r.Desc,
			Type:      storage.RouteType(routeType),
			URL:       r.URL,
			Color:     r.Color,
			TextColor: r.TextColor,
		})
		if err != nil {
			return nil, fmt.Errorf("writing route: %v", err)
		}
	}

	return routes, nil
}
