package parse

import (
	"fmt"
	"io"

	"github.com/gocarina/gocsv"

	"tidbyt.dev/gtfs/storage"
)

type StopCSV struct {
	ID   string  `csv:"stop_id"`
	Code string  `csv:"stop_code"`
	Name string  `csv:"stop_name"`
	Desc string  `csv:"stop_desc"`
	Lat  float64 `csv:"stop_lat"`
	Lon  float64 `csv:"stop_lon"`
	// ZoneID        string  `csv:"zone_id"`
	URL           string `csv:"stop_url"`
	LocationType  int8   `csv:"location_type"`
	ParentStation string `csv:"parent_station"`
	// Timezone      string  `csv:"stop_timezone"`
	// WheelchairBoarding string `csv:"wheelchair_boarding"`
	// LevelID       string  `csv:"level_id"`
	PlatformCode string `csv:"platform_code"`
}

func ParseStops(writer storage.FeedWriter, data io.Reader) (map[string]bool, error) {
	stopCsv := []*StopCSV{}
	if err := gocsv.Unmarshal(data, &stopCsv); err != nil {
		return nil, fmt.Errorf("unmarshaling stops csv: %w", err)
	}

	stopIDs := map[string]bool{}
	parentRef := map[string]string{}
	for _, st := range stopCsv {
		if stopIDs[st.ID] {
			return nil, fmt.Errorf("repeated stop_id '%s'", st.ID)
		}
		stopIDs[st.ID] = true

		if st.ID == "" {
			return nil, fmt.Errorf("empty stop_id")
		}

		locationType := storage.LocationType(st.LocationType)

		if locationType != storage.LocationTypeGenericNode && locationType != storage.LocationTypeBoardingArea {
			// stop_name is "[o]ptional for locations which are
			// generic nodes (location_type=3) or boarding areas
			// (location_type=4)" and otherwise required
			if st.Name == "" {
				return nil, fmt.Errorf("empty stop_name for stop_id '%s'", st.ID)
			}

			// stop_lat and stop_lon are "[o]ptional for
			// locations which are generic nodes
			// (location_type=3) or boarding areas
			// (location_type=4)" and otherwise required.
			if st.Lat == 0 || st.Lon == 0 {
				return nil, fmt.Errorf("empty stop_lat or stop_lon for stop_id '%s'", st.ID)
			}
		}

		stop := storage.Stop{
			ID:            st.ID,
			Code:          st.Code,
			Name:          st.Name,
			Desc:          st.Desc,
			Lat:           st.Lat,
			Lon:           st.Lon,
			URL:           st.URL,
			LocationType:  locationType,
			ParentStation: st.ParentStation,
			PlatformCode:  st.PlatformCode,
		}

		if st.ParentStation != "" {
			parentRef[st.ID] = st.ParentStation
		}

		err := writer.WriteStop(&stop)
		if err != nil {
			return nil, fmt.Errorf("writing stop '%s': %w", st.ID, err)
		}
	}

	// verify stops referenced by parent_station exist
	for stopID, parentID := range parentRef {
		if !stopIDs[parentID] {
			return nil, fmt.Errorf("stop '%s' references unknown parent_station '%s'", stopID, parentID)
		}
	}

	return stopIDs, nil
}
