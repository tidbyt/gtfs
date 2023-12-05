package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/spf13/cobra"
)

var stopsCmd = &cobra.Command{
	Use:   "stops [lat lng] [limit]",
	Short: "Lists stops near a geographical location",
	Args:  cobra.RangeArgs(0, 3),
	RunE:  stops,
}

func init() {
	rootCmd.AddCommand(stopsCmd)
}

func stops(cmd *cobra.Command, args []string) error {
	var lat, lng float64
	var limit int
	var err error

	gotLocation := false
	if len(args) == 1 {
		return fmt.Errorf("missing lng")
	}
	if len(args) >= 2 {
		gotLocation = true
		lat, err = strconv.ParseFloat(args[0], 64)
		if err != nil {
			return fmt.Errorf("invalid lat: %w", err)
		}
		lng, err = strconv.ParseFloat(args[1], 64)
		if err != nil {
			return fmt.Errorf("invalid lng: %w", err)
		}
	}
	if len(args) == 3 {
		limit, err = strconv.Atoi(args[2])
		if err != nil {
			return fmt.Errorf("invalid limit: %w", err)
		}
		if limit < 0 {
			return fmt.Errorf("limit must be >= 0")
		}
	}

	static, err := LoadStaticFeed(feedURL)
	if err != nil {
		return err
	}

	stops, err := static.NearbyStops(lat, lng, limit, nil)
	if err != nil {
		return err
	}

	if !gotLocation {
		// sort by name
		sort.Slice(stops, func(i, j int) bool {
			return stops[i].Name < stops[j].Name
		})
	}

	for _, stop := range stops {
		fmt.Printf("%s: %s\n", stop.ID, stop.Name)
	}

	return nil
}
