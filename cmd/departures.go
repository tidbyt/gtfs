package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var departuresCmd = &cobra.Command{
	Use:   "departures <stop_id>",
	Short: "Lists departures near a geographical location",
	Args:  cobra.ExactArgs(1),
	RunE:  departures,
}

var (
	window    time.Duration
	limit     int
	direction int
	routeID   string
)

func init() {
	departuresCmd.Flags().DurationVarP(&window, "window", "W", 15*time.Minute, "Time window to search for departures")
	departuresCmd.Flags().IntVarP(&limit, "limit", "l", -1, "Limit the number of departures returned")
	departuresCmd.Flags().IntVarP(&direction, "direction", "d", -1, "Restrict to a specific direction")
	departuresCmd.Flags().StringVarP(&routeID, "route", "r", "", "Restrict to a specific route")
}

func departures(cmd *cobra.Command, args []string) error {
	stopID := args[0]

	static, err := LoadStaticFeed(feedURL)
	if err != nil {
		return err
	}

	departures, err := static.Departures(stopID, time.Now(), window, limit, routeID, int8(direction), nil)
	if err != nil {
		return err
	}

	for _, departure := range departures {
		fmt.Printf("%s %s %s\n", departure.RouteID, departure.Time, departure.Headsign)
	}

	return nil
}
