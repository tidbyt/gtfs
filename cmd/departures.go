package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"tidbyt.dev/gtfs/model"
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

	type DepartureProvider interface {
		Departures(string, time.Time, time.Duration, int, string, int8, []model.RouteType) ([]model.Departure, error)
	}

	var provider DepartureProvider
	var err error
	if realtimeURL != "" {
		provider, err = LoadRealtimeFeed()
	} else {
		provider, err = LoadStaticFeed()
	}

	if err != nil {
		return err
	}

	departures, err := provider.Departures(stopID, time.Now(), window, limit, routeID, int8(direction), nil)
	if err != nil {
		return err
	}

	for _, departure := range departures {
		delay := ""
		if departure.Delay != 0 {
			delay = fmt.Sprintf(" (%s)", departure.Delay)
		}
		fmt.Printf(
			"%s%s - %s - %s\n",
			departure.Time.Format("15:04:05"),
			delay,
			departure.RouteID,
			departure.Headsign,
		)
	}

	return nil
}
