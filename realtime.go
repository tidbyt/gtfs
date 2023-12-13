package gtfs

import (
	"context"
	"fmt"
	"sort"
	"time"

	"tidbyt.dev/gtfs/parse"
	"tidbyt.dev/gtfs/storage"
)

type Realtime struct {
	static *Static
	reader storage.FeedReader

	updatesByTrip map[string][]RealtimeUpdate
	skippedTrips  map[string]bool
}

// Similar to parse.StopTimeUpdate, but trimmed down to what's
// necessary to serve realtime predictions. Should be suitable for
// caching and sharing with other instances.
type RealtimeUpdate struct {
	ArrivalDelay   time.Duration
	DepartureDelay time.Duration
	StopSequence   uint32
	Type           parse.StopTimeUpdateScheduleRelationship
}

func NewRealtime(static *Static, reader storage.FeedReader) *Realtime {
	return &Realtime{
		static: static,
		reader: reader,
	}
}

func (rt *Realtime) LoadData(ctx context.Context, feedData [][]byte) error {
	realtime, err := parse.ParseRealtime(ctx, feedData)
	if err != nil {
		return fmt.Errorf("parsing realtime feeds: %w", err)
	}

	rt.skippedTrips = realtime.SkippedTrips

	// Retrieve Static stop time events for all trips in the realtime feed
	trips := map[string]bool{}
	for _, update := range realtime.Updates {
		trips[update.TripID] = true
	}
	tripIDs := []string{}
	for tripID := range trips {
		tripIDs = append(tripIDs, tripID)
	}

	events, err := rt.reader.StopTimeEvents(storage.StopTimeEventFilter{
		DirectionID: -1,
		TripIDs:     tripIDs,
	})
	if err != nil {
		return fmt.Errorf("loading stop time events: %w", err)
	}

	// And the static feed's timezone
	timezone, err := time.LoadLocation(rt.static.Metadata.Timezone)
	if err != nil {
		return fmt.Errorf("loading static timezone: %w", err)
	}

	// Infer missing stop_id/stop_sequence from static data
	resolveStopReferences(realtime.Updates, events)

	// Construct RealtimeUpdate objects from the parsed
	// StopTimeUpdates.
	rt.updatesByTrip = buildRealtimeUpdates(timezone, realtime.Updates, events)

	return nil
}

func (rt *Realtime) Departures(
	stopID string,
	windowStart time.Time,
	windowLength time.Duration,
	numDepartures int,
	routeID string,
	directionID int8,
	routeTypes []storage.RouteType) ([]Departure, error) {

	// Get the static schedule for the requested time window.
	scheduled, err := rt.static.Departures(
		stopID,
		windowStart,
		windowLength,
		numDepartures,
		routeID,
		directionID,
		routeTypes,
	)
	if err != nil {
		return nil, fmt.Errorf("getting static departures: %w", err)
	}

	// Process each scheduled departure, applying realtime updates
	departures := []Departure{}
	for _, dep := range scheduled {

		// If trip is cancelled, the the departure is too
		if rt.skippedTrips[dep.TripID] {
			continue
		}

		// Get all updates for this trip
		updates, found := rt.updatesByTrip[dep.TripID]
		if !found || len(updates) == 0 {
			// None provided, so schedule applies
			departures = append(departures, dep)
			continue
		}

		// In GTFS-rt, when no other data is provided,
		// previous delays along a trip have to be propagated
		// to later stops. This searches for the first update
		// that applies to a _later_ stop.
		idx := sort.Search(len(updates), func(i int) bool {
			return updates[i].StopSequence > dep.StopSequence
		})

		// And this places index to the update (if any) that
		// applies to this stop.
		idx--

		// If none is available, the static schedule applies
		if idx < 0 {
			departures = append(departures, dep)
			continue
		}

		if updates[idx].Type == parse.StopTimeUpdateSkipped {
			// If this specific stop is skipped, then
			// the departure should be ignored
			if updates[idx].StopSequence == dep.StopSequence {
				continue
			}

			// If the skipped stop was earlier on the
			// trip, then keep searching for the first
			// non-skipped stop
			for idx >= 0 && updates[idx].Type == parse.StopTimeUpdateSkipped {
				idx--
			}

			// Again, if no (non-skipped) update exists,
			// then the static schedule applies
			if idx < 0 {
				departures = append(departures, dep)
				continue
			}
		}

		// The idx now points to the update that applies. This
		// may be for a prior stop, where the delay should be
		// propagated forward.

		switch updates[idx].Type {
		case parse.StopTimeUpdateNoData:
			// NO_DATA => rely on to static schedule
			departures = append(departures, dep)
		case parse.StopTimeUpdateScheduled:
			// SCHEDULED => update to static schedule
			if updates[idx].DepartureDelay > 0 {
				dep.Time = dep.Time.Add(updates[idx].DepartureDelay)
			}

			// If the delay pushed the departure outside
			// of the requested time window, it must be
			// ignored
			if dep.Time.Before(windowStart) || dep.Time.After(windowStart.Add(windowLength)) {
				continue
			}

			departures = append(departures, dep)
		}
	}

	sort.Slice(departures, func(i, j int) bool {
		return departures[i].Time.Before(departures[j].Time)
	})

	return departures, nil

	// Missing:
	//
	//  - Trips pushed into the time window by a delay
	//  - Added trips
	//  - Added stops (is that a thing?)
	//
	//  - Early arrival should cancel propagation of (positive)
	//  delays (see TestRealtimeArrivalRecovery) in internal gtfs
	//  lib for motivation.
	//
	//  I suspect some agencies will only pass arrival OR
	//  departure info. There should be some logic to translate
	//  between the two. E.g., if train departs late from previous
	//  stop, and departs late from current stop, it seems obvious
	//  that arrival should also be considered late.

}

// Updates all updates to have both stop_id and stop_sequence set.
//
// GTFS-rt's StopTimeUpdates can rereference stops using stop_id,
// stop_sequence, or both. We absolutely need stop_sequence to handle
// propagation of delay, and it seems likely we'll need stop_id as
// well to handle added stops/trips in the future.
//
// This function takes StopTimeUpdates from a realtime feed, along
// with all static StopTimeEvents for the associated trips, and
// updates the updates so that stop_id and stop_sequence is set on
// all.
func resolveStopReferences(updates []*parse.StopTimeUpdate, events []*storage.StopTimeEvent) {
	// Map to resolve stop_id from stop_sequence
	type tripAndSeq struct {
		tripID string
		seq    uint32
	}
	stopIDByTripAndSeq := map[tripAndSeq]string{}
	for _, event := range events {
		stopIDByTripAndSeq[tripAndSeq{event.Trip.ID, event.StopTime.StopSequence}] = event.Stop.ID
	}

	// Map to resolve stop_sequence from stop_id
	type tripAndStopID struct {
		tripID string
		stopID string
	}
	stopSeqByTripAndStopID := map[tripAndStopID]uint32{}
	for _, event := range events {
		stopSeqByTripAndStopID[tripAndStopID{event.Trip.ID, event.Stop.ID}] = event.StopTime.StopSequence
	}

	for _, update := range updates {
		if update.StopID != "" {
			// Got stop_id. StopSequence 0 could be legit, or it
			// could be unspecified. At last attempt to
			// resolve in this case.
			if update.StopSequence == 0 {
				stopSeq, ok := stopSeqByTripAndStopID[tripAndStopID{update.TripID, update.StopID}]
				if ok {
					update.StopSequence = stopSeq
				}
			}
			continue
		}

		// No stop_id. Must be inferred from stop_sequence.
		stopID, ok := stopIDByTripAndSeq[tripAndSeq{update.TripID, update.StopSequence}]
		if ok {
			update.StopID = stopID
		}
	}
}

// The full GTFS-rt StopTimeUpdates are great and all, but we only
// need some of the information they hold.
//
// This function takes StopTimeUpdates from a realtime feed, along
// with all associated static StopTimeEvents, and returns
// RealtimeUpdates, grouped by trip, and sorted by stop_sequence.
func buildRealtimeUpdates(
	timezone *time.Location,
	stups []*parse.StopTimeUpdate,
	events []*storage.StopTimeEvent,
) map[string][]RealtimeUpdate {

	// Group static events by trip, and sort by stop_sequence
	eventsByTrip := map[string][]*storage.StopTimeEvent{}
	for _, event := range events {
		eventsByTrip[event.Trip.ID] = append(eventsByTrip[event.Trip.ID], event)
	}
	for _, events := range eventsByTrip {
		sort.Slice(events, func(i, j int) bool {
			return events[i].StopTime.StopSequence < events[j].StopTime.StopSequence
		})
	}

	// Group updates in the same manner
	updatesByTrip := map[string][]*parse.StopTimeUpdate{}
	for _, update := range stups {
		updatesByTrip[update.TripID] = append(updatesByTrip[update.TripID], update)
	}
	for _, updates := range updatesByTrip {
		sort.Slice(updates, func(i, j int) bool {
			return updates[i].StopSequence < updates[j].StopSequence
		})
	}

	// Computes delay of an update, given the correspnding time
	// from static schedule.
	updateDelay := func(eventOffset time.Duration, updateTime time.Time) time.Duration {
		upTime := updateTime.In(timezone)
		upNoon := time.Date(upTime.Year(), upTime.Month(), upTime.Day(), 12, 0, 0, 0, timezone)

		// Static schdule can have time exceeding 24h, in
		// which case we need this adjustment to take
		// potential DST switch into account.
		if eventOffset >= 24*time.Hour {
			upNoon = upNoon.AddDate(0, 0, -1)
		}
		eventTime := upNoon.Add(-12 * time.Hour).Add(eventOffset)

		return upTime.Sub(eventTime)
	}

	realtimeUpdates := map[string][]RealtimeUpdate{}

	// Combine static schedule and realtime updates
	for tripID, tripUpdates := range updatesByTrip {
		events, found := eventsByTrip[tripID]
		if !found {
			// TODO: Added trips are not handled yet
			continue
		}

		ei := 0
		for _, u := range tripUpdates {
			// Find event matching this update's stop_sequence
			for ; ei < len(events); ei++ {
				if events[ei].StopTime.StopSequence == u.StopSequence {
					break
				}
			}
			if ei >= len(events) {
				break
			}

			// A NO_DATA update means we should fall back
			// to static schedule. In this model, that
			// means delays of 0s. A SKIPPED update means
			// the stop should be skipped. No need to
			// attach delays information.
			if u.Type == parse.StopTimeUpdateNoData || u.Type == parse.StopTimeUpdateSkipped {
				realtimeUpdates[tripID] = append(realtimeUpdates[tripID], RealtimeUpdate{
					StopSequence: u.StopSequence,
					Type:         u.Type,
				})
				continue
			}

			// Type is SCHEDULED. Compute delays.
			rtUp := RealtimeUpdate{
				StopSequence: u.StopSequence,
				Type:         u.Type,
			}

			if u.ArrivalIsSet {
				// If exact time is provided, it takes
				// precedence over delay.
				rtUp.ArrivalDelay = u.ArrivalDelay
				if !u.ArrivalTime.IsZero() {
					rtUp.ArrivalDelay = updateDelay(
						events[ei].StopTime.ArrivalTime(),
						u.ArrivalTime,
					)
				}
			}
			if u.DepartureIsSet {
				// Same here: if exact time is
				// provided, it takes precedene over
				// delay.
				rtUp.DepartureDelay = u.DepartureDelay
				if !u.DepartureTime.IsZero() {
					rtUp.DepartureDelay = updateDelay(
						events[ei].StopTime.DepartureTime(),
						u.DepartureTime,
					)
				}
			} else {
				// Lacking Departure data, assume
				// arrival delay applies to departure
				rtUp.DepartureDelay = u.ArrivalDelay
			}

			realtimeUpdates[tripID] = append(realtimeUpdates[tripID], rtUp)
		}
	}

	return realtimeUpdates
}
