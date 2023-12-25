package parse

import (
	"context"
	"fmt"
	"time"

	gtfsproto "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	proto "google.golang.org/protobuf/proto"
)

type StopTimeUpdateScheduleRelationship int

const (
	StopTimeUpdateScheduled StopTimeUpdateScheduleRelationship = iota
	StopTimeUpdateSkipped
	StopTimeUpdateNoData
)

type StopTimeUpdate struct {
	TripID         string
	StopID         string
	StopSequence   uint32
	ArrivalIsSet   bool
	ArrivalTime    time.Time
	ArrivalDelay   time.Duration
	DepartureIsSet bool
	DepartureTime  time.Time
	DepartureDelay time.Duration
	Type           StopTimeUpdateScheduleRelationship
}

// Contains key data from a GTFS Realtime feed
type Realtime struct {
	// Timestamp of the feed. If loaded from multiple feeds, the
	// last one wins.
	Timestamp    uint64
	SkippedTrips map[string]bool
	Updates      []*StopTimeUpdate

	// These exist to simplify debugging down the road
	NumScheduledTrips   int
	NumAddedTrips       int
	NumUnscheduledTrips int
	NumCanceledTrips    int
	NumDuplicatedTrips  int
}

func ParseRealtime(ctx context.Context, feeds [][]byte) (*Realtime, error) {
	rt := &Realtime{
		SkippedTrips: map[string]bool{},
		Updates:      []*StopTimeUpdate{},
	}

	for _, feed := range feeds {
		// Unmarshal proto
		f := &gtfsproto.FeedMessage{}
		err := proto.Unmarshal(feed, f)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling protobuf: %w", err)
		}

		// Header
		header := f.GetHeader()

		version := header.GetGtfsRealtimeVersion()
		if version != "2.0" && version != "1.0" {
			return nil, fmt.Errorf("version %s not supported", version)
		}

		if header.GetIncrementality() != gtfsproto.FeedHeader_FULL_DATASET {
			return nil, fmt.Errorf("feed incrementality %s not supported", header.GetIncrementality())
		}

		rt.Timestamp = header.GetTimestamp()

		// Process the feed entities
		err = processEntities(ctx, rt, f.GetEntity())
		if err != nil {
			return nil, fmt.Errorf("processing entities: %w", err)
		}
	}

	return rt, nil
}

func processEntities(ctx context.Context, rt *Realtime, entities []*gtfsproto.FeedEntity) error {
	for _, entity := range entities {
		// We only care about TripUpdates
		if entity.TripUpdate == nil {
			continue
		}

		trip := entity.TripUpdate.Trip
		if trip == nil {
			return fmt.Errorf("trip_update missing trip")
		}

		// Blank trip ID is allowed when (route_id,
		// direction_id, start_time, start_date) is provided
		// and uniquely identifies the trip in the static
		// schedule. Also allowed for frequency based trips.
		//
		// That said, we don't support it.
		if trip.GetTripId() == "" {
			continue
		}

		switch sr := trip.GetScheduleRelationship(); sr {

		case gtfsproto.TripDescriptor_SCHEDULED:
			// Trip running in accordance with GTFS schedule
			for _, update := range entity.TripUpdate.GetStopTimeUpdate() {
				err := processStopTimeUpdate(ctx, rt, trip.GetTripId(), update)
				if err != nil {
					return fmt.Errorf("processing stop time update: %w", err)
				}
			}
			rt.NumScheduledTrips++

		case gtfsproto.TripDescriptor_ADDED:
			// An extra trip that's been added. Not supported!
			rt.NumAddedTrips++

		case gtfsproto.TripDescriptor_UNSCHEDULED:
			// For frequency based trips only. Not supported!
			rt.NumUnscheduledTrips++

		case gtfsproto.TripDescriptor_CANCELED:
			// Trip in GTFS schedule that has been canceled.
			rt.SkippedTrips[trip.GetTripId()] = true
			rt.NumCanceledTrips++

		case gtfsproto.TripDescriptor_DUPLICATED:
			// Copy of a trip in GTFS schedule. Not supported!
			rt.NumDuplicatedTrips++

		}
	}

	return nil
}

func processStopTimeUpdate(
	ctx context.Context,
	rt *Realtime,
	tripID string,
	update *gtfsproto.TripUpdate_StopTimeUpdate,
) error {

	var arrivalIsSet bool
	var arrivalTime time.Time
	var arrivalDelay time.Duration
	var departureIsSet bool
	var departureTime time.Time
	var departureDelay time.Duration

	if update.Arrival != nil {
		arrivalIsSet = true
		arrivalUnix := int64(update.GetArrival().GetTime())
		if arrivalUnix != 0 {
			arrivalTime = time.Unix(arrivalUnix, 0).UTC()
		}
		arrivalDelay = time.Duration(update.GetArrival().GetDelay()) * time.Second
	}

	if update.Departure != nil {
		departureIsSet = true
		departureUnix := int64(update.GetDeparture().GetTime())
		if departureUnix != 0 {
			departureTime = time.Unix(departureUnix, 0).UTC()
		}
		departureDelay = time.Duration(update.GetDeparture().GetDelay()) * time.Second
	}

	stup := &StopTimeUpdate{
		TripID:         tripID,
		StopID:         update.GetStopId(),
		StopSequence:   uint32(update.GetStopSequence()),
		ArrivalIsSet:   arrivalIsSet,
		ArrivalTime:    arrivalTime,
		ArrivalDelay:   arrivalDelay,
		DepartureIsSet: departureIsSet,
		DepartureTime:  departureTime,
		DepartureDelay: departureDelay,
	}

	if stup.StopID == "" && stup.StopSequence == 0 {
		// XXX: StopSequence 0 is actually allowed by
		// spec. This may cause problems.
		return fmt.Errorf("stop_time_update missing stop_id and stop_sequence")
	}

	switch sr := update.GetScheduleRelationship(); sr {

	case gtfsproto.TripUpdate_StopTimeUpdate_SCHEDULED:
		// Vehicle will stop according to GTFS schedule, but
		// possibly with delay.
		stup.Type = StopTimeUpdateScheduled
		rt.Updates = append(rt.Updates, stup)

	case gtfsproto.TripUpdate_StopTimeUpdate_SKIPPED:
		// Stop skipped
		stup.Type = StopTimeUpdateSkipped
		rt.Updates = append(rt.Updates, stup)

	case gtfsproto.TripUpdate_StopTimeUpdate_NO_DATA:
		// No data for this stop
		stup.Type = StopTimeUpdateNoData
		rt.Updates = append(rt.Updates, stup)

	case gtfsproto.TripUpdate_StopTimeUpdate_UNSCHEDULED:
		// For frequency based trips. Not supported!
	}

	return nil
}
