package parse

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/pkg/errors"

	"tidbyt.dev/gtfs/storage"
)

type StopTimeCSV struct {
	TripID        string `csv:"trip_id"`
	StopID        string `csv:"stop_id"`
	StopSequence  uint32 `csv:"stop_sequence"`
	ArrivalTime   string `csv:"arrival_time"`
	DepartureTime string `csv:"departure_time"`
	Headsign      string `csv:"stop_headsign"`
}

func parseStopTimeTime(s string) (string, error) {
	split := strings.Split(s, ":")
	if len(split) != 3 {
		return "", fmt.Errorf("found %d parts in '%s'", len(split), s)
	}

	hms := [3]int{}
	for i, str := range split {
		j, err := strconv.Atoi(str)
		if err != nil {
			return "", fmt.Errorf("non-integer in '%s' pos %d", s, i)
		}
		hms[i] = j
	}

	if hms[0] < 0 || hms[0] > 99 {
		return "", fmt.Errorf("invalid hour in '%s'", s)
	}

	if hms[1] < 0 || hms[1] > 59 {
		return "", fmt.Errorf("invalid minute in '%s'", s)
	}

	if hms[2] < 0 || hms[2] > 59 {
		return "", fmt.Errorf("invalid second in '%s'", s)
	}

	return fmt.Sprintf("%02d%02d%02d", hms[0], hms[1], hms[2]), nil
}

func ParseStopTimes(
	writer storage.FeedWriter,
	data io.Reader,
	trips map[string]bool,
	stops map[string]bool,
) (string, string, error) {

	stopTimes := []storage.StopTime{}

	stopSeq := map[string][]int{}

	maxArrival := "000000"
	maxDeparture := "000000"

	i := -1
	err := gocsv.UnmarshalToCallbackWithError(data, func(st *StopTimeCSV) error {
		i += 1
		if !trips[st.TripID] {
			return fmt.Errorf("unknown trip_id: '%s' (row %d)", st.TripID, i+1)
		}
		if st.StopID == "" {
			return fmt.Errorf("missing stop_id (row %d)", i+1)
		}
		if !stops[st.StopID] {
			return fmt.Errorf("unknown stop_id: '%s' (row %d)", st.StopID, i+1)
		}

		arrivalTime, err := parseStopTimeTime(st.ArrivalTime)
		if err != nil {
			return errors.Wrapf(err, "parsing arrival_time (row %d)", i+1)
		}

		departureTime, err := parseStopTimeTime(st.DepartureTime)
		if err != nil {
			return errors.Wrapf(err, "parsing departure_time (row %d)", i+1)
		}

		stopSeq[st.TripID] = append(stopSeq[st.TripID], int(st.StopSequence))

		if arrivalTime > maxArrival {
			maxArrival = arrivalTime
		}
		if departureTime > maxDeparture {
			maxDeparture = departureTime
		}

		stopTime := storage.StopTime{
			TripID:       st.TripID,
			StopID:       st.StopID,
			Headsign:     st.Headsign,
			StopSequence: st.StopSequence,
			Arrival:      arrivalTime,
			Departure:    departureTime,
		}

		stopTimes = append(stopTimes, stopTime)
		err = writer.WriteStopTime(&stopTime)
		if err != nil {
			return errors.Wrapf(err, "writing stop_time (row %d)", i+1)
		}

		return nil
	})

	if err != nil {
		return "", "", errors.Wrap(err, "unmarshaling stop_times csv")
	}

	// Verify that stop_sequence is unique for each trip
	for tripID, seq := range stopSeq {
		seqSeen := map[int]bool{}
		for _, i := range seq {
			if seqSeen[i] {
				return "", "", fmt.Errorf("duplicate stop_sequence %d for trip_id '%s'", i, tripID)
			}
			seqSeen[i] = true
		}
	}

	sort.SliceStable(stopTimes, func(i, j int) bool {
		cmp := strings.Compare(
			stopTimes[i].TripID,
			stopTimes[j].TripID,
		)

		if cmp < 0 {
			return true
		}
		if cmp == 0 {
			return stopTimes[i].StopSequence < stopTimes[j].StopSequence
		}
		return false
	})

	return maxArrival, maxDeparture, nil
}
