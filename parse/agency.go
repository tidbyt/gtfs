package parse

import (
	"fmt"
	"io"
	"time"

	"github.com/gocarina/gocsv"

	"tidbyt.dev/gtfs/storage"
)

type AgencyCSV struct {
	ID       string `csv:"agency_id"`
	Name     string `csv:"agency_name"`
	URL      string `csv:"agency_url"`
	Timezone string `csv:"agency_timezone"`
	// Lang     string `csv:"agency_lang"`
	// Phone    string `csv:"agency_phone"`
	// FareURL  string `csv:"agency_fare_url"`
	// Email    string `csv:"agency_email"`
}

func ParseAgency(writer storage.FeedWriter, data io.Reader) (map[string]bool, string, error) {
	agencyCsv := []*AgencyCSV{}
	if err := gocsv.Unmarshal(data, &agencyCsv); err != nil {
		return nil, "", fmt.Errorf("unmarshaling agency csv: %w", err)
	}

	if len(agencyCsv) == 0 {
		return nil, "", fmt.Errorf("no agency record found")
	}

	// "If multiple agencies are specified in the dataset, each
	// must have the same agency_timezone."
	agencyTz := map[string]bool{}
	for _, a := range agencyCsv {
		agencyTz[a.Timezone] = true
	}
	if len(agencyTz) != 1 {
		return nil, "", fmt.Errorf("multiple agency_timezone")
	}

	tz := agencyCsv[0].Timezone
	if tz == "" {
		return nil, "", fmt.Errorf("missing agency_timezone")
	}
	_, err := time.LoadLocation(tz)
	if err != nil {
		return nil, "", fmt.Errorf("agency_timezone '%s' is invalid: %w", tz, err)
	}

	agency := map[string]bool{}
	for _, a := range agencyCsv {
		if agency[a.ID] {
			return nil, "", fmt.Errorf("duplicated agency_id: '%s'", a.ID)
		}
		agency[a.ID] = true

		if a.Name == "" {
			return nil, "", fmt.Errorf("missing agency_name")
		}

		if a.URL == "" {
			return nil, "", fmt.Errorf("missing agency_url")
		}

		writer.WriteAgency(&storage.Agency{
			ID:       a.ID,
			Name:     a.Name,
			URL:      a.URL,
			Timezone: tz,
		})

	}

	return agency, tz, nil
}
