package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"tidbyt.dev/gtfs/model"
)

func TestHaversineDistance(t *testing.T) {
	var loc = map[string]model.Stop{
		"nyc":    {ID: "nyc", Lat: 40.700000, Lon: -74.100000},
		"philly": {ID: "philly", Lat: 40.000000, Lon: -75.200000},
		"sf":     {ID: "sf", Lat: 37.800000, Lon: -122.500000},
		"la":     {ID: "la", Lat: 34.000000, Lon: -118.500000},
		"sto":    {ID: "sto", Lat: 59.300000, Lon: 17.900000},
		"lon":    {ID: "lon", Lat: 51.500000, Lon: -0.200000},
		"rey":    {ID: "rey", Lat: 64.100000, Lon: -21.900000},
	}

	assert.InDelta(t, 121.438585, HaversineDistance(loc["nyc"].Lat, loc["nyc"].Lon, loc["philly"].Lat, loc["philly"].Lon), 0.001)
	assert.InDelta(t, 4127.311071, HaversineDistance(loc["nyc"].Lat, loc["nyc"].Lon, loc["sf"].Lat, loc["sf"].Lon), 0.001)
	assert.InDelta(t, 3951.861367, HaversineDistance(loc["nyc"].Lat, loc["nyc"].Lon, loc["la"].Lat, loc["la"].Lon), 0.001)
	assert.InDelta(t, 6318.636281, HaversineDistance(loc["nyc"].Lat, loc["nyc"].Lon, loc["sto"].Lat, loc["sto"].Lon), 0.001)
	assert.InDelta(t, 5572.804939, HaversineDistance(loc["nyc"].Lat, loc["nyc"].Lon, loc["lon"].Lat, loc["lon"].Lon), 0.001)
	assert.InDelta(t, 4209.275847, HaversineDistance(loc["nyc"].Lat, loc["nyc"].Lon, loc["rey"].Lat, loc["rey"].Lon), 0.001)
	assert.InDelta(t, 4052.204563, HaversineDistance(loc["philly"].Lat, loc["philly"].Lon, loc["sf"].Lat, loc["sf"].Lon), 0.001)
	assert.InDelta(t, 3864.146847, HaversineDistance(loc["philly"].Lat, loc["philly"].Lon, loc["la"].Lat, loc["la"].Lon), 0.001)
	assert.InDelta(t, 6437.030542, HaversineDistance(loc["philly"].Lat, loc["philly"].Lon, loc["sto"].Lat, loc["sto"].Lon), 0.001)
	assert.InDelta(t, 5694.234270, HaversineDistance(loc["philly"].Lat, loc["philly"].Lon, loc["lon"].Lat, loc["lon"].Lon), 0.001)
	assert.InDelta(t, 4325.964058, HaversineDistance(loc["philly"].Lat, loc["philly"].Lon, loc["rey"].Lat, loc["rey"].Lon), 0.001)
	assert.InDelta(t, 555.165790, HaversineDistance(loc["sf"].Lat, loc["sf"].Lon, loc["la"].Lat, loc["la"].Lon), 0.001)
	assert.InDelta(t, 8619.312141, HaversineDistance(loc["sf"].Lat, loc["sf"].Lon, loc["sto"].Lat, loc["sto"].Lon), 0.001)
	assert.InDelta(t, 8615.077500, HaversineDistance(loc["sf"].Lat, loc["sf"].Lon, loc["lon"].Lat, loc["lon"].Lon), 0.001)
	assert.InDelta(t, 6760.677281, HaversineDistance(loc["sf"].Lat, loc["sf"].Lon, loc["rey"].Lat, loc["rey"].Lon), 0.001)
	assert.InDelta(t, 8891.306919, HaversineDistance(loc["la"].Lat, loc["la"].Lon, loc["sto"].Lat, loc["sto"].Lon), 0.001)
	assert.InDelta(t, 8770.450733, HaversineDistance(loc["la"].Lat, loc["la"].Lon, loc["lon"].Lat, loc["lon"].Lon), 0.001)
	assert.InDelta(t, 6952.152842, HaversineDistance(loc["la"].Lat, loc["la"].Lon, loc["rey"].Lat, loc["rey"].Lon), 0.001)
	assert.InDelta(t, 1426.989197, HaversineDistance(loc["sto"].Lat, loc["sto"].Lon, loc["lon"].Lat, loc["lon"].Lon), 0.001)
	assert.InDelta(t, 2126.357273, HaversineDistance(loc["sto"].Lat, loc["sto"].Lon, loc["rey"].Lat, loc["rey"].Lon), 0.001)
	assert.InDelta(t, 1882.845837, HaversineDistance(loc["lon"].Lat, loc["lon"].Lon, loc["rey"].Lat, loc["rey"].Lon), 0.001)
}
