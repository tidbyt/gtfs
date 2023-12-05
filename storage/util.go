package storage

import (
	"math"
)

func HaversineDistance(aLat, aLon, bLat, bLon float64) float64 {
	const earthRadiusKm = 6371

	aLatRad := aLat * math.Pi / 180
	aLonRad := aLon * math.Pi / 180
	bLatRad := bLat * math.Pi / 180
	bLonRad := bLon * math.Pi / 180
	deltaLat := aLatRad - bLatRad
	deltaLon := aLonRad - bLonRad

	a := math.Cos(aLatRad)*math.Cos(bLatRad)*math.Pow(math.Sin(deltaLon/2), 2) + math.Pow(math.Sin(deltaLat/2), 2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return c * earthRadiusKm
}
