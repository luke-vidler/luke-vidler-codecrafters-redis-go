package main

import "math"

// GeohashEncode converts latitude and longitude to a 52-bit geohash integer
// using binary search interval division (geohash algorithm).
//
// The geohash is calculated by interleaving bits from longitude and latitude:
// - Even bits (0, 2, 4, ...) encode longitude
// - Odd bits (1, 3, 5, ...) encode latitude
//
// This allows efficient spatial indexing and range queries in Redis.
func GeohashEncode(longitude float64, latitude float64) uint64 {
	var score uint64 = 0

	// WGS84 ranges - Web Mercator projection limits
	lonMin, lonMax := -180.0, 180.0
	latMin, latMax := -85.05112878, 85.05112878

	// Encode 52 bits by alternating between longitude and latitude
	// Even bits (0, 2, 4, ...) = longitude
	// Odd bits (1, 3, 5, ...) = latitude
	for i := 0; i < 52; i++ {
		if i%2 == 0 { // longitude bit
			xmid := (lonMin + lonMax) / 2
			if longitude < xmid {
				score = score << 1 // left half = 0
				lonMax = xmid
			} else {
				score = score<<1 | 1 // right half = 1
				lonMin = xmid
			}
		} else { // latitude bit
			ymid := (latMin + latMax) / 2
			if latitude < ymid {
				score = score << 1 // bottom half = 0
				latMax = ymid
			} else {
				score = score<<1 | 1 // top half = 1
				latMin = ymid
			}
		}
	}

	return score
}

// GeohashDecode converts a 52-bit geohash integer back to latitude and longitude.
// This is the reverse of GeohashEncode.
//
// Note: Due to the lossy nature of geohash encoding, the decoded coordinates
// will be approximations of the original values (typically within 0.6 meters).
func GeohashDecode(score uint64) (longitude float64, latitude float64) {
	// WGS84 ranges - Web Mercator projection limits
	lonMin, lonMax := -180.0, 180.0
	latMin, latMax := -85.05112878, 85.05112878

	// Decode 52 bits by alternating between longitude and latitude
	// We read bits from most significant to least significant (left to right)
	// Even bits (0, 2, 4, ...) = longitude
	// Odd bits (1, 3, 5, ...) = latitude
	for i := 51; i >= 0; i-- {
		bit := (score >> i) & 1

		if (51-i)%2 == 0 { // longitude bit
			xmid := (lonMin + lonMax) / 2
			if bit == 0 {
				lonMax = xmid // left half
			} else {
				lonMin = xmid // right half
			}
		} else { // latitude bit
			ymid := (latMin + latMax) / 2
			if bit == 0 {
				latMax = ymid // bottom half
			} else {
				latMin = ymid // top half
			}
		}
	}

	// Return the midpoint of the final ranges
	longitude = (lonMin + lonMax) / 2
	latitude = (latMin + latMax) / 2
	return
}

// GeohashDistance calculates the great-circle distance between two geographic
// points using the Haversine formula.
//
// Parameters:
//   - lon1, lat1: Coordinates of the first point in degrees
//   - lon2, lat2: Coordinates of the second point in degrees
//
// Returns:
//   - Distance in meters
//
// The function uses Earth's radius of 6372797.560856 meters, which is the
// exact value used by Redis for compatibility.
func GeohashDistance(lon1, lat1, lon2, lat2 float64) float64 {
	// Earth's radius in meters (exact value used by Redis)
	const earthRadiusMeters = 6372797.560856

	// Convert degrees to radians
	toRadians := func(deg float64) float64 {
		return deg * math.Pi / 180.0
	}

	lat1Rad := toRadians(lat1)
	lat2Rad := toRadians(lat2)
	lon1Rad := toRadians(lon1)
	lon2Rad := toRadians(lon2)

	// Haversine formula
	dLat := lat2Rad - lat1Rad
	dLon := lon2Rad - lon1Rad

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	distance := earthRadiusMeters * c
	return distance
}
