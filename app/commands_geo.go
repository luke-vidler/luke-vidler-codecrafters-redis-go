package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// handleGEOADD handles the GEOADD command which adds geospatial items (longitude, latitude, name) to a sorted set
func handleGEOADD(args []string, conn net.Conn) {
	if len(args) < 5 {
		conn.Write([]byte("-ERR wrong number of arguments for 'geoadd' command\r\n"))
		return
	}

	key := args[1]
	longitudeStr := args[2]
	latitudeStr := args[3]
	member := args[4]

	// Parse longitude and latitude
	longitude, errLon := strconv.ParseFloat(longitudeStr, 64)
	latitude, errLat := strconv.ParseFloat(latitudeStr, 64)

	if errLon != nil || errLat != nil {
		conn.Write([]byte("-ERR invalid longitude,latitude pair\r\n"))
		return
	}

	// Validate longitude: -180 to +180 (inclusive)
	if longitude < -180 || longitude > 180 {
		errorMsg := fmt.Sprintf("-ERR invalid longitude,latitude pair %f,%f\r\n", longitude, latitude)
		conn.Write([]byte(errorMsg))
		return
	}

	// Validate latitude: -85.05112878 to +85.05112878 (inclusive)
	if latitude < -85.05112878 || latitude > 85.05112878 {
		errorMsg := fmt.Sprintf("-ERR invalid longitude,latitude pair %f,%f\r\n", longitude, latitude)
		conn.Write([]byte(errorMsg))
		return
	}

	// Store location in sorted set
	// Calculate geohash score from latitude and longitude
	geohash := GeohashEncode(longitude, latitude)
	score := float64(geohash)

	storeMutex.Lock()
	item, exists := store[key]

	// Check if member already exists
	memberExists := false
	memberIndex := -1
	if exists {
		for i, m := range item.sortedSet {
			if m.member == member {
				memberExists = true
				memberIndex = i
				break
			}
		}
	}

	added := 0
	if !exists {
		// Create new sorted set with this member
		store[key] = storeItem{
			sortedSet: []sortedSetMember{{member: member, score: score}},
		}
		added = 1
	} else if memberExists {
		// Update existing member's score
		item.sortedSet = append(item.sortedSet[:memberIndex], item.sortedSet[memberIndex+1:]...)

		// Insert member with new score in sorted position
		newMember := sortedSetMember{member: member, score: score}
		inserted := false

		for i, m := range item.sortedSet {
			if score < m.score || (score == m.score && member < m.member) {
				item.sortedSet = append(item.sortedSet[:i], append([]sortedSetMember{newMember}, item.sortedSet[i:]...)...)
				inserted = true
				break
			}
		}

		if !inserted {
			item.sortedSet = append(item.sortedSet, newMember)
		}
		store[key] = item
		added = 0
	} else {
		// Add new member to existing sorted set
		newMember := sortedSetMember{member: member, score: score}
		inserted := false

		for i, m := range item.sortedSet {
			if score < m.score || (score == m.score && member < m.member) {
				item.sortedSet = append(item.sortedSet[:i], append([]sortedSetMember{newMember}, item.sortedSet[i:]...)...)
				inserted = true
				break
			}
		}

		if !inserted {
			item.sortedSet = append(item.sortedSet, newMember)
		}
		store[key] = item
		added = 1
	}

	storeMutex.Unlock()

	response := fmt.Sprintf(":%d\r\n", added)
	conn.Write([]byte(response))
}

// handleGEOPOS handles the GEOPOS command which returns the coordinates of members in a geospatial index
func handleGEOPOS(args []string, conn net.Conn) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'geopos' command\r\n"))
		return
	}

	key := args[1]
	members := args[2:]

	storeMutex.RLock()
	item, keyExists := store[key]
	storeMutex.RUnlock()

	// Build response array
	var response strings.Builder
	response.WriteString(fmt.Sprintf("*%d\r\n", len(members)))

	for _, member := range members {
		// Check if member exists in the sorted set and get its score
		memberExists := false
		var score float64
		if keyExists {
			for _, m := range item.sortedSet {
				if m.member == member {
					memberExists = true
					score = m.score
					break
				}
			}
		}

		if memberExists {
			// Decode the geohash score back to lon/lat
			lon, lat := GeohashDecode(uint64(score))
			lonStr := fmt.Sprintf("%f", lon)
			latStr := fmt.Sprintf("%f", lat)
			response.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(lonStr), lonStr, len(latStr), latStr))
		} else {
			// Return null array for non-existent member
			response.WriteString("*-1\r\n")
		}
	}

	conn.Write([]byte(response.String()))
}

// handleGEODIST handles the GEODIST command which returns the distance between two members
func handleGEODIST(args []string, conn net.Conn) {
	if len(args) < 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'geodist' command\r\n"))
		return
	}

	key := args[1]
	member1 := args[2]
	member2 := args[3]

	storeMutex.RLock()
	item, keyExists := store[key]
	storeMutex.RUnlock()

	// Find both members and their scores
	var score1, score2 float64
	member1Exists := false
	member2Exists := false

	if keyExists {
		for _, m := range item.sortedSet {
			if m.member == member1 {
				member1Exists = true
				score1 = m.score
			}
			if m.member == member2 {
				member2Exists = true
				score2 = m.score
			}
		}
	}

	// If either member doesn't exist, return null bulk string
	if !member1Exists || !member2Exists {
		conn.Write([]byte("$-1\r\n"))
		return
	}

	// Decode both coordinates
	lon1, lat1 := GeohashDecode(uint64(score1))
	lon2, lat2 := GeohashDecode(uint64(score2))

	// Calculate distance using Haversine formula
	distance := GeohashDistance(lon1, lat1, lon2, lat2)

	// Format distance as string
	distanceStr := fmt.Sprintf("%f", distance)

	// Return as RESP bulk string
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(distanceStr), distanceStr)
	conn.Write([]byte(response))
}

// handleGEOSEARCH handles the GEOSEARCH command which searches for members within a radius
func handleGEOSEARCH(args []string, conn net.Conn) {
	if len(args) < 8 {
		conn.Write([]byte("-ERR wrong number of arguments for 'geosearch' command\r\n"))
		return
	}

	key := args[1]

	// Parse FROMLONLAT option
	var centerLon, centerLat float64
	if strings.ToUpper(args[2]) == "FROMLONLAT" {
		var err error
		centerLon, err = strconv.ParseFloat(args[3], 64)
		if err != nil {
			conn.Write([]byte("-ERR invalid longitude\r\n"))
			return
		}
		centerLat, err = strconv.ParseFloat(args[4], 64)
		if err != nil {
			conn.Write([]byte("-ERR invalid latitude\r\n"))
			return
		}
	} else {
		conn.Write([]byte("-ERR FROMLONLAT option required\r\n"))
		return
	}

	// Parse BYRADIUS option
	var radiusMeters float64
	if strings.ToUpper(args[5]) == "BYRADIUS" {
		radius, err := strconv.ParseFloat(args[6], 64)
		if err != nil {
			conn.Write([]byte("-ERR invalid radius\r\n"))
			return
		}

		// Convert radius to meters based on unit
		unit := strings.ToLower(args[7])
		switch unit {
		case "m":
			radiusMeters = radius
		case "km":
			radiusMeters = radius * 1000
		case "mi":
			radiusMeters = radius * 1609.34
		case "ft":
			radiusMeters = radius * 0.3048
		default:
			conn.Write([]byte("-ERR unsupported unit\r\n"))
			return
		}
	} else {
		conn.Write([]byte("-ERR BYRADIUS option required\r\n"))
		return
	}

	// Search for locations within radius
	storeMutex.RLock()
	item, keyExists := store[key]
	storeMutex.RUnlock()

	var matchingMembers []string

	if keyExists {
		for _, m := range item.sortedSet {
			// Decode member's coordinates
			memberLon, memberLat := GeohashDecode(uint64(m.score))

			// Calculate distance from center point
			distance := GeohashDistance(centerLon, centerLat, memberLon, memberLat)

			// Check if within radius
			if distance <= radiusMeters {
				matchingMembers = append(matchingMembers, m.member)
			}
		}
	}

	// Build RESP array response
	var response strings.Builder
	response.WriteString(fmt.Sprintf("*%d\r\n", len(matchingMembers)))
	for _, member := range matchingMembers {
		response.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(member), member))
	}

	conn.Write([]byte(response.String()))
}
