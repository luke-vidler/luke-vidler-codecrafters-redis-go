package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// handleXADD handles the XADD command which adds an entry to a stream
func handleXADD(args []string, conn net.Conn) {
	if len(args) >= 5 && len(args)%2 == 1 { // Must have odd number: XADD key id field1 value1 [field2 value2 ...]
		key := args[1]
		entryID := args[2]
		// Parse entry ID (might contain wildcard)
		timestamp, sequence, isWildcard, err := parseEntryIDWithWildcard(entryID)
		if err != nil {
			conn.Write([]byte("-ERR invalid entry ID format\r\n"))
			return
		}
		// Handle wildcard sequence number generation
		var finalEntryID string
		if isWildcard {
			// Need to access store to generate sequence number
			storeMutex.RLock()
			item, exists := store[key]
			var stream []streamEntry
			if exists {
				stream = item.stream
			}
			storeMutex.RUnlock()
			sequence = generateSequenceNumber(stream, timestamp)
			finalEntryID = fmt.Sprintf("%d-%d", timestamp, sequence)
		} else {
			finalEntryID = entryID
		}
		// Check minimum ID requirement (must be greater than 0-0)
		cmp, err := compareEntryIDs(finalEntryID, "0-0")
		if err != nil {
			conn.Write([]byte("-ERR invalid entry ID format\r\n"))
			return
		}
		if cmp <= 0 {
			conn.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
			return
		}
		// Parse field-value pairs (args[3], args[4], args[5], args[6], ...)
		fields := make(map[string]string)
		for i := 3; i < len(args); i += 2 {
			if i+1 < len(args) {
				fieldName := args[i]
				fieldValue := args[i+1]
				fields[fieldName] = fieldValue
			}
		}
		// Create the stream entry
		entry := streamEntry{
			id:     finalEntryID,
			fields: fields,
		}
		storeMutex.Lock()
		item, exists := store[key]
		// Check if ID is greater than the last entry (if stream exists)
		if exists && len(item.stream) > 0 {
			lastEntry := item.stream[len(item.stream)-1]
			cmp, err := compareEntryIDs(finalEntryID, lastEntry.id)
			if err != nil {
				storeMutex.Unlock()
				conn.Write([]byte("-ERR invalid entry ID format\r\n"))
				return
			}
			if cmp <= 0 {
				storeMutex.Unlock()
				conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
				return
			}
		}
		if !exists {
			// Create new stream
			store[key] = storeItem{stream: []streamEntry{entry}}
		} else {
			// Append to existing stream
			item.stream = append(item.stream, entry)
			store[key] = item
		}
		storeMutex.Unlock()
		// Notify blocked stream clients
		notifyBlockedStreamClients(key)
		// Return the entry ID as a bulk string
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(finalEntryID), finalEntryID)
		conn.Write([]byte(response))
	}
}

// handleTYPE handles the TYPE command which returns the type of a key
func handleTYPE(args []string, conn net.Conn) {
	if len(args) >= 2 {
		key := args[1]
		storeMutex.RLock()
		item, exists := store[key]
		storeMutex.RUnlock()
		if !exists {
			// Key doesn't exist
			conn.Write([]byte("+none\r\n"))
		} else {
			// Determine the type based on which field is populated
			var dataType string
			if len(item.stream) > 0 {
				// Has stream data
				dataType = "stream"
			} else if len(item.list) > 0 {
				// Has list data
				dataType = "list"
			} else if item.value != "" {
				// Has string data
				dataType = "string"
			} else {
				// Empty/unknown type
				dataType = "none"
			}
			response := fmt.Sprintf("+%s\r\n", dataType)
			conn.Write([]byte(response))
		}
	}
}

// handleXRANGE handles the XRANGE command which returns a range of stream entries
func handleXRANGE(args []string, conn net.Conn) {
	if len(args) >= 4 {
		key := args[1]
		startID := args[2]
		endID := args[3]
		storeMutex.RLock()
		item, exists := store[key]
		storeMutex.RUnlock()
		if !exists {
			// Stream doesn't exist, return empty array
			conn.Write([]byte("*0\r\n"))
			return
		}
		// Filter entries within the range
		filteredEntries, err := filterStreamEntries(item.stream, startID, endID)
		if err != nil {
			conn.Write([]byte("-ERR invalid range ID format\r\n"))
			return
		}
		// Build RESP array response
		response := fmt.Sprintf("*%d\r\n", len(filteredEntries))
		for _, entry := range filteredEntries {
			// Each entry is an array: [id, [field1, value1, field2, value2, ...]]
			fieldCount := len(entry.fields) * 2
			response += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(entry.id), entry.id, fieldCount)
			// Add field-value pairs
			for field, value := range entry.fields {
				response += fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(field), field, len(value), value)
			}
		}
		conn.Write([]byte(response))
	}
}

// handleXREAD handles the XREAD command which reads from one or more streams
func handleXREAD(args []string, conn net.Conn) {
	// Handle both: XREAD streams key1 key2... id1 id2...
	// and: XREAD block timeout streams key1 key2... id1 id2...
	var blockTimeout float64 = -1 // -1 means no blocking
	var isBlocking = false
	var streamsIndex = 1 // Default position of "streams"
	// Check if BLOCK parameter is present
	if len(args) >= 3 && strings.ToUpper(args[1]) == "BLOCK" {
		var err error
		blockTimeout, err = strconv.ParseFloat(args[2], 64)
		if err != nil {
			conn.Write([]byte("-ERR timeout is not a float or integer\r\n"))
			return
		}
		isBlocking = true
		streamsIndex = 3
	}
	if len(args) >= streamsIndex+3 && strings.ToUpper(args[streamsIndex]) == "STREAMS" {
		// Parse stream keys and IDs: XREAD [block timeout] streams key1 key2... id1 id2...
		// Find the split point between keys and IDs (args are split in half after "streams")
		streamArgsCount := len(args) - streamsIndex - 1 // exclude args before "streams" and "streams" itself
		if streamArgsCount%2 != 0 {
			conn.Write([]byte("-ERR wrong number of arguments\r\n"))
			return
		}
		numStreams := streamArgsCount / 2
		streamKeys := args[streamsIndex+1 : streamsIndex+1+numStreams]
		streamIDs := args[streamsIndex+1+numStreams : streamsIndex+1+streamArgsCount]
		var streamResults []string
		storeMutex.RLock()
		// First pass: resolve any $ IDs to actual IDs
		resolvedStreamIDs := make([]string, len(streamIDs))
		for i, key := range streamKeys {
			startID := streamIDs[i]
			item, exists := store[key]
			if !exists {
				// Stream doesn't exist, treat $ as "0-0"
				resolvedStreamIDs[i] = resolveStreamID([]streamEntry{}, startID)
			} else {
				resolvedStreamIDs[i] = resolveStreamID(item.stream, startID)
			}
		}
		for i, key := range streamKeys {
			startID := resolvedStreamIDs[i]
			item, exists := store[key]
			if !exists {
				// Stream doesn't exist, skip it
				return
			}
			// Filter entries greater than the start ID (exclusive)
			filteredEntries, err := filterStreamEntriesGreaterThan(item.stream, startID)
			if err != nil {
				storeMutex.RUnlock()
				conn.Write([]byte("-ERR invalid entry ID format\r\n"))
				return
			}
			// Only include streams that have matching entries
			if len(filteredEntries) > 0 {
				// Build stream result: [stream_name, [entries]]
				streamResult := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(key), key, len(filteredEntries))
				for _, entry := range filteredEntries {
					// Each entry is an array: [id, [field1, value1, field2, value2, ...]]
					fieldCount := len(entry.fields) * 2
					streamResult += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(entry.id), entry.id, fieldCount)
					// Add field-value pairs
					for field, value := range entry.fields {
						streamResult += fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(field), field, len(value), value)
					}
				}
				streamResults = append(streamResults, streamResult)
			}
		}
		storeMutex.RUnlock()
		// If we have results or not blocking, return immediately
		if len(streamResults) > 0 || !isBlocking {
			// Build final response: array of stream results
			if len(streamResults) == 0 {
				conn.Write([]byte("*0\r\n"))
			} else {
				response := fmt.Sprintf("*%d\r\n", len(streamResults))
				for _, streamResult := range streamResults {
					response += streamResult
				}
				conn.Write([]byte(response))
			}
		} else {
			// No results and blocking requested - block the client
			// Create result channel for this blocked client
			resultCh := make(chan string, 1)
			// Add client to blocked list for each stream key
			blockedMutex.Lock()
			for _, key := range streamKeys {
				blockedStreamClients[key] = append(blockedStreamClients[key], blockedStreamClient{
					conn:       conn,
					streamKeys: streamKeys,
					streamIDs:  resolvedStreamIDs, // Use resolved IDs instead of original IDs
					timestamp:  time.Now(),
					resultCh:   resultCh,
				})
			}
			blockedMutex.Unlock()
			// Determine timeout duration
			var timeoutDuration time.Duration
			if blockTimeout == 0 {
				// Infinite timeout
				timeoutDuration = time.Hour * 24 * 365 // Very long time
			} else {
				// Convert milliseconds to duration (Redis BLOCK uses milliseconds)
				timeoutDuration = time.Duration(blockTimeout) * time.Millisecond
			}
			// Wait for result from channel or timeout
			select {
			case response := <-resultCh:
				// Got unblocked, send response
				conn.Write([]byte(response))
			case <-time.After(timeoutDuration):
				// Timeout expired, remove from all blocked clients lists and send null
				blockedMutex.Lock()
				for _, key := range streamKeys {
					if clients, exists := blockedStreamClients[key]; exists {
						var remainingClients []blockedStreamClient
						for _, client := range clients {
							if client.conn != conn {
								remainingClients = append(remainingClients, client)
							}
						}
						if len(remainingClients) == 0 {
							delete(blockedStreamClients, key)
						} else {
							blockedStreamClients[key] = remainingClients
						}
					}
				}
				blockedMutex.Unlock()
				// Send null array for timeout
				conn.Write([]byte("*-1\r\n"))
			}
		}
	}
}
