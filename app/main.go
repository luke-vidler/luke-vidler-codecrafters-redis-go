package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type streamEntry struct {
	id     string
	fields map[string]string
}

type storeItem struct {
	value  string
	list   []string
	stream []streamEntry
	expiry *time.Time
}

type blockedClient struct {
	conn      net.Conn
	listKey   string
	timestamp time.Time
	resultCh  chan string // Channel to send result when unblocked
}

type blockedStreamClient struct {
	conn       net.Conn
	streamKeys []string
	streamIDs  []string
	timestamp  time.Time
	resultCh   chan string // Channel to send result when unblocked
}

var (
	store                = make(map[string]storeItem)
	storeMutex           sync.RWMutex
	blockedClients       = make(map[string][]blockedClient)       // key -> list of blocked clients
	blockedStreamClients = make(map[string][]blockedStreamClient) // key -> list of blocked stream clients
	blockedMutex         sync.RWMutex
	transactionStates    = make(map[net.Conn]bool)       // conn -> is in transaction
	transactionQueues    = make(map[net.Conn][][]string) // conn -> queue of commands
	transactionMutex     sync.RWMutex
)

func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimRight(line, "\r\n")

	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array, got %s", line)
	}

	arrayLen, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, arrayLen)

	for i := 0; i < arrayLen; i++ {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimRight(line, "\r\n")

		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("expected bulk string, got %s", line)
		}

		strLen, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}

		str, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		args[i] = strings.TrimRight(str, "\r\n")[:strLen]
	}

	return args, nil
}

// parseEntryID parses a stream entry ID in format "timestamp-sequence"
func parseEntryID(id string) (int64, int64, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid entry ID format")
	}

	timestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid timestamp in entry ID")
	}

	sequence, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid sequence number in entry ID")
	}

	return timestamp, sequence, nil
}

// parseEntryIDWithWildcard parses entry ID that might contain * for sequence number or complete *
// Returns (timestamp, sequence, isWildcard, error)
func parseEntryIDWithWildcard(id string) (int64, int64, bool, error) {
	// Handle complete wildcard "*"
	if id == "*" {
		// Use current Unix timestamp in milliseconds
		currentTime := time.Now().UnixMilli()
		return currentTime, 0, true, nil
	}

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid entry ID format")
	}

	timestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid timestamp in entry ID")
	}

	if parts[1] == "*" {
		return timestamp, 0, true, nil
	}

	sequence, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid sequence number in entry ID")
	}

	return timestamp, sequence, false, nil
}

// generateSequenceNumber generates the next sequence number for a given timestamp
func generateSequenceNumber(stream []streamEntry, timestamp int64) int64 {
	// Special case: if timestamp is 0, default sequence is 1
	defaultSeq := int64(0)
	if timestamp == 0 {
		defaultSeq = 1
	}

	// Find the highest sequence number for this timestamp
	highestSeq := int64(-1)
	for _, entry := range stream {
		entryTs, entrySeq, err := parseEntryID(entry.id)
		if err != nil {
			continue
		}
		if entryTs == timestamp && entrySeq > highestSeq {
			highestSeq = entrySeq
		}
	}

	if highestSeq == -1 {
		return defaultSeq
	}

	return highestSeq + 1
}

// compareEntryIDs compares two entry IDs. Returns:
// -1 if id1 < id2
//
//	0 if id1 == id2
//	1 if id1 > id2
func compareEntryIDs(id1, id2 string) (int, error) {
	ts1, seq1, err := parseEntryID(id1)
	if err != nil {
		return 0, err
	}

	ts2, seq2, err := parseEntryID(id2)
	if err != nil {
		return 0, err
	}

	if ts1 < ts2 {
		return -1, nil
	} else if ts1 > ts2 {
		return 1, nil
	} else {
		// Same timestamp, compare sequence numbers
		if seq1 < seq2 {
			return -1, nil
		} else if seq1 > seq2 {
			return 1, nil
		} else {
			return 0, nil
		}
	}
}

// parseRangeID parses a range ID which can be "-", "+", or a normal entry ID
func parseRangeID(id string) (int64, int64, bool, error) {
	switch id {
	case "-":
		// Minimum possible ID
		return 0, 0, false, nil
	case "+":
		// Maximum possible ID (use max int64 values)
		return 9223372036854775807, 9223372036854775807, false, nil
	default:
		// Normal entry ID
		timestamp, sequence, err := parseEntryID(id)
		return timestamp, sequence, false, err
	}
}

// filterStreamEntries filters stream entries within the inclusive range [start, end]
func filterStreamEntries(stream []streamEntry, startID, endID string) ([]streamEntry, error) {
	startTs, startSeq, _, err := parseRangeID(startID)
	if err != nil {
		return nil, err
	}

	endTs, endSeq, _, err := parseRangeID(endID)
	if err != nil {
		return nil, err
	}

	var result []streamEntry
	for _, entry := range stream {
		entryTs, entrySeq, err := parseEntryID(entry.id)
		if err != nil {
			continue
		}

		// Check if entry is within range (inclusive)
		inRange := false

		// Check if entry >= start
		if entryTs > startTs || (entryTs == startTs && entrySeq >= startSeq) {
			// Check if entry <= end
			if entryTs < endTs || (entryTs == endTs && entrySeq <= endSeq) {
				inRange = true
			}
		}

		if inRange {
			result = append(result, entry)
		}
	}

	return result, nil
}

// filterStreamEntriesGreaterThan filters stream entries that are greater than the given ID (exclusive)
func filterStreamEntriesGreaterThan(stream []streamEntry, startID string) ([]streamEntry, error) {
	startTs, startSeq, err := parseEntryID(startID)
	if err != nil {
		return nil, err
	}

	var result []streamEntry
	for _, entry := range stream {
		entryTs, entrySeq, err := parseEntryID(entry.id)
		if err != nil {
			continue
		}

		// Check if entry > start (exclusive)
		if entryTs > startTs || (entryTs == startTs && entrySeq > startSeq) {
			result = append(result, entry)
		}
	}

	return result, nil
}

// resolveStreamID resolves special IDs like "$" to actual entry IDs
func resolveStreamID(stream []streamEntry, id string) string {
	if id == "$" {
		// Return the ID of the last entry in the stream, or "0-0" if stream is empty
		if len(stream) == 0 {
			return "0-0"
		}
		return stream[len(stream)-1].id
	}
	return id
}

func notifyBlockedClients(key string) {
	blockedMutex.Lock()
	defer blockedMutex.Unlock()

	clients, exists := blockedClients[key]
	if !exists || len(clients) == 0 {
		return
	}

	// Process the first (longest waiting) blocked client
	client := clients[0]
	blockedClients[key] = clients[1:] // Remove the first client

	// If no more clients, remove the key
	if len(blockedClients[key]) == 0 {
		delete(blockedClients, key)
	}

	// Try to pop an element for the unblocked client (synchronously)
	storeMutex.Lock()
	item, exists := store[key]
	if exists && len(item.list) > 0 {
		element := item.list[0]
		item.list = item.list[1:]
		store[key] = item
		storeMutex.Unlock()

		// Send response through channel
		response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(client.listKey), client.listKey, len(element), element)

		select {
		case client.resultCh <- response:
		default:
			// Client might be gone, ignore
		}
	} else {
		storeMutex.Unlock()
	}
}

func notifyBlockedStreamClients(key string) {
	blockedMutex.Lock()
	defer blockedMutex.Unlock()

	clients, exists := blockedStreamClients[key]
	if !exists || len(clients) == 0 {
		return
	}

	// Process all blocked clients for this stream key
	var remainingClients []blockedStreamClient

	for _, client := range clients {
		// Check if this client has new entries available
		hasNewEntries := false
		var streamResults []string

		storeMutex.RLock()
		for i, streamKey := range client.streamKeys {
			if streamKey != key {
				continue // Only check the stream that was updated
			}

			startID := client.streamIDs[i]
			item, exists := store[streamKey]

			if !exists {
				continue
			}

			// Filter entries greater than the start ID (exclusive)
			filteredEntries, err := filterStreamEntriesGreaterThan(item.stream, startID)
			if err != nil {
				continue
			}

			// Only include streams that have matching entries
			if len(filteredEntries) > 0 {
				hasNewEntries = true
				// Build stream result: [stream_name, [entries]]
				streamResult := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(streamKey), streamKey, len(filteredEntries))
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
				break // Found new entries for this client
			}
		}
		storeMutex.RUnlock()

		if hasNewEntries {
			// Build final response: array of stream results
			response := fmt.Sprintf("*%d\r\n", len(streamResults))
			for _, streamResult := range streamResults {
				response += streamResult
			}

			select {
			case client.resultCh <- response:
			default:
				// Client might be gone, ignore
			}
		} else {
			// Keep this client blocked
			remainingClients = append(remainingClients, client)
		}
	}

	// Update the blocked clients list
	if len(remainingClients) == 0 {
		delete(blockedStreamClients, key)
	} else {
		blockedStreamClients[key] = remainingClients
	}
}

func executeCommand(args []string, conn net.Conn) string {
	if len(args) == 0 {
		return ""
	}

	command := strings.ToUpper(args[0])

	switch command {
	case "PING":
		return "+PONG\r\n"
	case "ECHO":
		if len(args) >= 2 {
			arg := args[1]
			return fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
		}
		return "$-1\r\n"
	case "SET":
		if len(args) >= 3 {
			key := args[1]
			value := args[2]
			var expiry *time.Time

			// Parse PX option
			for i := 3; i < len(args)-1; i++ {
				if strings.ToUpper(args[i]) == "PX" && i+1 < len(args) {
					if expiryMs, err := strconv.Atoi(args[i+1]); err == nil {
						expiryTime := time.Now().Add(time.Duration(expiryMs) * time.Millisecond)
						expiry = &expiryTime
					}
					break
				}
			}

			storeMutex.Lock()
			store[key] = storeItem{value: value, expiry: expiry}
			storeMutex.Unlock()

			return "+OK\r\n"
		}
		return "-ERR wrong number of arguments\r\n"
	case "GET":
		if len(args) >= 2 {
			key := args[1]

			storeMutex.RLock()
			item, exists := store[key]
			storeMutex.RUnlock()

			// Check if key exists and is not expired
			if exists && (item.expiry == nil || item.expiry.After(time.Now())) {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(item.value), item.value)
			} else {
				// If expired, remove from store
				if exists && item.expiry != nil && !item.expiry.After(time.Now()) {
					storeMutex.Lock()
					delete(store, key)
					storeMutex.Unlock()
				}
				return "$-1\r\n"
			}
		}
		return "$-1\r\n"
	case "INCR":
		if len(args) >= 2 {
			key := args[1]

			storeMutex.Lock()
			item, exists := store[key]

			// Check if key exists and is not expired
			if exists && (item.expiry == nil || item.expiry.After(time.Now())) {
				// Try to parse the current value as an integer
				currentValue, err := strconv.Atoi(item.value)
				if err != nil {
					storeMutex.Unlock()
					return "-ERR value is not an integer or out of range\r\n"
				}

				// Increment the value
				newValue := currentValue + 1

				// Store the new value back
				item.value = strconv.Itoa(newValue)
				store[key] = item
				storeMutex.Unlock()

				// Return the new value as a RESP integer
				return fmt.Sprintf(":%d\r\n", newValue)
			} else {
				// Key doesn't exist or is expired, set to 1
				store[key] = storeItem{value: "1"}
				storeMutex.Unlock()

				// Return 1 as a RESP integer
				return ":1\r\n"
			}
		}
		return "-ERR wrong number of arguments\r\n"
	default:
		return "-ERR unknown command\r\n"
	}
}

func handleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		// Clean up transaction state when connection closes
		transactionMutex.Lock()
		delete(transactionStates, conn)
		delete(transactionQueues, conn)
		transactionMutex.Unlock()
	}()
	reader := bufio.NewReader(conn)

	for {
		args, err := parseRESP(reader)
		if err != nil {
			fmt.Println("Error parsing RESP: ", err.Error())
			break
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		// Check if we're in a transaction and should queue commands
		transactionMutex.RLock()
		inTransaction := transactionStates[conn]
		transactionMutex.RUnlock()

		if inTransaction && command != "EXEC" && command != "MULTI" {
			// Queue the command instead of executing it
			transactionMutex.Lock()
			transactionQueues[conn] = append(transactionQueues[conn], args)
			transactionMutex.Unlock()
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(args) >= 2 {
				arg := args[1]
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(response))
			}
		case "SET":
			if len(args) >= 3 {
				key := args[1]
				value := args[2]
				var expiry *time.Time

				// Parse PX option
				for i := 3; i < len(args)-1; i++ {
					if strings.ToUpper(args[i]) == "PX" && i+1 < len(args) {
						if expiryMs, err := strconv.Atoi(args[i+1]); err == nil {
							expiryTime := time.Now().Add(time.Duration(expiryMs) * time.Millisecond)
							expiry = &expiryTime

						}
						break
					}
				}

				storeMutex.Lock()
				store[key] = storeItem{value: value, expiry: expiry}
				storeMutex.Unlock()

				conn.Write([]byte("+OK\r\n"))
			}
		case "GET":
			if len(args) >= 2 {
				key := args[1]

				storeMutex.RLock()
				item, exists := store[key]
				storeMutex.RUnlock()

				// Check if key exists and is not expired
				if exists && (item.expiry == nil || item.expiry.After(time.Now())) {
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(item.value), item.value)
					conn.Write([]byte(response))
				} else {
					// If expired, remove from store
					if exists && item.expiry != nil && !item.expiry.After(time.Now()) {
						storeMutex.Lock()
						delete(store, key)
						storeMutex.Unlock()

					}
					conn.Write([]byte("$-1\r\n"))
				}
			}
		case "RPUSH":
			if len(args) >= 3 {
				key := args[1]
				elements := args[2:] // All elements from index 2 onwards

				storeMutex.Lock()
				item, exists := store[key]
				wasEmpty := !exists || len(item.list) == 0
				if !exists {
					// Create new list with all elements
					store[key] = storeItem{list: elements}
				} else {
					// Append all elements to existing list
					item.list = append(item.list, elements...)
					store[key] = item
				}
				listLen := len(store[key].list)
				storeMutex.Unlock()

				// Notify blocked clients if the list was empty before
				if wasEmpty {
					notifyBlockedClients(key)
				}

				// Return the number of elements in the list as RESP integer
				response := fmt.Sprintf(":%d\r\n", listLen)
				conn.Write([]byte(response))
			}
		case "LRANGE":
			if len(args) >= 4 {
				key := args[1]
				startStr := args[2]
				endStr := args[3]

				start, startErr := strconv.Atoi(startStr)
				end, endErr := strconv.Atoi(endStr)

				if startErr != nil || endErr != nil {
					conn.Write([]byte("-ERR invalid index\r\n"))
					continue
				}

				storeMutex.RLock()
				item, exists := store[key]
				storeMutex.RUnlock()

				if !exists || len(item.list) == 0 {
					// List doesn't exist or is empty, return empty array
					conn.Write([]byte("*0\r\n"))
					continue
				}

				listLen := len(item.list)

				// Convert negative indexes to positive indexes
				if start < 0 {
					start = listLen + start
					// If still negative after conversion, treat as 0
					if start < 0 {
						start = 0
					}
				}

				if end < 0 {
					end = listLen + end
					// If still negative after conversion, treat as 0
					if end < 0 {
						end = 0
					}
				}

				// Handle edge cases
				if start >= listLen || start > end {
					// Start index out of bounds or start > end, return empty array
					conn.Write([]byte("*0\r\n"))
					continue
				}

				// Adjust end index if it's beyond the list length
				if end >= listLen {
					end = listLen - 1
				}

				// Extract the slice
				result := item.list[start : end+1]

				// Build RESP array response
				response := fmt.Sprintf("*%d\r\n", len(result))
				for _, element := range result {
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
				}

				conn.Write([]byte(response))
			}
		case "LPUSH":
			if len(args) >= 3 {
				key := args[1]
				elements := args[2:] // All elements from index 2 onwards

				storeMutex.Lock()
				item, exists := store[key]
				wasEmpty := !exists || len(item.list) == 0
				if !exists {
					// Create new list with all elements (reverse order for left insertion)
					newList := make([]string, len(elements))
					for i, element := range elements {
						newList[len(elements)-1-i] = element
					}
					store[key] = storeItem{list: newList}
				} else {
					// Prepend all elements to existing list (reverse order for left insertion)
					newList := make([]string, len(elements)+len(item.list))
					for i, element := range elements {
						newList[len(elements)-1-i] = element
					}
					copy(newList[len(elements):], item.list)
					item.list = newList
					store[key] = item
				}
				listLen := len(store[key].list)
				storeMutex.Unlock()

				// Notify blocked clients if the list was empty before
				if wasEmpty {
					notifyBlockedClients(key)
				}

				// Return the number of elements in the list as RESP integer
				response := fmt.Sprintf(":%d\r\n", listLen)
				conn.Write([]byte(response))
			}
		case "LLEN":
			if len(args) >= 2 {
				key := args[1]

				storeMutex.RLock()
				item, exists := store[key]
				storeMutex.RUnlock()

				if !exists {
					// List doesn't exist, return 0
					conn.Write([]byte(":0\r\n"))
				} else {
					// Return the length of the list
					listLen := len(item.list)
					response := fmt.Sprintf(":%d\r\n", listLen)
					conn.Write([]byte(response))
				}
			}
		case "LPOP":
			if len(args) >= 2 {
				key := args[1]
				count := 1 // Default count is 1

				// Parse optional count argument
				if len(args) >= 3 {
					if parsedCount, err := strconv.Atoi(args[2]); err == nil && parsedCount > 0 {
						count = parsedCount
					}
				}

				storeMutex.Lock()
				item, exists := store[key]

				if !exists || len(item.list) == 0 {
					// List doesn't exist or is empty
					storeMutex.Unlock()
					if count == 1 {
						// Single element LPOP returns null bulk string
						conn.Write([]byte("$-1\r\n"))
					} else {
						// Multi-element LPOP returns empty array
						conn.Write([]byte("*0\r\n"))
					}
				} else {
					// Determine how many elements to actually remove
					actualCount := count
					if actualCount > len(item.list) {
						actualCount = len(item.list)
					}

					// Extract the elements to remove
					removedElements := item.list[:actualCount]
					item.list = item.list[actualCount:] // Remove elements from front

					store[key] = item
					storeMutex.Unlock()

					if count == 1 {
						// Single element LPOP returns bulk string
						response := fmt.Sprintf("$%d\r\n%s\r\n", len(removedElements[0]), removedElements[0])
						conn.Write([]byte(response))
					} else {
						// Multi-element LPOP returns array
						response := fmt.Sprintf("*%d\r\n", len(removedElements))
						for _, element := range removedElements {
							response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
						}
						conn.Write([]byte(response))
					}
				}
			}
		case "BLPOP":
			if len(args) >= 3 {
				key := args[1]
				timeoutStr := args[2]

				timeoutFloat, err := strconv.ParseFloat(timeoutStr, 64)
				if err != nil {
					conn.Write([]byte("-ERR timeout is not a float\r\n"))
					continue
				}

				// Check if there's an element available immediately
				storeMutex.Lock()
				item, exists := store[key]
				if exists && len(item.list) > 0 {
					// Element available, pop it immediately
					element := item.list[0]
					item.list = item.list[1:]
					store[key] = item
					storeMutex.Unlock()

					// Return [list_key, element] as RESP array
					response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
						len(key), key, len(element), element)
					conn.Write([]byte(response))
					continue
				}
				storeMutex.Unlock()

				// No element available, block the client
				// Create result channel for this blocked client
				resultCh := make(chan string, 1)

				// Add client to blocked list
				blockedMutex.Lock()
				blockedClients[key] = append(blockedClients[key], blockedClient{
					conn:      conn,
					listKey:   key,
					timestamp: time.Now(),
					resultCh:  resultCh,
				})
				blockedMutex.Unlock()

				// Determine timeout duration
				var timeoutDuration time.Duration
				if timeoutFloat == 0 {
					// Infinite timeout
					timeoutDuration = time.Hour * 24 * 365 // Very long time
				} else {
					// Convert seconds to duration
					timeoutDuration = time.Duration(timeoutFloat * float64(time.Second))
				}

				// Wait for result from channel or timeout
				select {
				case response := <-resultCh:
					// Got unblocked, send response
					conn.Write([]byte(response))
				case <-time.After(timeoutDuration):
					// Timeout expired, remove from blocked clients and send null
					blockedMutex.Lock()
					if clients, exists := blockedClients[key]; exists {
						for i, client := range clients {
							if client.conn == conn {
								blockedClients[key] = append(clients[:i], clients[i+1:]...)
								if len(blockedClients[key]) == 0 {
									delete(blockedClients, key)
								}
								break
							}
						}
					}
					blockedMutex.Unlock()

					// Send null array for timeout
					conn.Write([]byte("*-1\r\n"))
				}

			}
		case "XADD":
			if len(args) >= 5 && len(args)%2 == 1 { // Must have odd number: XADD key id field1 value1 [field2 value2 ...]
				key := args[1]
				entryID := args[2]

				// Parse entry ID (might contain wildcard)
				timestamp, sequence, isWildcard, err := parseEntryIDWithWildcard(entryID)
				if err != nil {
					conn.Write([]byte("-ERR invalid entry ID format\r\n"))
					continue
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
					continue
				}
				if cmp <= 0 {
					conn.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
					continue
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
						continue
					}
					if cmp <= 0 {
						storeMutex.Unlock()
						conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
						continue
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
		case "TYPE":
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
		case "XRANGE":
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
					continue
				}

				// Filter entries within the range
				filteredEntries, err := filterStreamEntries(item.stream, startID, endID)
				if err != nil {
					conn.Write([]byte("-ERR invalid range ID format\r\n"))
					continue
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
		case "XREAD":
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
					continue
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
					continue
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
						continue
					}

					// Filter entries greater than the start ID (exclusive)
					filteredEntries, err := filterStreamEntriesGreaterThan(item.stream, startID)
					if err != nil {
						storeMutex.RUnlock()
						conn.Write([]byte("-ERR invalid entry ID format\r\n"))
						continue
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
		case "INCR":
			if len(args) >= 2 {
				key := args[1]

				storeMutex.Lock()
				item, exists := store[key]

				// Check if key exists and is not expired
				if exists && (item.expiry == nil || item.expiry.After(time.Now())) {
					// Try to parse the current value as an integer
					currentValue, err := strconv.Atoi(item.value)
					if err != nil {
						storeMutex.Unlock()
						conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
						continue
					}

					// Increment the value
					newValue := currentValue + 1

					// Store the new value back
					item.value = strconv.Itoa(newValue)
					store[key] = item
					storeMutex.Unlock()

					// Return the new value as a RESP integer
					response := fmt.Sprintf(":%d\r\n", newValue)
					conn.Write([]byte(response))
				} else {
					// Key doesn't exist or is expired, set to 1
					store[key] = storeItem{value: "1"}
					storeMutex.Unlock()

					// Return 1 as a RESP integer
					conn.Write([]byte(":1\r\n"))
				}
			}
		case "MULTI":
			transactionMutex.Lock()
			transactionStates[conn] = true
			transactionQueues[conn] = make([][]string, 0) // Initialize empty queue
			transactionMutex.Unlock()
			conn.Write([]byte("+OK\r\n"))
		case "EXEC":
			transactionMutex.RLock()
			inTransaction := transactionStates[conn]
			transactionMutex.RUnlock()

			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			} else {
				// Get the queued commands before cleaning up
				transactionMutex.RLock()
				queue := transactionQueues[conn]
				transactionMutex.RUnlock()

				// Execute queued commands and collect responses
				var responses []string

				for _, cmdArgs := range queue {
					response := executeCommand(cmdArgs, conn)
					responses = append(responses, response)
				}

				// Clean up transaction state
				transactionMutex.Lock()
				delete(transactionStates, conn)
				delete(transactionQueues, conn)
				transactionMutex.Unlock()

				// Send array of responses
				result := fmt.Sprintf("*%d\r\n", len(responses))
				for _, response := range responses {
					result += response
				}
				conn.Write([]byte(result))
			}
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClient(conn)
	}
}
