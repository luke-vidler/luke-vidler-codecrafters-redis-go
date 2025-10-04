package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
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

type sortedSetMember struct {
	member string
	score  float64
}

type storeItem struct {
	value     string
	list      []string
	stream    []streamEntry
	sortedSet []sortedSetMember // Members stored in sorted order by score
	expiry    *time.Time
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

type replicaConnection struct {
	conn      net.Conn
	reader    *bufio.Reader
	mutex     sync.Mutex // Mutex to synchronize access to the connection
	isReplica bool       // Flag to indicate this is a replica connection
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
	isReplica            = false                                      // Track if server is running as a replica
	masterHost           = ""                                         // Master host if running as replica
	masterPort           = ""                                         // Master port if running as replica
	masterReplid         = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" // Hardcoded replication ID for master
	masterReplOffset     = 0                                          // Replication offset for master
	replicas             []*replicaConnection                         // List of connected replicas
	replicasMutex        sync.RWMutex
	replicaOffset        = 0 // Offset for replica - number of bytes of commands processed
	replicaOffsetMutex   sync.Mutex
	replicaConnections   = make(map[net.Conn]bool) // Track which connections are replicas
	replicaConnMutex     sync.RWMutex
	configDir            = ""                          // RDB file directory
	configDbFilename     = ""                          // RDB file name
	clientSubscriptions  = make(map[net.Conn][]string) // conn -> list of subscribed channels
	channelSubscribers   = make(map[string][]net.Conn) // channel -> list of subscriber connections
	subscriptionsMutex   sync.RWMutex
)

// propagateToReplicas sends a command to all connected replicas and returns the number of bytes sent
func propagateToReplicas(args []string) int {
	if len(args) == 0 {
		return 0
	}

	// Build RESP array for the command
	resp := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	replicasMutex.RLock()
	defer replicasMutex.RUnlock()

	// Send to all replicas
	for _, replica := range replicas {
		replica.conn.Write([]byte(resp))
	}

	return len(resp)
}

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

// parseRESPWithBytes parses RESP and returns both the parsed args and the number of bytes read
func parseRESPWithBytes(reader *bufio.Reader) ([]string, int, error) {
	bytesRead := 0

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, 0, err
	}
	bytesRead += len(line)

	line = strings.TrimRight(line, "\r\n")

	if !strings.HasPrefix(line, "*") {
		return nil, 0, fmt.Errorf("expected array, got %s", line)
	}

	arrayLen, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, 0, err
	}

	args := make([]string, arrayLen)

	for i := 0; i < arrayLen; i++ {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		bytesRead += len(line)

		line = strings.TrimRight(line, "\r\n")

		if !strings.HasPrefix(line, "$") {
			return nil, 0, fmt.Errorf("expected bulk string, got %s", line)
		}

		strLen, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, 0, err
		}

		str, err := reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		bytesRead += len(str)

		args[i] = strings.TrimRight(str, "\r\n")[:strLen]
	}

	return args, bytesRead, nil
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
		// Only close if it's not a replica connection
		replicaConnMutex.RLock()
		isReplicaConn := replicaConnections[conn]
		replicaConnMutex.RUnlock()

		if !isReplicaConn {
			conn.Close()
		}

		// Clean up transaction state when connection closes
		transactionMutex.Lock()
		delete(transactionStates, conn)
		delete(transactionQueues, conn)
		transactionMutex.Unlock()

		// Clean up subscription state when connection closes
		subscriptionsMutex.Lock()
		// Remove client from all channel subscriber lists
		if channels, exists := clientSubscriptions[conn]; exists {
			for _, channel := range channels {
				if subscribers, ok := channelSubscribers[channel]; ok {
					// Remove this connection from the channel's subscriber list
					for i, sub := range subscribers {
						if sub == conn {
							channelSubscribers[channel] = append(subscribers[:i], subscribers[i+1:]...)
							break
						}
					}
					// Remove channel entry if no more subscribers
					if len(channelSubscribers[channel]) == 0 {
						delete(channelSubscribers, channel)
					}
				}
			}
		}
		delete(clientSubscriptions, conn)
		subscriptionsMutex.Unlock()
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

		// Check if client is in subscribed mode
		subscriptionsMutex.RLock()
		inSubscribedMode := len(clientSubscriptions[conn]) > 0
		subscriptionsMutex.RUnlock()

		// In subscribed mode, only allow specific commands
		if inSubscribedMode {
			allowedInSubscribedMode := map[string]bool{
				"SUBSCRIBE":    true,
				"UNSUBSCRIBE":  true,
				"PSUBSCRIBE":   true,
				"PUNSUBSCRIBE": true,
				"PING":         true,
				"QUIT":         true,
				"RESET":        true,
			}

			if !allowedInSubscribedMode[command] {
				errorMsg := fmt.Sprintf("-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", strings.ToLower(command))
				conn.Write([]byte(errorMsg))
				continue
			}
		}

		// Check if we're in a transaction and should queue commands
		transactionMutex.RLock()
		inTransaction := transactionStates[conn]
		transactionMutex.RUnlock()

		if inTransaction && command != "EXEC" && command != "MULTI" && command != "DISCARD" {
			// Queue the command instead of executing it
			transactionMutex.Lock()
			transactionQueues[conn] = append(transactionQueues[conn], args)
			transactionMutex.Unlock()
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		switch command {
		case "PING":
			// Check if client is in subscribed mode
			if inSubscribedMode {
				// In subscribed mode, respond with ["pong", ""] as RESP array
				response := "*2\r\n$4\r\npong\r\n$0\r\n\r\n"
				conn.Write([]byte(response))
			} else {
				// Normal mode: respond with +PONG
				conn.Write([]byte("+PONG\r\n"))
			}
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

				// Propagate to replicas if this is a master
				if !isReplica {
					bytesWritten := propagateToReplicas(args)
					// Update master offset
					masterReplOffset += bytesWritten
				}
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
		case "INFO":
			if len(args) >= 2 && strings.ToLower(args[1]) == "replication" {
				// Return replication section with appropriate role
				var info string
				if isReplica {
					info = "role:slave"
				} else {
					info = fmt.Sprintf("role:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", masterReplid, masterReplOffset)
				}
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)
				conn.Write([]byte(response))
			} else {
				// For now, only support replication section
				conn.Write([]byte("-ERR unknown section or missing argument\r\n"))
			}
		case "REPLCONF":
			// Handle REPLCONF command from replicas during handshake
			// We can ignore the arguments and just respond with OK
			conn.Write([]byte("+OK\r\n"))
		case "PSYNC":
			// Handle PSYNC command from replicas during handshake
			// Respond with FULLRESYNC <REPL_ID> <OFFSET>
			response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", masterReplid, masterReplOffset)
			conn.Write([]byte(response))

			// Send empty RDB file
			// This is a hex representation of an empty RDB file
			emptyRDB := []byte{
				0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69,
				0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65,
				0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69,
				0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d,
				0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61,
				0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
			}

			// Send RDB file in the format: $<length>\r\n<contents>
			rdbResponse := fmt.Sprintf("$%d\r\n", len(emptyRDB))
			conn.Write([]byte(rdbResponse))
			conn.Write(emptyRDB)

			// Mark this connection as a replica
			replicaConnMutex.Lock()
			replicaConnections[conn] = true
			replicaConnMutex.Unlock()

			// Add this connection to the list of replicas
			replicasMutex.Lock()
			replicas = append(replicas, &replicaConnection{
				conn:      conn,
				reader:    reader,
				isReplica: true,
			})
			replicasMutex.Unlock()

			// Stop processing commands from this connection in handleClient
			// The WAIT command will handle communication with this replica
			return
		case "WAIT":
			// WAIT command: WAIT <numreplicas> <timeout>
			if len(args) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'wait' command\r\n"))
				continue
			}

			expectedReplicas, err := strconv.Atoi(args[1])
			if err != nil {
				conn.Write([]byte("-ERR invalid numreplicas\r\n"))
				continue
			}

			timeoutMs, err := strconv.Atoi(args[2])
			if err != nil {
				conn.Write([]byte("-ERR invalid timeout\r\n"))
				continue
			}

			replicasMutex.RLock()
			numReplicas := len(replicas)
			replicasMutex.RUnlock()

			// If no replicas, return 0 immediately
			if numReplicas == 0 {
				conn.Write([]byte(":0\r\n"))
				continue
			}

			// If no writes have been made (offset is 0), all replicas are in sync
			if masterReplOffset == 0 {
				response := fmt.Sprintf(":%d\r\n", numReplicas)
				conn.Write([]byte(response))
				continue
			}

			// Send REPLCONF GETACK to all replicas
			getackCmd := "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"

			// Create channels to receive ACK responses
			type ackResponse struct {
				offset int
				err    error
			}
			ackChan := make(chan ackResponse, numReplicas)

			replicasMutex.RLock()
			for _, replica := range replicas {
				go func(r *replicaConnection) {
					// Lock the replica connection for exclusive access
					r.mutex.Lock()
					defer r.mutex.Unlock()

					// Send GETACK command
					_, err := r.conn.Write([]byte(getackCmd))
					if err != nil {
						ackChan <- ackResponse{err: err}
						return
					}

					// Set read deadline to prevent blocking forever
					r.conn.SetReadDeadline(time.Now().Add(time.Duration(timeoutMs+100) * time.Millisecond))
					defer r.conn.SetReadDeadline(time.Time{}) // Clear deadline

					// Read the response using the stored reader
					ackArgs, _, err := parseRESPWithBytes(r.reader)
					if err != nil {
						ackChan <- ackResponse{err: err}
						return
					}

					// Parse the offset from REPLCONF ACK <offset>
					if len(ackArgs) >= 3 && strings.ToUpper(ackArgs[0]) == "REPLCONF" && strings.ToUpper(ackArgs[1]) == "ACK" {
						offset, err := strconv.Atoi(ackArgs[2])
						if err != nil {
							ackChan <- ackResponse{err: err}
							return
						}
						ackChan <- ackResponse{offset: offset}
					} else {
						ackChan <- ackResponse{err: fmt.Errorf("unexpected response format")}
					}
				}(replica)
			}
			replicasMutex.RUnlock()

			// Wait for responses or timeout
			timeout := time.After(time.Duration(timeoutMs) * time.Millisecond)
			ackedCount := 0
			responsesReceived := 0

		waitLoop:
			for responsesReceived < numReplicas {
				select {
				case ack := <-ackChan:
					responsesReceived++
					if ack.err == nil && ack.offset >= masterReplOffset {
						ackedCount++
						// Check if we've reached the expected number of replicas
						if ackedCount >= expectedReplicas {
							response := fmt.Sprintf(":%d\r\n", ackedCount)
							conn.Write([]byte(response))
							break waitLoop
						}
					}
				case <-timeout:
					// Timeout expired, return the count of replicas that have acknowledged
					response := fmt.Sprintf(":%d\r\n", ackedCount)
					conn.Write([]byte(response))
					break waitLoop
				}
			}

			// All replicas responded (only reached if loop completes normally)
			if responsesReceived >= numReplicas {
				response := fmt.Sprintf(":%d\r\n", ackedCount)
				conn.Write([]byte(response))
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
		case "DISCARD":
			transactionMutex.RLock()
			inTransaction := transactionStates[conn]
			transactionMutex.RUnlock()

			if !inTransaction {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			} else {
				// Clear transaction state and queued commands
				transactionMutex.Lock()
				delete(transactionStates, conn)
				delete(transactionQueues, conn)
				transactionMutex.Unlock()

				conn.Write([]byte("+OK\r\n"))
			}
		case "CONFIG":
			if len(args) >= 3 && strings.ToUpper(args[1]) == "GET" {
				paramName := strings.ToLower(args[2])
				var paramValue string

				switch paramName {
				case "dir":
					paramValue = configDir
				case "dbfilename":
					paramValue = configDbFilename
				default:
					conn.Write([]byte("-ERR unknown config parameter\r\n"))
					continue
				}

				// Return array with [parameter_name, parameter_value]
				response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(paramName), paramName, len(paramValue), paramValue)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'config' command\r\n"))
			}
		case "KEYS":
			if len(args) >= 2 {
				pattern := args[1]

				// Only support "*" pattern for now
				if pattern == "*" {
					storeMutex.RLock()
					keys := make([]string, 0, len(store))
					for key := range store {
						keys = append(keys, key)
					}
					storeMutex.RUnlock()

					// Build RESP array response
					response := fmt.Sprintf("*%d\r\n", len(keys))
					for _, key := range keys {
						response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
					}
					conn.Write([]byte(response))
				} else {
					// Pattern not supported
					conn.Write([]byte("*0\r\n"))
				}
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'keys' command\r\n"))
			}
		case "SUBSCRIBE":
			if len(args) >= 2 {
				channelName := args[1]

				subscriptionsMutex.Lock()
				// Add channel to this client's subscription list
				if clientSubscriptions[conn] == nil {
					clientSubscriptions[conn] = make([]string, 0)
				}
				clientSubscriptions[conn] = append(clientSubscriptions[conn], channelName)
				numSubscriptions := len(clientSubscriptions[conn])

				// Add client to the channel's subscriber list
				if channelSubscribers[channelName] == nil {
					channelSubscribers[channelName] = make([]net.Conn, 0)
				}
				channelSubscribers[channelName] = append(channelSubscribers[channelName], conn)
				subscriptionsMutex.Unlock()

				// Build response: ["subscribe", channel_name, num_subscriptions]
				response := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
					len(channelName), channelName, numSubscriptions)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'subscribe' command\r\n"))
			}
		case "PUBLISH":
			if len(args) >= 3 {
				channelName := args[1]
				message := args[2]

				subscriptionsMutex.RLock()
				// Get a copy of the subscribers list
				subscribers := make([]net.Conn, len(channelSubscribers[channelName]))
				copy(subscribers, channelSubscribers[channelName])
				numSubscribers := len(subscribers)
				subscriptionsMutex.RUnlock()

				// Deliver message to all subscribers
				// Build message array: ["message", channel_name, message_content]
				messageResponse := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(channelName), channelName, len(message), message)

				for _, subscriber := range subscribers {
					subscriber.Write([]byte(messageResponse))
				}

				// Return the number of subscribers as RESP integer
				response := fmt.Sprintf(":%d\r\n", numSubscribers)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'publish' command\r\n"))
			}
		case "UNSUBSCRIBE":
			if len(args) >= 2 {
				channelName := args[1]

				subscriptionsMutex.Lock()

				// Remove channel from client's subscription list
				if channels, exists := clientSubscriptions[conn]; exists {
					for i, ch := range channels {
						if ch == channelName {
							// Remove this channel from the list
							clientSubscriptions[conn] = append(channels[:i], channels[i+1:]...)
							break
						}
					}
				}

				// Remove client from channel's subscriber list
				if subscribers, exists := channelSubscribers[channelName]; exists {
					for i, sub := range subscribers {
						if sub == conn {
							// Remove this connection from the channel's subscriber list
							channelSubscribers[channelName] = append(subscribers[:i], subscribers[i+1:]...)
							break
						}
					}
					// Remove channel entry if no more subscribers
					if len(channelSubscribers[channelName]) == 0 {
						delete(channelSubscribers, channelName)
					}
				}

				// Get remaining subscription count for this client
				numSubscriptions := len(clientSubscriptions[conn])
				subscriptionsMutex.Unlock()

				// Build response: ["unsubscribe", channel_name, num_subscriptions]
				response := fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
					len(channelName), channelName, numSubscriptions)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'unsubscribe' command\r\n"))
			}
		case "ZADD":
			if len(args) >= 4 {
				key := args[1]
				scoreStr := args[2]
				member := args[3]

				// Parse score as float64
				score, err := strconv.ParseFloat(scoreStr, 64)
				if err != nil {
					conn.Write([]byte("-ERR value is not a valid float\r\n"))
					continue
				}

				storeMutex.Lock()
				item, exists := store[key]

				// Check if member already exists and find its index
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

				if !exists {
					// Create new sorted set with this member
					store[key] = storeItem{
						sortedSet: []sortedSetMember{{member: member, score: score}},
					}
				} else if memberExists {
					// Update existing member's score - remove old, insert new
					// Remove the old member
					item.sortedSet = append(item.sortedSet[:memberIndex], item.sortedSet[memberIndex+1:]...)

					// Insert member with new score in sorted position
					newMember := sortedSetMember{member: member, score: score}
					inserted := false

					for i, m := range item.sortedSet {
						if score < m.score || (score == m.score && member < m.member) {
							// Insert before this position
							item.sortedSet = append(item.sortedSet[:i], append([]sortedSetMember{newMember}, item.sortedSet[i:]...)...)
							inserted = true
							break
						}
					}

					if !inserted {
						// Append at the end
						item.sortedSet = append(item.sortedSet, newMember)
					}

					store[key] = item
				} else {
					// Add new member to existing sorted set
					// Insert member in sorted position
					newMember := sortedSetMember{member: member, score: score}
					inserted := false

					for i, m := range item.sortedSet {
						if score < m.score || (score == m.score && member < m.member) {
							// Insert before this position
							item.sortedSet = append(item.sortedSet[:i], append([]sortedSetMember{newMember}, item.sortedSet[i:]...)...)
							inserted = true
							break
						}
					}

					if !inserted {
						// Append at the end
						item.sortedSet = append(item.sortedSet, newMember)
					}

					store[key] = item
				}

				storeMutex.Unlock()

				// Return number of members added (1 if new, 0 if already existed)
				added := 0
				if !memberExists {
					added = 1
				}
				response := fmt.Sprintf(":%d\r\n", added)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'zadd' command\r\n"))
			}
		case "ZRANK":
			if len(args) >= 3 {
				key := args[1]
				member := args[2]

				storeMutex.RLock()
				item, exists := store[key]
				storeMutex.RUnlock()

				// Check if sorted set exists
				if !exists || len(item.sortedSet) == 0 {
					// Key doesn't exist or is not a sorted set
					conn.Write([]byte("$-1\r\n"))
					continue
				}

				// Find the member's rank (index) in the sorted set
				rank := -1
				for i, m := range item.sortedSet {
					if m.member == member {
						rank = i
						break
					}
				}

				if rank == -1 {
					// Member not found
					conn.Write([]byte("$-1\r\n"))
				} else {
					// Return the rank as RESP integer
					response := fmt.Sprintf(":%d\r\n", rank)
					conn.Write([]byte(response))
				}
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'zrank' command\r\n"))
			}
		case "ZRANGE":
			if len(args) >= 4 {
				key := args[1]
				startStr := args[2]
				stopStr := args[3]

				// Parse start and stop indices
				start, err := strconv.Atoi(startStr)
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					continue
				}

				stop, err := strconv.Atoi(stopStr)
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					continue
				}

				storeMutex.RLock()
				item, exists := store[key]
				storeMutex.RUnlock()

				// If sorted set doesn't exist, return empty array
				if !exists || len(item.sortedSet) == 0 {
					conn.Write([]byte("*0\r\n"))
					continue
				}

				setLen := len(item.sortedSet)

				// Handle negative indexes
				// -1 refers to last element, -2 to second last, etc.
				if start < 0 {
					start = setLen + start
					// If still negative (out of range), treat as 0
					if start < 0 {
						start = 0
					}
				}

				if stop < 0 {
					stop = setLen + stop
					// If still negative (out of range), treat as 0
					if stop < 0 {
						stop = 0
					}
				}

				// If start >= setLen, return empty array
				if start >= setLen {
					conn.Write([]byte("*0\r\n"))
					continue
				}

				// If stop >= setLen, treat stop as last element
				if stop >= setLen {
					stop = setLen - 1
				}

				// If start > stop, return empty array
				if start > stop {
					conn.Write([]byte("*0\r\n"))
					continue
				}

				// Extract members in range [start, stop] inclusive
				members := item.sortedSet[start : stop+1]

				// Build RESP array response
				response := fmt.Sprintf("*%d\r\n", len(members))
				for _, m := range members {
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(m.member), m.member)
				}

				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'zrange' command\r\n"))
			}
		case "ZCARD":
			if len(args) >= 2 {
				key := args[1]

				storeMutex.RLock()
				item, exists := store[key]
				storeMutex.RUnlock()

				// If sorted set doesn't exist, return 0
				cardinality := 0
				if exists && len(item.sortedSet) > 0 {
					cardinality = len(item.sortedSet)
				}

				// Return cardinality as RESP integer
				response := fmt.Sprintf(":%d\r\n", cardinality)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'zcard' command\r\n"))
			}
		case "ZSCORE":
			if len(args) >= 3 {
				key := args[1]
				member := args[2]

				storeMutex.RLock()
				item, exists := store[key]
				storeMutex.RUnlock()

				// Check if sorted set exists
				if !exists || len(item.sortedSet) == 0 {
					// Key doesn't exist or is not a sorted set
					conn.Write([]byte("$-1\r\n"))
					continue
				}

				// Find the member's score
				found := false
				var score float64
				for _, m := range item.sortedSet {
					if m.member == member {
						score = m.score
						found = true
						break
					}
				}

				if !found {
					// Member not found
					conn.Write([]byte("$-1\r\n"))
				} else {
					// Return the score as RESP bulk string
					scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(scoreStr), scoreStr)
					conn.Write([]byte(response))
				}
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'zscore' command\r\n"))
			}
		case "ZREM":
			if len(args) >= 3 {
				key := args[1]
				member := args[2]

				storeMutex.Lock()
				item, exists := store[key]

				// Check if sorted set exists
				if !exists || len(item.sortedSet) == 0 {
					// Key doesn't exist or is not a sorted set
					storeMutex.Unlock()
					conn.Write([]byte(":0\r\n"))
					continue
				}

				// Find and remove the member
				removed := 0
				for i, m := range item.sortedSet {
					if m.member == member {
						// Remove this member
						item.sortedSet = append(item.sortedSet[:i], item.sortedSet[i+1:]...)
						store[key] = item
						removed = 1
						break
					}
				}

				storeMutex.Unlock()

				// Return number of members removed (1 if found, 0 if not)
				response := fmt.Sprintf(":%d\r\n", removed)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'zrem' command\r\n"))
			}
		case "GEOADD":
			if len(args) >= 5 {
				key := args[1]
				longitudeStr := args[2]
				latitudeStr := args[3]
				member := args[4]

				// Parse longitude and latitude
				longitude, errLon := strconv.ParseFloat(longitudeStr, 64)
				latitude, errLat := strconv.ParseFloat(latitudeStr, 64)

				if errLon != nil || errLat != nil {
					conn.Write([]byte("-ERR invalid longitude,latitude pair\r\n"))
					continue
				}

				// Validate longitude: -180 to +180 (inclusive)
				if longitude < -180 || longitude > 180 {
					errorMsg := fmt.Sprintf("-ERR invalid longitude,latitude pair %f,%f\r\n", longitude, latitude)
					conn.Write([]byte(errorMsg))
					continue
				}

				// Validate latitude: -85.05112878 to +85.05112878 (inclusive)
				if latitude < -85.05112878 || latitude > 85.05112878 {
					errorMsg := fmt.Sprintf("-ERR invalid longitude,latitude pair %f,%f\r\n", longitude, latitude)
					conn.Write([]byte(errorMsg))
					continue
				}

				// Store location in sorted set
				// Calculate geohash score from latitude and longitude
				geohash := geohashEncode(longitude, latitude)
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
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'geoadd' command\r\n"))
			}
		case "GEOPOS":
			if len(args) >= 2 {
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
						lon, lat := geohashDecode(uint64(score))
						lonStr := fmt.Sprintf("%f", lon)
						latStr := fmt.Sprintf("%f", lat)
						response.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(lonStr), lonStr, len(latStr), latStr))
					} else {
						// Return null array for non-existent member
						response.WriteString("*-1\r\n")
					}
				}

				conn.Write([]byte(response.String()))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'geopos' command\r\n"))
			}
		case "GEODIST":
			if len(args) >= 4 {
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
				} else {
					// Decode both coordinates
					lon1, lat1 := geohashDecode(uint64(score1))
					lon2, lat2 := geohashDecode(uint64(score2))

					// Calculate distance using Haversine formula
					distance := geohashDistance(lon1, lat1, lon2, lat2)

					// Format distance as string
					distanceStr := fmt.Sprintf("%f", distance)

					// Return as RESP bulk string
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(distanceStr), distanceStr)
					conn.Write([]byte(response))
				}
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'geodist' command\r\n"))
			}
		}
	}
}

// geohashEncode converts latitude and longitude to a 52-bit geohash integer
// using binary search interval division (geohash algorithm)
func geohashEncode(longitude float64, latitude float64) uint64 {
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

// geohashDecode converts a 52-bit geohash integer back to latitude and longitude
// This is the reverse of geohashEncode
func geohashDecode(score uint64) (longitude float64, latitude float64) {
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

// geohashDistance calculates the distance between two points using Haversine formula
// Returns distance in meters
func geohashDistance(lon1, lat1, lon2, lat2 float64) float64 {
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

// loadRDB loads an RDB file and populates the store
func loadRDB(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		// File doesn't exist or can't be opened - treat as empty database
		return nil
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read and verify magic string "REDIS"
	magic := make([]byte, 5)
	if _, err := io.ReadFull(reader, magic); err != nil {
		return fmt.Errorf("failed to read magic string: %w", err)
	}
	if string(magic) != "REDIS" {
		return fmt.Errorf("invalid RDB file: wrong magic string")
	}

	// Read version (4 bytes)
	version := make([]byte, 4)
	if _, err := io.ReadFull(reader, version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	// Parse the RDB file
	for {
		opcode, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read opcode: %w", err)
		}

		switch opcode {
		case 0xFF: // EOF
			// End of RDB file
			return nil
		case 0xFE: // SELECTDB
			// Read database number (length-encoded)
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("failed to read database number: %w", err)
			}
			// We only support database 0, so just continue
		case 0xFB: // RESIZEDB
			// Read hash table size and expiry table size
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("failed to read hash table size: %w", err)
			}
			_, err = readLength(reader)
			if err != nil {
				return fmt.Errorf("failed to read expiry table size: %w", err)
			}
		case 0xFD: // EXPIRETIME (seconds)
			// Read 4-byte expiry timestamp
			expiryBytes := make([]byte, 4)
			if _, err := io.ReadFull(reader, expiryBytes); err != nil {
				return fmt.Errorf("failed to read expiry time: %w", err)
			}
			expiryTimestamp := binary.LittleEndian.Uint32(expiryBytes)
			expiryTime := time.Unix(int64(expiryTimestamp), 0)

			// Read the value type and key-value pair
			if err := readKeyValuePair(reader, &expiryTime); err != nil {
				return fmt.Errorf("failed to read key-value pair with expiry: %w", err)
			}
		case 0xFC: // EXPIRETIME_MS (milliseconds)
			// Read 8-byte expiry timestamp in milliseconds
			expiryBytes := make([]byte, 8)
			if _, err := io.ReadFull(reader, expiryBytes); err != nil {
				return fmt.Errorf("failed to read expiry time (ms): %w", err)
			}
			expiryTimestamp := binary.LittleEndian.Uint64(expiryBytes)
			expiryTime := time.UnixMilli(int64(expiryTimestamp))

			// Read the value type and key-value pair
			if err := readKeyValuePair(reader, &expiryTime); err != nil {
				return fmt.Errorf("failed to read key-value pair with expiry (ms): %w", err)
			}
		case 0xFA: // AUX field
			// Read auxiliary field (key-value metadata)
			_, err := readString(reader)
			if err != nil {
				return fmt.Errorf("failed to read aux key: %w", err)
			}
			_, err = readString(reader)
			if err != nil {
				return fmt.Errorf("failed to read aux value: %w", err)
			}
		default:
			// This is a value type byte, read key-value pair
			if err := readKeyValuePairWithType(reader, opcode, nil); err != nil {
				return fmt.Errorf("failed to read key-value pair: %w", err)
			}
		}
	}

	return nil
}

// readLength reads a length-encoded integer
func readLength(reader *bufio.Reader) (uint64, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// Check the first 2 bits
	typeVal := (firstByte & 0xC0) >> 6

	switch typeVal {
	case 0: // 6-bit length
		return uint64(firstByte & 0x3F), nil
	case 1: // 14-bit length
		secondByte, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint64((firstByte&0x3F))<<8 | uint64(secondByte), nil
	case 2: // 32-bit length (big-endian)
		lengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(reader, lengthBytes); err != nil {
			return 0, err
		}
		return uint64(binary.BigEndian.Uint32(lengthBytes)), nil
	case 3: // Special encoding
		// Return a special marker (we'll handle this in readString)
		return uint64(firstByte&0x3F) | 0x8000000000000000, nil
	}

	return 0, fmt.Errorf("invalid length encoding")
}

// readString reads a length-prefixed string
func readString(reader *bufio.Reader) (string, error) {
	length, err := readLength(reader)
	if err != nil {
		return "", err
	}

	// Check if this is a special encoding
	if length&0x8000000000000000 != 0 {
		// Special encoding
		encodingType := length & 0x3F
		switch encodingType {
		case 0: // 8-bit integer
			b, err := reader.ReadByte()
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int8(b)), nil
		case 1: // 16-bit integer (little-endian)
			intBytes := make([]byte, 2)
			if _, err := io.ReadFull(reader, intBytes); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(intBytes))), nil
		case 2: // 32-bit integer (little-endian)
			intBytes := make([]byte, 4)
			if _, err := io.ReadFull(reader, intBytes); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(intBytes))), nil
		default:
			return "", fmt.Errorf("unsupported special encoding type: %d", encodingType)
		}
	}

	strBytes := make([]byte, length)
	if _, err := io.ReadFull(reader, strBytes); err != nil {
		return "", err
	}

	return string(strBytes), nil
}

// readKeyValuePair reads a key-value pair (reads value type first)
func readKeyValuePair(reader *bufio.Reader, expiry *time.Time) error {
	valueType, err := reader.ReadByte()
	if err != nil {
		return err
	}
	return readKeyValuePairWithType(reader, valueType, expiry)
}

// readKeyValuePairWithType reads a key-value pair with a given value type
func readKeyValuePairWithType(reader *bufio.Reader, valueType byte, expiry *time.Time) error {
	// Read the key
	key, err := readString(reader)
	if err != nil {
		return fmt.Errorf("failed to read key: %w", err)
	}

	// Read the value based on type
	switch valueType {
	case 0: // String encoding
		value, err := readString(reader)
		if err != nil {
			return fmt.Errorf("failed to read string value: %w", err)
		}

		storeMutex.Lock()
		store[key] = storeItem{value: value, expiry: expiry}
		storeMutex.Unlock()
	default:
		// Other types not supported yet
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	return nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	port := "6379" // Default port

	// Parse command line arguments
	for i, arg := range os.Args {
		if arg == "--port" && i+1 < len(os.Args) {
			port = os.Args[i+1]
		} else if arg == "--replicaof" && i+1 < len(os.Args) {
			// Parse master host and port from next argument
			replicaofValue := os.Args[i+1]
			parts := strings.Split(replicaofValue, " ")
			if len(parts) == 2 {
				isReplica = true
				masterHost = parts[0]
				masterPort = parts[1]
			}
		} else if arg == "--dir" && i+1 < len(os.Args) {
			configDir = os.Args[i+1]
		} else if arg == "--dbfilename" && i+1 < len(os.Args) {
			configDbFilename = os.Args[i+1]
		}
	}

	// Load RDB file if dir and dbfilename are specified
	if configDir != "" && configDbFilename != "" {
		rdbPath := filepath.Join(configDir, configDbFilename)
		if err := loadRDB(rdbPath); err != nil {
			fmt.Printf("Error loading RDB file: %s\n", err.Error())
		} else {
			fmt.Printf("Loaded RDB file from %s\n", rdbPath)
		}
	}

	// If running as replica, connect to master and perform handshake
	if isReplica {
		go func() {
			masterAddr := net.JoinHostPort(masterHost, masterPort)
			masterConn, err := net.Dial("tcp", masterAddr)
			if err != nil {
				fmt.Printf("Failed to connect to master at %s: %s\n", masterAddr, err.Error())
				return
			}
			defer masterConn.Close()

			reader := bufio.NewReader(masterConn)

			// Step 1: Send PING command as RESP array
			pingCmd := "*1\r\n$4\r\nPING\r\n"
			_, err = masterConn.Write([]byte(pingCmd))
			if err != nil {
				fmt.Printf("Failed to send PING to master: %s\n", err.Error())
				return
			}
			fmt.Println("Sent PING to master")

			// Read PING response
			_, err = reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Failed to read PING response: %s\n", err.Error())
				return
			}

			// Step 2: Send REPLCONF listening-port <PORT>
			replconfPort := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port)
			_, err = masterConn.Write([]byte(replconfPort))
			if err != nil {
				fmt.Printf("Failed to send REPLCONF listening-port: %s\n", err.Error())
				return
			}
			fmt.Println("Sent REPLCONF listening-port")

			// Read REPLCONF response
			_, err = reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Failed to read REPLCONF listening-port response: %s\n", err.Error())
				return
			}

			// Step 3: Send REPLCONF capa psync2
			replconfCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
			_, err = masterConn.Write([]byte(replconfCapa))
			if err != nil {
				fmt.Printf("Failed to send REPLCONF capa: %s\n", err.Error())
				return
			}
			fmt.Println("Sent REPLCONF capa psync2")

			// Read REPLCONF response
			_, err = reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Failed to read REPLCONF capa response: %s\n", err.Error())
				return
			}

			// Step 4: Send PSYNC ? -1
			psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
			_, err = masterConn.Write([]byte(psyncCmd))
			if err != nil {
				fmt.Printf("Failed to send PSYNC: %s\n", err.Error())
				return
			}
			fmt.Println("Sent PSYNC ? -1")

			// Read PSYNC response (FULLRESYNC)
			_, err = reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Failed to read PSYNC response: %s\n", err.Error())
				return
			}

			// Read RDB file
			// Format is: $<length>\r\n<binary_contents>
			rdbHeader, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Failed to read RDB header: %s\n", err.Error())
				return
			}

			// Parse the length from the header (format: $<length>\r\n)
			rdbHeader = strings.TrimRight(rdbHeader, "\r\n")
			if !strings.HasPrefix(rdbHeader, "$") {
				fmt.Printf("Invalid RDB header format: %s\n", rdbHeader)
				return
			}

			rdbLength, err := strconv.Atoi(rdbHeader[1:])
			if err != nil {
				fmt.Printf("Failed to parse RDB length: %s\n", err.Error())
				return
			}

			// Read the RDB file contents
			rdbContents := make([]byte, rdbLength)
			_, err = reader.Read(rdbContents)
			if err != nil {
				fmt.Printf("Failed to read RDB contents: %s\n", err.Error())
				return
			}

			fmt.Println("Handshake completed successfully, RDB file received")

			// Now keep listening for commands from the master
			for {
				args, bytesRead, err := parseRESPWithBytes(reader)
				if err != nil {
					fmt.Printf("Error parsing command from master: %s\n", err.Error())
					break
				}

				if len(args) == 0 {
					continue
				}

				// Process the command but don't send a response
				// Exception: REPLCONF GETACK requires a response
				command := strings.ToUpper(args[0])
				switch command {
				case "REPLCONF":
					// Handle REPLCONF GETACK command
					if len(args) >= 2 && strings.ToUpper(args[1]) == "GETACK" {
						// Get current offset before updating it
						replicaOffsetMutex.Lock()
						currentOffset := replicaOffset
						replicaOffsetMutex.Unlock()

						// Respond with REPLCONF ACK <offset>
						offsetStr := strconv.Itoa(currentOffset)
						ackResponse := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)
						_, err := masterConn.Write([]byte(ackResponse))
						if err != nil {
							fmt.Printf("Failed to send ACK response: %s\n", err.Error())
						}
						fmt.Printf("Sent REPLCONF ACK %d\n", currentOffset)

						// Now update the offset to include this REPLCONF GETACK command
						replicaOffsetMutex.Lock()
						replicaOffset += bytesRead
						replicaOffsetMutex.Unlock()
					} else {
						// Other REPLCONF commands (not GETACK) - just update offset
						replicaOffsetMutex.Lock()
						replicaOffset += bytesRead
						replicaOffsetMutex.Unlock()
					}
				case "PING":
					// PING command from master - just update offset
					replicaOffsetMutex.Lock()
					replicaOffset += bytesRead
					replicaOffsetMutex.Unlock()
					fmt.Println("Replica processed PING")
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

						fmt.Printf("Replica processed SET %s %s\n", key, value)
					}

					// Update offset for SET command
					replicaOffsetMutex.Lock()
					replicaOffset += bytesRead
					replicaOffsetMutex.Unlock()
				// Add other write commands here as needed
				default:
					fmt.Printf("Replica received command: %s\n", command)
					// Update offset for unknown commands too
					replicaOffsetMutex.Lock()
					replicaOffset += bytesRead
					replicaOffsetMutex.Unlock()
				}
			}
		}()
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", port)
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
