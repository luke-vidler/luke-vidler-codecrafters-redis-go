package main

import (
	"bufio"
	"fmt"
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

func (s *Server) executeCommand(args []string, conn net.Conn) string {
	if len(args) == 0 {
		return ""
	}

	command := strings.ToUpper(args[0])

	switch command {
	case "PING":
		return s.handlePING(args, conn)
	case "ECHO":
		return s.handleECHO(args, conn)
	case "SET":
		return s.handleSET(args, conn)
	case "GET":
		if len(args) >= 2 {
			key := args[1]

			s.store.mutex.RLock()
			item, exists := s.store.data[key]
			s.store.mutex.RUnlock()

			// Check if key exists and is not expired
			if exists && (item.expiry == nil || item.expiry.After(time.Now())) {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(item.value), item.value)
			} else {
				// If expired, remove from store
				if exists && item.expiry != nil && !item.expiry.After(time.Now()) {
					s.store.mutex.Lock()
					delete(s.store.data, key)
					s.store.mutex.Unlock()
				}
				return "$-1\r\n"
			}
		}
		return "$-1\r\n"
	case "INCR":
		return s.handleINCR(args, conn)
	default:
		return "-ERR unknown command\r\n"
	}
}

func (s *Server) handleClient(conn net.Conn) {
	defer func() {
		// Only close if it's not a replica connection
		s.replication.replicaConnMutex.RLock()
		isReplicaConn := s.replication.replicaConnections[conn]
		s.replication.replicaConnMutex.RUnlock()

		if !isReplicaConn {
			conn.Close()
		}

		// Clean up transaction state when connection closes
		s.transactions.mutex.Lock()
		delete(s.transactions.states, conn)
		delete(s.transactions.queues, conn)
		s.transactions.mutex.Unlock()

		// Clean up subscription state when connection closes
		s.pubsub.mutex.Lock()
		// Remove client from all channel subscriber lists
		if channels, exists := s.pubsub.clientSubscriptions[conn]; exists {
			for _, channel := range channels {
				if subscribers, ok := s.pubsub.channelSubscribers[channel]; ok {
					// Remove this connection from the channel's subscriber list
					for i, sub := range subscribers {
						if sub == conn {
							s.pubsub.channelSubscribers[channel] = append(subscribers[:i], subscribers[i+1:]...)
							break
						}
					}
					// Remove channel entry if no more subscribers
					if len(s.pubsub.channelSubscribers[channel]) == 0 {
						delete(s.pubsub.channelSubscribers, channel)
					}
				}
			}
		}
		delete(s.pubsub.clientSubscriptions, conn)
		s.pubsub.mutex.Unlock()
	}()
	reader := bufio.NewReader(conn)

	for {
		args, err := ParseRESP(reader)
		if err != nil {
			fmt.Println("Error parsing RESP: ", err.Error())
			break
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		// Check if client is in subscribed mode
		s.pubsub.mutex.RLock()
		inSubscribedMode := len(s.pubsub.clientSubscriptions[conn]) > 0
		s.pubsub.mutex.RUnlock()

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
		s.transactions.mutex.RLock()
		inTransaction := s.transactions.states[conn]
		s.transactions.mutex.RUnlock()

		if inTransaction && command != "EXEC" && command != "MULTI" && command != "DISCARD" {
			// Queue the command instead of executing it
			s.transactions.mutex.Lock()
			s.transactions.queues[conn] = append(s.transactions.queues[conn], args)
			s.transactions.mutex.Unlock()
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

				s.store.mutex.Lock()
				s.store.data[key] = storeItem{value: value, expiry: expiry}
				s.store.mutex.Unlock()

				conn.Write([]byte("+OK\r\n"))

				// Propagate to s.replication.replicas if this is a master
				if !s.replication.isReplica {
					bytesWritten := s.propagateToReplicas(args)
					// Update master offset
					s.replication.masterReplOffset += bytesWritten
				}
			}
		case "GET":
			if len(args) >= 2 {
				key := args[1]

				s.store.mutex.RLock()
				item, exists := s.store.data[key]
				s.store.mutex.RUnlock()

				// Check if key exists and is not expired
				if exists && (item.expiry == nil || item.expiry.After(time.Now())) {
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(item.value), item.value)
					conn.Write([]byte(response))
				} else {
					// If expired, remove from store
					if exists && item.expiry != nil && !item.expiry.After(time.Now()) {
						s.store.mutex.Lock()
						delete(s.store.data, key)
						s.store.mutex.Unlock()

					}
					conn.Write([]byte("$-1\r\n"))
				}
			}
		case "RPUSH":
			s.handleRPUSH(args, conn)
		case "LRANGE":
			s.handleLRANGE(args, conn)
		case "LPUSH":
			s.handleLPUSH(args, conn)
		case "LLEN":
			s.handleLLEN(args, conn)
		case "LPOP":
			s.handleLPOP(args, conn)
		case "BLPOP":
			s.handleBLPOP(args, conn)
		case "XADD":
			s.handleXADD(args, conn)
		case "TYPE":
			s.handleTYPE(args, conn)
		case "XRANGE":
			s.handleXRANGE(args, conn)
		case "XREAD":
			s.handleXREAD(args, conn)
		case "INCR":
			if len(args) >= 2 {
				key := args[1]

				s.store.mutex.Lock()
				item, exists := s.store.data[key]

				// Check if key exists and is not expired
				if exists && (item.expiry == nil || item.expiry.After(time.Now())) {
					// Try to parse the current value as an integer
					currentValue, err := strconv.Atoi(item.value)
					if err != nil {
						s.store.mutex.Unlock()
						conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
						continue
					}

					// Increment the value
					newValue := currentValue + 1

					// Store the new value back
					item.value = strconv.Itoa(newValue)
					s.store.data[key] = item
					s.store.mutex.Unlock()

					// Return the new value as a RESP integer
					response := fmt.Sprintf(":%d\r\n", newValue)
					conn.Write([]byte(response))
				} else {
					// Key doesn't exist or is expired, set to 1
					s.store.data[key] = storeItem{value: "1"}
					s.store.mutex.Unlock()

					// Return 1 as a RESP integer
					conn.Write([]byte(":1\r\n"))
				}
			}
		case "INFO":
			if len(args) >= 2 && strings.ToLower(args[1]) == "replication" {
				// Return replication section with appropriate role
				var info string
				if s.replication.isReplica {
					info = "role:slave"
				} else {
					info = fmt.Sprintf("role:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", s.replication.masterReplid, s.replication.masterReplOffset)
				}
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)
				conn.Write([]byte(response))
			} else {
				// For now, only support replication section
				conn.Write([]byte("-ERR unknown section or missing argument\r\n"))
			}
		case "REPLCONF":
			// Handle REPLCONF command from s.replication.replicas during handshake
			// We can ignore the arguments and just respond with OK
			conn.Write([]byte("+OK\r\n"))
		case "PSYNC":
			// Handle PSYNC command from s.replication.replicas during handshake
			// Respond with FULLRESYNC <REPL_ID> <OFFSET>
			response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", s.replication.masterReplid, s.replication.masterReplOffset)
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
			s.replication.replicaConnMutex.Lock()
			s.replication.replicaConnections[conn] = true
			s.replication.replicaConnMutex.Unlock()

			// Add this connection to the list of s.replication.replicas
			s.replication.replicasMutex.Lock()
			s.replication.replicas = append(s.replication.replicas, &replicaConnection{
				conn:      conn,
				reader:    reader,
				isReplica: true,
			})
			s.replication.replicasMutex.Unlock()

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

			s.replication.replicasMutex.RLock()
			numReplicas := len(s.replication.replicas)
			s.replication.replicasMutex.RUnlock()

			// If no s.replication.replicas, return 0 immediately
			if numReplicas == 0 {
				conn.Write([]byte(":0\r\n"))
				continue
			}

			// If no writes have been made (offset is 0), all s.replication.replicas are in sync
			if s.replication.masterReplOffset == 0 {
				response := fmt.Sprintf(":%d\r\n", numReplicas)
				conn.Write([]byte(response))
				continue
			}

			// Send REPLCONF GETACK to all s.replication.replicas
			getackCmd := "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"

			// Create channels to receive ACK responses
			type ackResponse struct {
				offset int
				err    error
			}
			ackChan := make(chan ackResponse, numReplicas)

			s.replication.replicasMutex.RLock()
			for _, replica := range s.replication.replicas {
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
					ackArgs, _, err := ParseRESPWithBytes(r.reader)
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
			s.replication.replicasMutex.RUnlock()

			// Wait for responses or timeout
			timeout := time.After(time.Duration(timeoutMs) * time.Millisecond)
			ackedCount := 0
			responsesReceived := 0

		waitLoop:
			for responsesReceived < numReplicas {
				select {
				case ack := <-ackChan:
					responsesReceived++
					if ack.err == nil && ack.offset >= s.replication.masterReplOffset {
						ackedCount++
						// Check if we've reached the expected number of s.replication.replicas
						if ackedCount >= expectedReplicas {
							response := fmt.Sprintf(":%d\r\n", ackedCount)
							conn.Write([]byte(response))
							break waitLoop
						}
					}
				case <-timeout:
					// Timeout expired, return the count of s.replication.replicas that have acknowledged
					response := fmt.Sprintf(":%d\r\n", ackedCount)
					conn.Write([]byte(response))
					break waitLoop
				}
			}

			// All s.replication.replicas responded (only reached if loop completes normally)
			if responsesReceived >= numReplicas {
				response := fmt.Sprintf(":%d\r\n", ackedCount)
				conn.Write([]byte(response))
			}
		case "MULTI":
			s.transactions.mutex.Lock()
			s.transactions.states[conn] = true
			s.transactions.queues[conn] = make([][]string, 0) // Initialize empty queue
			s.transactions.mutex.Unlock()
			conn.Write([]byte("+OK\r\n"))
		case "EXEC":
			s.transactions.mutex.RLock()
			inTransaction := s.transactions.states[conn]
			s.transactions.mutex.RUnlock()

			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			} else {
				// Get the queued commands before cleaning up
				s.transactions.mutex.RLock()
				queue := s.transactions.queues[conn]
				s.transactions.mutex.RUnlock()

				// Execute queued commands and collect responses
				var responses []string

				for _, cmdArgs := range queue {
					response := s.executeCommand(cmdArgs, conn)
					responses = append(responses, response)
				}

				// Clean up transaction state
				s.transactions.mutex.Lock()
				delete(s.transactions.states, conn)
				delete(s.transactions.queues, conn)
				s.transactions.mutex.Unlock()

				// Send array of responses
				result := fmt.Sprintf("*%d\r\n", len(responses))
				for _, response := range responses {
					result += response
				}
				conn.Write([]byte(result))
			}
		case "DISCARD":
			s.transactions.mutex.RLock()
			inTransaction := s.transactions.states[conn]
			s.transactions.mutex.RUnlock()

			if !inTransaction {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			} else {
				// Clear transaction state and queued commands
				s.transactions.mutex.Lock()
				delete(s.transactions.states, conn)
				delete(s.transactions.queues, conn)
				s.transactions.mutex.Unlock()

				conn.Write([]byte("+OK\r\n"))
			}
		case "CONFIG":
			if len(args) >= 3 && strings.ToUpper(args[1]) == "GET" {
				paramName := strings.ToLower(args[2])
				var paramValue string

				switch paramName {
				case "dir":
					paramValue = s.config.dir
				case "dbfilename":
					paramValue = s.config.dbFilename
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
					s.store.mutex.RLock()
					keys := make([]string, 0, len(s.store.data))
					for key := range s.store.data {
						keys = append(keys, key)
					}
					s.store.mutex.RUnlock()

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
			s.handleSUBSCRIBE(args, conn)
		case "PUBLISH":
			s.handlePUBLISH(args, conn)
		case "UNSUBSCRIBE":
			s.handleUNSUBSCRIBE(args, conn)
		case "ZADD":
			s.handleZADD(args, conn)
		case "ZRANK":
			s.handleZRANK(args, conn)
		case "ZRANGE":
			s.handleZRANGE(args, conn)
		case "ZCARD":
			s.handleZCARD(args, conn)
		case "ZSCORE":
			s.handleZSCORE(args, conn)
		case "ZREM":
			s.handleZREM(args, conn)
		case "GEOADD":
			s.handleGEOADD(args, conn)
		case "GEOPOS":
			s.handleGEOPOS(args, conn)
		case "GEODIST":
			s.handleGEODIST(args, conn)
		case "GEOSEARCH":
			s.handleGEOSEARCH(args, conn)
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Create server instance
	s := NewServer()

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
				s.replication.isReplica = true
				s.replication.masterHost = parts[0]
				s.replication.masterPort = parts[1]
			}
		} else if arg == "--dir" && i+1 < len(os.Args) {
			s.config.dir = os.Args[i+1]
		} else if arg == "--dbfilename" && i+1 < len(os.Args) {
			s.config.dbFilename = os.Args[i+1]
		}
	}

	// Load RDB file if dir and dbfilename are specified
	if s.config.dir != "" && s.config.dbFilename != "" {
		rdbPath := filepath.Join(s.config.dir, s.config.dbFilename)
		if err := s.LoadRDB(rdbPath); err != nil {
			fmt.Printf("Error loading RDB file: %s\n", err.Error())
		} else {
			fmt.Printf("Loaded RDB file from %s\n", rdbPath)
		}
	}

	// If running as replica, connect to master and perform handshake
	if s.replication.isReplica {
		go func() {
			masterAddr := net.JoinHostPort(s.replication.masterHost, s.replication.masterPort)
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
				args, bytesRead, err := ParseRESPWithBytes(reader)
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
						s.replication.replicaOffsetMutex.Lock()
						currentOffset := s.replication.replicaOffset
						s.replication.replicaOffsetMutex.Unlock()

						// Respond with REPLCONF ACK <offset>
						offsetStr := strconv.Itoa(currentOffset)
						ackResponse := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)
						_, err := masterConn.Write([]byte(ackResponse))
						if err != nil {
							fmt.Printf("Failed to send ACK response: %s\n", err.Error())
						}
						fmt.Printf("Sent REPLCONF ACK %d\n", currentOffset)

						// Now update the offset to include this REPLCONF GETACK command
						s.replication.replicaOffsetMutex.Lock()
						s.replication.replicaOffset += bytesRead
						s.replication.replicaOffsetMutex.Unlock()
					} else {
						// Other REPLCONF commands (not GETACK) - just update offset
						s.replication.replicaOffsetMutex.Lock()
						s.replication.replicaOffset += bytesRead
						s.replication.replicaOffsetMutex.Unlock()
					}
				case "PING":
					// PING command from master - just update offset
					s.replication.replicaOffsetMutex.Lock()
					s.replication.replicaOffset += bytesRead
					s.replication.replicaOffsetMutex.Unlock()
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

						s.store.mutex.Lock()
						s.store.data[key] = storeItem{value: value, expiry: expiry}
						s.store.mutex.Unlock()

						fmt.Printf("Replica processed SET %s %s\n", key, value)
					}

					// Update offset for SET command
					s.replication.replicaOffsetMutex.Lock()
					s.replication.replicaOffset += bytesRead
					s.replication.replicaOffsetMutex.Unlock()
				// Add other write commands here as needed
				default:
					fmt.Printf("Replica received command: %s\n", command)
					// Update offset for unknown commands too
					s.replication.replicaOffsetMutex.Lock()
					s.replication.replicaOffset += bytesRead
					s.replication.replicaOffsetMutex.Unlock()
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

		go s.handleClient(conn)
	}
}
