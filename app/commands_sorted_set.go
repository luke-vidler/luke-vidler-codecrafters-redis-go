package main

import (
	"fmt"
	"net"
	"strconv"
)

// handleZADD handles the ZADD command which adds members with scores to a sorted set
func (s *Server) handleZADD(args []string, conn net.Conn) {
	if len(args) >= 4 {
		key := args[1]
		scoreStr := args[2]
		member := args[3]

		// Parse score as float64
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			conn.Write([]byte("-ERR value is not a valid float\r\n"))
			return
		}

		s.store.mutex.Lock()
		item, exists := s.store.data[key]

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
			s.store.data[key] = storeItem{
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

			s.store.data[key] = item
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

			s.store.data[key] = item
		}

		s.store.mutex.Unlock()

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
}

// handleZRANK handles the ZRANK command which returns the rank of a member in a sorted set
func (s *Server) handleZRANK(args []string, conn net.Conn) {
	if len(args) >= 3 {
		key := args[1]
		member := args[2]

		s.store.mutex.RLock()
		item, exists := s.store.data[key]
		s.store.mutex.RUnlock()

		// Check if sorted set exists
		if !exists || len(item.sortedSet) == 0 {
			// Key doesn't exist or is not a sorted set
			conn.Write([]byte("$-1\r\n"))
			return
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
}

// handleZRANGE handles the ZRANGE command which returns members in a sorted set within a range
func (s *Server) handleZRANGE(args []string, conn net.Conn) {
	if len(args) >= 4 {
		key := args[1]
		startStr := args[2]
		stopStr := args[3]

		// Parse start and stop indices
		start, err := strconv.Atoi(startStr)
		if err != nil {
			conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			return
		}

		stop, err := strconv.Atoi(stopStr)
		if err != nil {
			conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			return
		}

		s.store.mutex.RLock()
		item, exists := s.store.data[key]
		s.store.mutex.RUnlock()

		// If sorted set doesn't exist, return empty array
		if !exists || len(item.sortedSet) == 0 {
			conn.Write([]byte("*0\r\n"))
			return
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
			return
		}

		// If stop >= setLen, treat stop as last element
		if stop >= setLen {
			stop = setLen - 1
		}

		// If start > stop, return empty array
		if start > stop {
			conn.Write([]byte("*0\r\n"))
			return
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
}

// handleZCARD handles the ZCARD command which returns the cardinality of a sorted set
func (s *Server) handleZCARD(args []string, conn net.Conn) {
	if len(args) >= 2 {
		key := args[1]

		s.store.mutex.RLock()
		item, exists := s.store.data[key]
		s.store.mutex.RUnlock()

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
}

// handleZSCORE handles the ZSCORE command which returns the score of a member in a sorted set
func (s *Server) handleZSCORE(args []string, conn net.Conn) {
	if len(args) >= 3 {
		key := args[1]
		member := args[2]

		s.store.mutex.RLock()
		item, exists := s.store.data[key]
		s.store.mutex.RUnlock()

		// Check if sorted set exists
		if !exists || len(item.sortedSet) == 0 {
			// Key doesn't exist or is not a sorted set
			conn.Write([]byte("$-1\r\n"))
			return
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
}

// handleZREM handles the ZREM command which removes members from a sorted set
func (s *Server) handleZREM(args []string, conn net.Conn) {
	if len(args) >= 3 {
		key := args[1]
		member := args[2]

		s.store.mutex.Lock()
		item, exists := s.store.data[key]

		// Check if sorted set exists
		if !exists || len(item.sortedSet) == 0 {
			// Key doesn't exist or is not a sorted set
			s.store.mutex.Unlock()
			conn.Write([]byte(":0\r\n"))
			return
		}

		// Find and remove the member
		removed := 0
		for i, m := range item.sortedSet {
			if m.member == member {
				// Remove this member
				item.sortedSet = append(item.sortedSet[:i], item.sortedSet[i+1:]...)
				s.store.data[key] = item
				removed = 1
				break
			}
		}

		s.store.mutex.Unlock()

		// Return number of members removed (1 if found, 0 if not)
		response := fmt.Sprintf(":%d\r\n", removed)
		conn.Write([]byte(response))
	} else {
		conn.Write([]byte("-ERR wrong number of arguments for 'zrem' command\r\n"))
	}
}
