package main

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

// handleRPUSH handles the RPUSH command which pushes elements to the end of a list
func (s *Server) handleRPUSH(args []string, conn net.Conn) {
	if len(args) >= 3 {
		key := args[1]
		elements := args[2:] // All elements from index 2 onwards
		s.store.mutex.Lock()
		item, exists := s.store.data[key]
		wasEmpty := !exists || len(item.list) == 0
		if !exists {
			// Create new list with all elements
			s.store.data[key] = storeItem{list: elements}
		} else {
			// Append all elements to existing list
			item.list = append(item.list, elements...)
			s.store.data[key] = item
		}
		listLen := len(s.store.data[key].list)
		s.store.mutex.Unlock()
		// Notify blocked clients if the list was empty before
		if wasEmpty {
			s.notifyBlockedClients(key)
		}
		// Return the number of elements in the list as RESP integer
		response := fmt.Sprintf(":%d\r\n", listLen)
		conn.Write([]byte(response))
	}
}

// handleLRANGE handles the LRANGE command which returns a range of elements from a list
func (s *Server) handleLRANGE(args []string, conn net.Conn) {
	if len(args) >= 4 {
		key := args[1]
		startStr := args[2]
		endStr := args[3]
		start, startErr := strconv.Atoi(startStr)
		end, endErr := strconv.Atoi(endStr)
		if startErr != nil || endErr != nil {
			conn.Write([]byte("-ERR invalid index\r\n"))
			return
		}
		s.store.mutex.RLock()
		item, exists := s.store.data[key]
		s.store.mutex.RUnlock()
		if !exists || len(item.list) == 0 {
			// List doesn't exist or is empty, return empty array
			conn.Write([]byte("*0\r\n"))
			return
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
			return
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
}

// handleLPUSH handles the LPUSH command which pushes elements to the beginning of a list
func (s *Server) handleLPUSH(args []string, conn net.Conn) {
	if len(args) >= 3 {
		key := args[1]
		elements := args[2:] // All elements from index 2 onwards
		s.store.mutex.Lock()
		item, exists := s.store.data[key]
		wasEmpty := !exists || len(item.list) == 0
		if !exists {
			// Create new list with all elements (reverse order for left insertion)
			newList := make([]string, len(elements))
			for i, element := range elements {
				newList[len(elements)-1-i] = element
			}
			s.store.data[key] = storeItem{list: newList}
		} else {
			// Prepend all elements to existing list (reverse order for left insertion)
			newList := make([]string, len(elements)+len(item.list))
			for i, element := range elements {
				newList[len(elements)-1-i] = element
			}
			copy(newList[len(elements):], item.list)
			item.list = newList
			s.store.data[key] = item
		}
		listLen := len(s.store.data[key].list)
		s.store.mutex.Unlock()
		// Notify blocked clients if the list was empty before
		if wasEmpty {
			s.notifyBlockedClients(key)
		}
		// Return the number of elements in the list as RESP integer
		response := fmt.Sprintf(":%d\r\n", listLen)
		conn.Write([]byte(response))
	}
}

// handleLLEN handles the LLEN command which returns the length of a list
func (s *Server) handleLLEN(args []string, conn net.Conn) {
	if len(args) >= 2 {
		key := args[1]
		s.store.mutex.RLock()
		item, exists := s.store.data[key]
		s.store.mutex.RUnlock()
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
}

// handleLPOP handles the LPOP command which removes and returns the first element of a list
func (s *Server) handleLPOP(args []string, conn net.Conn) {
	if len(args) >= 2 {
		key := args[1]
		count := 1 // Default count is 1
		// Parse optional count argument
		if len(args) >= 3 {
			if parsedCount, err := strconv.Atoi(args[2]); err == nil && parsedCount > 0 {
				count = parsedCount
			}
		}
		s.store.mutex.Lock()
		item, exists := s.store.data[key]
		if !exists || len(item.list) == 0 {
			// List doesn't exist or is empty
			s.store.mutex.Unlock()
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
			s.store.data[key] = item
			s.store.mutex.Unlock()
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
}

// handleBLPOP handles the BLPOP command which blocks until an element is available to pop
func (s *Server) handleBLPOP(args []string, conn net.Conn) {
	if len(args) >= 3 {
		key := args[1]
		timeoutStr := args[2]
		timeoutFloat, err := strconv.ParseFloat(timeoutStr, 64)
		if err != nil {
			conn.Write([]byte("-ERR timeout is not a float\r\n"))
			return
		}
		// Check if there's an element available immediately
		s.store.mutex.Lock()
		item, exists := s.store.data[key]
		if exists && len(item.list) > 0 {
			// Element available, pop it immediately
			element := item.list[0]
			item.list = item.list[1:]
			s.store.data[key] = item
			s.store.mutex.Unlock()
			// Return [list_key, element] as RESP array
			response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(key), key, len(element), element)
			conn.Write([]byte(response))
			return
		}
		s.store.mutex.Unlock()
		// No element available, block the client
		// Create result channel for this blocked client
		resultCh := make(chan string, 1)
		// Add client to blocked list
		s.blocking.mutex.Lock()
		s.blocking.listClients[key] = append(s.blocking.listClients[key], blockedClient{
			conn:      conn,
			listKey:   key,
			timestamp: time.Now(),
			resultCh:  resultCh,
		})
		s.blocking.mutex.Unlock()
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
			s.blocking.mutex.Lock()
			if clients, exists := s.blocking.listClients[key]; exists {
				for i, client := range clients {
					if client.conn == conn {
						s.blocking.listClients[key] = append(clients[:i], clients[i+1:]...)
						if len(s.blocking.listClients[key]) == 0 {
							delete(s.blocking.listClients, key)
						}
						break
					}
				}
			}
			s.blocking.mutex.Unlock()
			// Send null array for timeout
			conn.Write([]byte("*-1\r\n"))
		}
	}
}
