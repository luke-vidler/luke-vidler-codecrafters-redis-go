package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// handlePING handles the PING command which returns PONG
func (s *Server) handlePING(args []string, conn interface{}) string {
	return "+PONG\r\n"
}

// handleECHO handles the ECHO command which returns the argument
func (s *Server) handleECHO(args []string, conn interface{}) string {
	if len(args) >= 2 {
		arg := args[1]
		return fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return "$-1\r\n"
}

// handleSET handles the SET command which sets a key-value pair with optional expiry
func (s *Server) handleSET(args []string, conn interface{}) string {
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

		return "+OK\r\n"
	}
	return "-ERR wrong number of arguments\r\n"
}

// handleGET handles the GET command which retrieves a value by key
func (s *Server) handleGET(args []string, conn interface{}) string {
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
}

// handleINCR handles the INCR command which increments the integer value of a key by 1
func (s *Server) handleINCR(args []string, conn interface{}) string {
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
				return "-ERR value is not an integer or out of range\r\n"
			}

			// Increment the value
			newValue := currentValue + 1

			// Store the new value back
			item.value = strconv.Itoa(newValue)
			s.store.data[key] = item
			s.store.mutex.Unlock()

			// Return the new value as a RESP integer
			return fmt.Sprintf(":%d\r\n", newValue)
		} else {
			// Key doesn't exist or is expired, set to 1
			s.store.data[key] = storeItem{value: "1"}
			s.store.mutex.Unlock()

			// Return 1 as a RESP integer
			return ":1\r\n"
		}
	}
	return "-ERR wrong number of arguments\r\n"
}
