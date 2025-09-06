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

type storeItem struct {
	value  string
	list   []string
	expiry *time.Time
}

type blockedClient struct {
	conn      net.Conn
	listKey   string
	timestamp time.Time
	resultCh  chan string // Channel to send result when unblocked
}

var (
	store          = make(map[string]storeItem)
	storeMutex     sync.RWMutex
	blockedClients = make(map[string][]blockedClient) // key -> list of blocked clients
	blockedMutex   sync.RWMutex
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

func handleClient(conn net.Conn) {
	defer conn.Close()
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
