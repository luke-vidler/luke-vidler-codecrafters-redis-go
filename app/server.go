package main

import (
	"net"
	"strconv"
	"sync"
)

// Store manages the key-value storage
type Store struct {
	data  map[string]storeItem
	mutex sync.RWMutex
}

// BlockingManager manages blocked clients for list and stream operations
type BlockingManager struct {
	listClients   map[string][]blockedClient
	streamClients map[string][]blockedStreamClient
	mutex         sync.RWMutex
}

// TransactionManager manages transaction state for connections
type TransactionManager struct {
	states map[net.Conn]bool
	queues map[net.Conn][][]string
	mutex  sync.RWMutex
}

// ReplicationManager manages replication state
type ReplicationManager struct {
	isReplica          bool
	masterHost         string
	masterPort         string
	masterReplid       string
	masterReplOffset   int
	replicas           []*replicaConnection
	replicasMutex      sync.RWMutex
	replicaOffset      int
	replicaOffsetMutex sync.Mutex
	replicaConnections map[net.Conn]bool
	replicaConnMutex   sync.RWMutex
}

// Config holds server configuration
type Config struct {
	dir        string
	dbFilename string
}

// PubSubManager manages pub/sub subscriptions
type PubSubManager struct {
	clientSubscriptions map[net.Conn][]string
	channelSubscribers  map[string][]net.Conn
	mutex               sync.RWMutex
}

// Server encapsulates all server state
type Server struct {
	store        *Store
	blocking     *BlockingManager
	transactions *TransactionManager
	replication  *ReplicationManager
	config       *Config
	pubsub       *PubSubManager
}

// NewServer creates a new server instance
func NewServer() *Server {
	return &Server{
		store: &Store{
			data: make(map[string]storeItem),
		},
		blocking: &BlockingManager{
			listClients:   make(map[string][]blockedClient),
			streamClients: make(map[string][]blockedStreamClient),
		},
		transactions: &TransactionManager{
			states: make(map[net.Conn]bool),
			queues: make(map[net.Conn][][]string),
		},
		replication: &ReplicationManager{
			masterReplid:       "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			replicas:           make([]*replicaConnection, 0),
			replicaConnections: make(map[net.Conn]bool),
		},
		config: &Config{},
		pubsub: &PubSubManager{
			clientSubscriptions: make(map[net.Conn][]string),
			channelSubscribers:  make(map[string][]net.Conn),
		},
	}
}

// propagateToReplicas sends a command to all connected replicas and returns the number of bytes sent
func (s *Server) propagateToReplicas(args []string) int {
	if len(args) == 0 {
		return 0
	}

	// Build RESP array for the command
	resp := ""
	for i, arg := range args {
		if i == 0 {
			resp += "*" + strconv.Itoa(len(args)) + "\r\n"
		}
		resp += "$" + strconv.Itoa(len(arg)) + "\r\n" + arg + "\r\n"
	}

	s.replication.replicasMutex.RLock()
	defer s.replication.replicasMutex.RUnlock()

	// Send to all replicas
	for _, replica := range s.replication.replicas {
		replica.conn.Write([]byte(resp))
	}

	return len(resp)
}

// notifyBlockedClients notifies blocked clients waiting on a list key
func (s *Server) notifyBlockedClients(key string) {
	s.blocking.mutex.Lock()
	defer s.blocking.mutex.Unlock()

	clients, exists := s.blocking.listClients[key]
	if !exists || len(clients) == 0 {
		return
	}

	// Process the first (longest waiting) blocked client
	client := clients[0]
	s.blocking.listClients[key] = clients[1:] // Remove the first client

	// If no more clients, remove the key
	if len(s.blocking.listClients[key]) == 0 {
		delete(s.blocking.listClients, key)
	}

	// Try to pop an element for the unblocked client (synchronously)
	s.store.mutex.Lock()
	item, exists := s.store.data[key]
	if exists && len(item.list) > 0 {
		element := item.list[0]
		item.list = item.list[1:]
		s.store.data[key] = item
		s.store.mutex.Unlock()

		// Send response through channel
		response := "*2\r\n$"
		response += strconv.Itoa(len(client.listKey)) + "\r\n" + client.listKey + "\r\n$"
		response += strconv.Itoa(len(element)) + "\r\n" + element + "\r\n"

		select {
		case client.resultCh <- response:
		default:
			// Client might be gone, ignore
		}
	} else {
		s.store.mutex.Unlock()
	}
}

// notifyBlockedStreamClients notifies blocked clients waiting on a stream key
func (s *Server) notifyBlockedStreamClients(key string) {
	s.blocking.mutex.Lock()
	defer s.blocking.mutex.Unlock()

	clients, exists := s.blocking.streamClients[key]
	if !exists || len(clients) == 0 {
		return
	}

	// Process all blocked clients for this stream key
	var remainingClients []blockedStreamClient

	for _, client := range clients {
		// Check if this client has new entries available
		hasNewEntries := false
		var streamResults []string

		s.store.mutex.RLock()
		for i, streamKey := range client.streamKeys {
			if streamKey != key {
				continue // Only check the stream that was updated
			}

			startID := client.streamIDs[i]
			item, exists := s.store.data[streamKey]

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
				streamResult := "*2\r\n$" + strconv.Itoa(len(streamKey)) + "\r\n" + streamKey + "\r\n*"
				streamResult += strconv.Itoa(len(filteredEntries)) + "\r\n"
				for _, entry := range filteredEntries {
					// Each entry is an array: [id, [field1, value1, field2, value2, ...]]
					fieldCount := len(entry.fields) * 2
					streamResult += "*2\r\n$" + strconv.Itoa(len(entry.id)) + "\r\n" + entry.id + "\r\n*"
					streamResult += strconv.Itoa(fieldCount) + "\r\n"
					// Add field-value pairs
					for field, value := range entry.fields {
						streamResult += "$" + strconv.Itoa(len(field)) + "\r\n" + field + "\r\n$"
						streamResult += strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
					}
				}
				streamResults = append(streamResults, streamResult)
				break // Found new entries for this client
			}
		}
		s.store.mutex.RUnlock()

		if hasNewEntries {
			// Build final response: array of stream results
			response := "*" + strconv.Itoa(len(streamResults)) + "\r\n"
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
		delete(s.blocking.streamClients, key)
	} else {
		s.blocking.streamClients[key] = remainingClients
	}
}
