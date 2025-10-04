package main

import (
	"fmt"
	"net"
)

// handleSUBSCRIBE handles the SUBSCRIBE command which subscribes a client to one or more channels
func handleSUBSCRIBE(args []string, conn net.Conn) {
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
}

// handlePUBLISH handles the PUBLISH command which publishes a message to a channel
func handlePUBLISH(args []string, conn net.Conn) {
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
}

// handleUNSUBSCRIBE handles the UNSUBSCRIBE command which unsubscribes a client from channels
func handleUNSUBSCRIBE(args []string, conn net.Conn) {
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
}
