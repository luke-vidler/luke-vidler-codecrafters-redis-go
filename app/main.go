package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var (
	store      = make(map[string]string)
	storeMutex sync.RWMutex
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

				storeMutex.Lock()
				store[key] = value
				storeMutex.Unlock()

				conn.Write([]byte("+OK\r\n"))
			}
		case "GET":
			if len(args) >= 2 {
				key := args[1]

				storeMutex.RLock()
				value, exists := store[key]
				storeMutex.RUnlock()

				if exists {
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
					conn.Write([]byte(response))
				} else {
					conn.Write([]byte("$-1\r\n"))
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
