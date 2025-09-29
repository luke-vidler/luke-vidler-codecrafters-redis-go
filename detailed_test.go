package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os/exec"
	"time"
)

func testRedisServer(port string, args []string) {
	// Start the server
	cmd := exec.Command("./main", args...)
	err := cmd.Start()
	if err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}
	defer cmd.Process.Kill()

	// Wait a moment for server to start
	time.Sleep(500 * time.Millisecond)

	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:"+port)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Send INFO replication command
	_, err = conn.Write([]byte("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n"))
	if err != nil {
		fmt.Printf("Failed to send command: %v\n", err)
		return
	}

	// Read response
	reader := bufio.NewReader(conn)

	// Read the length line
	lengthLine, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Failed to read length line: %v\n", err)
		return
	}
	fmt.Printf("Length line: %s", lengthLine)

	// Read the content
	content, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		fmt.Printf("Failed to read content: %v\n", err)
		return
	}

	fmt.Printf("Full content: %q\n", content)
	fmt.Printf("Parsed content:\n")

	// Split by \r\n to show individual lines
	lines := ""
	for i, char := range content {
		if char == '\r' {
			if i+1 < len(content) && rune(content[i+1]) == '\n' {
				fmt.Printf("  %s\n", lines)
				lines = ""
				continue
			}
		} else if char == '\n' && i > 0 && rune(content[i-1]) == '\r' {
			continue
		} else if char != '\n' {
			lines += string(char)
		}
	}
	if lines != "" {
		fmt.Printf("  %s\n", lines)
	}
}

func main() {
	// Test master mode
	fmt.Println("=== Testing master mode ===")
	testRedisServer("6380", []string{"--port", "6380"})

	fmt.Println("\n=== Testing replica mode ===")
	testRedisServer("6381", []string{"--port", "6381", "--replicaof", "localhost 6379"})
}
