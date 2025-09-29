package main

import (
	"bufio"
	"fmt"
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
	time.Sleep(100 * time.Millisecond)

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
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		return
	}

	// Read the next line (actual content)
	content, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Failed to read content: %v\n", err)
		return
	}

	fmt.Printf("Response: %s", response)
	fmt.Printf("Content: %s", content)
}

func main() {
	// Test master mode
	fmt.Println("Testing master mode...")
	testRedisServer("6380", []string{"--port", "6380"})

	fmt.Println("\nTesting replica mode...")
	testRedisServer("6381", []string{"--port", "6381", "--replicaof", "localhost 6379"})
}
