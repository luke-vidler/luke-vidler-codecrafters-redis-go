package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// ParseRESP parses a RESP (Redis Serialization Protocol) array from the reader.
// Returns the parsed command arguments as a string slice.
//
// RESP format example:
//
//	*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
//
// This represents: ["ECHO", "hey"]
func ParseRESP(reader *bufio.Reader) ([]string, error) {
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

// ParseRESPWithBytes parses RESP and returns both the parsed args and the number of bytes read.
// This is used for replication offset tracking where we need to know exactly how many bytes
// were consumed from the input stream.
func ParseRESPWithBytes(reader *bufio.Reader) ([]string, int, error) {
	bytesRead := 0

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, 0, err
	}
	bytesRead += len(line)

	line = strings.TrimRight(line, "\r\n")

	if !strings.HasPrefix(line, "*") {
		return nil, 0, fmt.Errorf("expected array, got %s", line)
	}

	arrayLen, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, 0, err
	}

	args := make([]string, arrayLen)

	for i := 0; i < arrayLen; i++ {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		bytesRead += len(line)

		line = strings.TrimRight(line, "\r\n")

		if !strings.HasPrefix(line, "$") {
			return nil, 0, fmt.Errorf("expected bulk string, got %s", line)
		}

		strLen, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, 0, err
		}

		str, err := reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		bytesRead += len(str)

		args[i] = strings.TrimRight(str, "\r\n")[:strLen]
	}

	return args, bytesRead, nil
}
