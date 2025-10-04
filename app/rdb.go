package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

// LoadRDB loads an RDB file and populates the store.
// Returns nil if the file doesn't exist (treating it as an empty database).
func LoadRDB(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		// File doesn't exist or can't be opened - treat as empty database
		return nil
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read and verify magic string "REDIS"
	magic := make([]byte, 5)
	if _, err := io.ReadFull(reader, magic); err != nil {
		return fmt.Errorf("failed to read magic string: %w", err)
	}
	if string(magic) != "REDIS" {
		return fmt.Errorf("invalid RDB file: wrong magic string")
	}

	// Read version (4 bytes)
	version := make([]byte, 4)
	if _, err := io.ReadFull(reader, version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	// Parse the RDB file
	for {
		opcode, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read opcode: %w", err)
		}

		switch opcode {
		case 0xFF: // EOF
			// End of RDB file
			return nil
		case 0xFA: // AUX (auxiliary fields)
			// Read auxiliary field name and value, then skip them
			_, err := readString(reader)
			if err != nil {
				return fmt.Errorf("failed to read aux field name: %w", err)
			}
			_, err = readString(reader)
			if err != nil {
				return fmt.Errorf("failed to read aux field value: %w", err)
			}
			// Continue parsing, these are just metadata
		case 0xFE: // SELECTDB
			// Read database number (length-encoded)
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("failed to read database number: %w", err)
			}
			// We only support database 0, so just continue
		case 0xFB: // RESIZEDB
			// Read hash table size and expiry table size
			_, err := readLength(reader)
			if err != nil {
				return fmt.Errorf("failed to read hash table size: %w", err)
			}
			_, err = readLength(reader)
			if err != nil {
				return fmt.Errorf("failed to read expiry table size: %w", err)
			}
		case 0xFD: // EXPIRETIME (seconds)
			// Read 4-byte expiry timestamp
			expiryBytes := make([]byte, 4)
			if _, err := io.ReadFull(reader, expiryBytes); err != nil {
				return fmt.Errorf("failed to read expiry time: %w", err)
			}
			expirySeconds := binary.LittleEndian.Uint32(expiryBytes)
			expiryTime := time.Unix(int64(expirySeconds), 0)

			// Read the key-value pair with this expiry
			if err := readKeyValuePair(reader, &expiryTime); err != nil {
				return fmt.Errorf("failed to read key-value pair with expiry: %w", err)
			}
		case 0xFC: // EXPIRETIME_MS (milliseconds)
			// Read 8-byte expiry timestamp in milliseconds
			expiryBytes := make([]byte, 8)
			if _, err := io.ReadFull(reader, expiryBytes); err != nil {
				return fmt.Errorf("failed to read expiry time ms: %w", err)
			}
			expiryMillis := binary.LittleEndian.Uint64(expiryBytes)
			expiryTime := time.Unix(0, int64(expiryMillis)*int64(time.Millisecond))

			// Read the key-value pair with this expiry
			if err := readKeyValuePair(reader, &expiryTime); err != nil {
				return fmt.Errorf("failed to read key-value pair with expiry ms: %w", err)
			}
		default:
			// This is a value type byte, read key-value pair
			if err := readKeyValuePairWithType(reader, opcode, nil); err != nil {
				return fmt.Errorf("failed to read key-value pair: %w", err)
			}
		}
	}

	return nil
}

// readLength reads a length-encoded integer from the RDB file.
// Redis uses a special encoding where the first 2 bits indicate the encoding type.
func readLength(reader *bufio.Reader) (uint64, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// Check the first 2 bits to determine encoding
	encodingType := (firstByte & 0xC0) >> 6

	switch encodingType {
	case 0: // 00: The next 6 bits represent the length
		return uint64(firstByte & 0x3F), nil
	case 1: // 01: Read one additional byte. The combined 14 bits represent the length
		secondByte, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint64((firstByte&0x3F))<<8 | uint64(secondByte), nil
	case 2: // 10: Discard remaining 6 bits. The next 4 bytes represent the length
		lengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(reader, lengthBytes); err != nil {
			return 0, err
		}
		return uint64(binary.BigEndian.Uint32(lengthBytes)), nil
	case 3: // 11: Special encoding
		specialFormat := firstByte & 0x3F
		switch specialFormat {
		case 0: // 8-bit integer
			b, err := reader.ReadByte()
			return uint64(b), err
		case 1: // 16-bit integer
			bytes := make([]byte, 2)
			if _, err := io.ReadFull(reader, bytes); err != nil {
				return 0, err
			}
			return uint64(binary.LittleEndian.Uint16(bytes)), nil
		case 2: // 32-bit integer
			bytes := make([]byte, 4)
			if _, err := io.ReadFull(reader, bytes); err != nil {
				return 0, err
			}
			return uint64(binary.LittleEndian.Uint32(bytes)), nil
		default:
			return 0, fmt.Errorf("unsupported special encoding format: %d", specialFormat)
		}
	}

	return 0, fmt.Errorf("invalid length encoding")
}

// readString reads a length-prefixed string from the RDB file.
func readString(reader *bufio.Reader) (string, error) {
	// Read the length
	firstByte, err := reader.ReadByte()
	if err != nil {
		return "", err
	}

	// Check if this is a special encoding (first 2 bits = 11)
	encodingType := (firstByte & 0xC0) >> 6
	if encodingType == 3 {
		// Special encoding for integer stored as string
		specialFormat := firstByte & 0x3F
		switch specialFormat {
		case 0: // 8-bit integer
			b, err := reader.ReadByte()
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int8(b)), nil
		case 1: // 16-bit integer
			bytes := make([]byte, 2)
			if _, err := io.ReadFull(reader, bytes); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(bytes))), nil
		case 2: // 32-bit integer
			bytes := make([]byte, 4)
			if _, err := io.ReadFull(reader, bytes); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(bytes))), nil
		default:
			return "", fmt.Errorf("unsupported special string encoding: %d", specialFormat)
		}
	}

	// Put the byte back since readLength expects it
	if err := reader.UnreadByte(); err != nil {
		return "", err
	}

	length, err := readLength(reader)
	if err != nil {
		return "", err
	}

	// Read the string content
	stringBytes := make([]byte, length)
	if _, err := io.ReadFull(reader, stringBytes); err != nil {
		return "", err
	}

	return string(stringBytes), nil
}

// readKeyValuePair reads a key-value pair from the RDB file (when the value type byte hasn't been read yet).
func readKeyValuePair(reader *bufio.Reader, expiry *time.Time) error {
	// Read value type
	valueType, err := reader.ReadByte()
	if err != nil {
		return err
	}

	return readKeyValuePairWithType(reader, valueType, expiry)
}

// readKeyValuePairWithType reads a key-value pair when the value type byte has already been read.
func readKeyValuePairWithType(reader *bufio.Reader, valueType byte, expiry *time.Time) error {
	// Read key
	key, err := readString(reader)
	if err != nil {
		return fmt.Errorf("failed to read key: %w", err)
	}

	// Read value based on type
	switch valueType {
	case 0: // String encoding
		value, err := readString(reader)
		if err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}

		// Store in the global store
		storeMutex.Lock()
		store[key] = storeItem{
			value:  value,
			expiry: expiry,
		}
		storeMutex.Unlock()

	default:
		return fmt.Errorf("unsupported value type: %d", valueType)
	}

	return nil
}
