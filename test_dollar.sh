#!/bin/bash

# Test script for $ functionality in XREAD BLOCK

echo "Testing $ functionality..."

# Add initial entry
echo "Adding initial entry..."
echo -e '*5\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$3\r\n0-1\r\n$11\r\ntemperature\r\n$2\r\n96\r\n' | nc 127.0.0.1 6379

# Test 1: XREAD with $ should return empty (no new entries after last one)
echo "Test 1: XREAD with $ (should be empty)"
echo -e '*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$10\r\nstream_key\r\n$1\r\n$\r\n' | nc 127.0.0.1 6379

# Test 2: Add another entry first, then XREAD with $ from before that entry
echo "Adding second entry..."
echo -e '*5\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$3\r\n0-2\r\n$11\r\ntemperature\r\n$2\r\n95\r\n' | nc 127.0.0.1 6379

echo "Test 2: XREAD with $ should now return the second entry"
echo -e '*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$10\r\nstream_key\r\n$3\r\n0-1\r\n' | nc 127.0.0.1 6379

echo "Test 3: XREAD with $ should be empty again"
echo -e '*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$10\r\nstream_key\r\n$1\r\n$\r\n' | nc 127.0.0.1 6379
