#!/bin/bash

echo "Testing master mode..."
echo -e '*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n' | ./main --port 6380 &
SERVER_PID=$!
sleep 1
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo
echo "Testing replica mode..."
echo -e '*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n' | ./main --port 6381 --replicaof "localhost 6379" &
SERVER_PID=$!
sleep 1
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Test completed."
