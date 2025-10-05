# Redis Clone in Go

[![progress-banner](https://backend.codecrafters.io/progress/redis/b364bdaf-bd07-4c8f-a53f-b515cdcf8b81)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

A feature-rich Redis clone implementation in Go, built as part of the [CodeCrafters "Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis). This implementation supports a comprehensive set of Redis commands and features including replication, transactions, pub/sub, streams, sorted sets, and geospatial operations.

## Features

### Core Commands
- **String Operations**: `GET`, `SET`, `INCR`, `ECHO`
- **Key Management**: `KEYS`, `TYPE`
- **Server**: `PING`, `INFO`, `CONFIG`

### List Operations
- `LPUSH`, `RPUSH` - Add elements to list
- `LPOP` - Remove and return element from list
- `BLPOP` - Blocking list pop with timeout
- `LRANGE` - Get range of elements
- `LLEN` - Get list length

### Stream Operations
- `XADD` - Add entries to stream
- `XRANGE` - Query stream entries by ID range
- `XREAD` - Read from one or multiple streams (with blocking support)

### Sorted Set Operations
- `ZADD` - Add members with scores
- `ZRANK` - Get rank of member
- `ZRANGE` - Get range of members by index
- `ZCARD` - Get cardinality (count)
- `ZSCORE` - Get score of member
- `ZREM` - Remove members

### Geospatial Operations
- `GEOADD` - Add geospatial items (longitude, latitude)
- `GEOPOS` - Get positions of members
- `GEODIST` - Calculate distance between members
- `GEOSEARCH` - Search for members within radius or box

### Pub/Sub
- `SUBSCRIBE` - Subscribe to channels
- `PUBLISH` - Publish messages to channels
- `UNSUBSCRIBE` - Unsubscribe from channels

### Replication
- Master-replica replication with `REPLICAOF` configuration
- `PSYNC` - Partial synchronization support
- `REPLCONF` - Replication configuration
- `WAIT` - Wait for replicas to acknowledge writes
- Automatic command propagation to replicas

### Transactions
- `MULTI` - Start transaction
- `EXEC` - Execute queued commands
- `DISCARD` - Discard transaction

### Advanced Features
- **TTL Support**: Set expiration times with `px` option on `SET`
- **RDB Persistence**: Load data from RDB files on startup
- **RESP Protocol**: Full Redis Serialization Protocol implementation
- **Concurrent Connections**: Multi-client support with goroutines
- **Blocking Operations**: Timeout-based blocking for lists and streams

## Architecture

The codebase is organized into modular components:

- `main.go` - Server initialization, connection handling, command routing
- `server.go` - Core server state management and replication
- `resp.go` - RESP protocol parser and encoder
- `rdb.go` - RDB file format parser
- `commands_string.go` - String command handlers
- `commands_list.go` - List command handlers
- `commands_stream.go` - Stream command handlers
- `commands_sorted_set.go` - Sorted set command handlers
- `commands_geo.go` - Geospatial command handlers
- `commands_pubsub.go` - Pub/Sub command handlers
- `geospatial.go` - Geospatial utilities (Geohash encoding)

## Building and Running

### Prerequisites
- Go 1.24 or higher

### Build
```sh
cd app
go build -o ../redis-server .
```

### Run

**Basic server:**
```sh
./redis-server
# Or using the wrapper script:
./your_program.sh
```

**With custom port:**
```sh
./redis-server --port 6380
```

**With RDB persistence:**
```sh
./redis-server --dir /path/to/data --dbfilename dump.rdb
```

**As replica:**
```sh
./redis-server --replicaof <master_host> <master_port>
```

**Combined options:**
```sh
./redis-server --port 6380 --dir ./data --dbfilename dump.rdb
```

## Testing

### Manual Testing with redis-cli

1. Start the server:
   ```sh
   ./redis-server
   ```

2. Connect with redis-cli:
   ```sh
   redis-cli -p 6379
   ```

3. Try some commands:
   ```
   PING
   SET mykey "Hello"
   GET mykey
   LPUSH mylist "world"
   LPUSH mylist "hello"
   LRANGE mylist 0 -1
   XADD mystream * temperature 20 humidity 50
   XRANGE mystream - +
   ```

### Testing Replication

**Terminal 1 - Start master:**
```sh
./redis-server --port 6379
```

**Terminal 2 - Start replica:**
```sh
./redis-server --port 6380 --replicaof localhost 6379
```

**Terminal 3 - Test replication:**
```sh
# Connect to master
redis-cli -p 6379
> SET foo bar
> WAIT 1 1000

# Connect to replica
redis-cli -p 6380
> GET foo
"bar"
```

### Testing Pub/Sub

**Terminal 1 - Subscriber:**
```sh
redis-cli -p 6379
> SUBSCRIBE channel1
```

**Terminal 2 - Publisher:**
```sh
redis-cli -p 6379
> PUBLISH channel1 "Hello subscribers!"
```

### Testing Transactions

```sh
redis-cli -p 6379
> MULTI
> SET key1 value1
> SET key2 value2
> INCR counter
> EXEC
```

### Testing Geospatial

```sh
redis-cli -p 6379
> GEOADD locations 13.361389 38.115556 "Palermo"
> GEOADD locations 15.087269 37.502669 "Catania"
> GEODIST locations Palermo Catania km
"166.2742"
> GEOSEARCH locations FROMLONLAT 15 37 BYRADIUS 200 km WITHDIST
```

## Development

### Running CodeCrafters Tests

```sh
git add .
git commit -m "your commit message"
git push origin master
```

Test output will be streamed to your terminal by CodeCrafters.

### Code Style

The project follows standard Go conventions:
- Use `gofmt` for formatting
- Descriptive variable and function names
- Comments for exported functions and complex logic

## Implementation Notes

### RESP Protocol
The implementation uses the Redis Serialization Protocol (RESP) for all client-server communication. The parser in `resp.go` handles:
- Simple strings (`+`)
- Errors (`-`)
- Integers (`:`)
- Bulk strings (`$`)
- Arrays (`*`)

### Replication
Replication uses the standard Redis replication protocol:
1. Replica sends `PING` to master
2. Replica sends `REPLCONF` for configuration
3. Replica sends `PSYNC` to start sync
4. Master sends RDB snapshot
5. Master propagates write commands to replicas

### Concurrency
- Global store protected with `sync.RWMutex`
- Separate mutexes for blocking operations, transactions, replication, and pub/sub
- Each client connection handled in its own goroutine

## Troubleshooting

**Port already in use:**
```sh
# Find process using port 6379
lsof -i :6379
# Kill it or use a different port
./redis-server --port 6380
```

**Connection refused:**
- Ensure server is running
- Check firewall settings
- Verify correct port number

**Replication not working:**
- Check master is accessible from replica
- Verify port is open
- Check logs for connection errors

## License

This project is part of the CodeCrafters challenge and is intended for educational purposes.

## Resources

- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [Redis Commands Reference](https://redis.io/commands/)
- [CodeCrafters Challenge](https://codecrafters.io/challenges/redis)
