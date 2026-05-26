# Story 1.1: Broker Server Startup

Status: done

## Story

As a client,
I want to connect to a broker on port 9092,
so I can send requests and receive responses.

## Acceptance Criteria

1. **Given** broker is started with `java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker`
   **When** a client opens a TCP connection to port 9092
   **Then** the broker accepts the connection and is ready to receive requests

2. Broker uses Java NIO SocketChannel for non-blocking I/O (per NFR2)

3. Broker starts a selector and registers server socket channel for OP_ACCEPT

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Layer 5)

- `SimpleKafkaBroker.java` — main entry point
- `start()` — Initialize NIO server socket, load metadata, accept connections
- `handleClientConnection()` — Process client requests

### From NFRs

- **NFR1:** Pure Java implementation with no external dependencies (only Java standard library)
- **NFR2:** Use NIO SocketChannel for non-blocking I/O
- **NFR8:** Request header v2 format with TAG_BUFFER for forward compatibility
- **NFR9:** Response header v1 format with TAG_BUFFER

### Coding Conventions (from memory)

- **DRY & SOLID** — all code follows these patterns
- **Shared code** goes in `com.simplekafka.shared` package

## Project Structure

```
src/main/java/com/simplekafka/
├── shared/           # Shared code (protocol primitives, error codes)
│   └── ...
├── broker/           # Broker implementation
│   └── SimpleKafkaBroker.java
└── client/           # Client library
    └── ...
```

## Tasks / Subtasks

- [x] Task 1: Set up broker package structure (AC: #1)
  - [x] Subtask 1.1: Create `com.simplekafka.broker` package
  - [x] Subtask 1.2: Create `com.simplekafka.shared` package for future shared code
- [x] Task 2: Implement SimpleKafkaBroker with NIO server socket (AC: #1, #2, #3)
  - [x] Subtask 2.1: Create `start()` method that opens ServerSocketChannel on port 9092
  - [x] Subtask 2.2: Initialize NIO selector
  - [x] Subtask 2.3: Register server channel for OP_ACCEPT
  - [x] Subtask 2.4: Create selector loop to accept connections
- [x] Task 3: Create basic Main class entry point (AC: #1)
  - [x] Subtask 3.1: Create `com.simplekafka.Main` with main() method

## Dev Notes

### Key Implementation Points

1. **NIO Selector Pattern:**
   ```java
   Selector selector = Selector.open();
   ServerSocketChannel serverChannel = ServerSocketChannel.open();
   serverChannel.socket().bind(new InetSocketAddress(9092));
   serverChannel.configureBlocking(false);
   serverChannel.register(selector, SelectionKey.OP_ACCEPT);
   ```

2. **Accept Loop:**
   ```java
   while (true) {
       selector.select();
       Set<SelectionKey> keys = selector.selectedKeys();
       for (SelectionKey key : keys) {
           if (key.isAcceptable()) {
               // handle new connection
           }
       }
   }
   ```

3. **DRY/SOLID:** Keep methods small and focused. Extract common patterns.

### From Conventions (memory/feedback-dev-conventions.md)

- Shared code goes in `com.simplekafka.shared` — start with placeholder package
- Follow DRY (Don't Repeat Yourself) and SOLID principles
- Pure Java — no external dependencies

## File List

- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — NEW
- `src/main/java/com/simplekafka/Main.java` — UPDATE (replace placeholder)
- `src/main/java/com/simplekafka/shared/` — placeholder for shared package

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Created com.simplekafka.Main entry point that starts SimpleKafkaBroker on port 9092
- Implemented NIO-based broker with ServerSocketChannel, Selector, OP_ACCEPT
- Added ConnectionBuffer inner class for per-connection read buffering (ready for later stories)
- Package structure: com.simplekafka.broker, com.simplekafka.shared (placeholder), com.simplekafka.client (placeholder), com.simplekafka.metadata (placeholder)
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/Main.java` -- NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` -- NEW