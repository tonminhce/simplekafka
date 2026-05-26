# Story 1.5: Sequential Requests on Same Connection

Status: done

## Story

As a client,
I want to send multiple requests on the same TCP connection,
so I can pipeline requests efficiently.

## Acceptance Criteria

1. **Given** a client is connected
   **When** client sends 3+ requests sequentially without closing connection
   **Then** broker processes each request correctly and returns responses in order

2. Broker reads message_size to determine request length

3. Multiple requests can be queued before broker processes them

## Technical Requirements

### Protocol Format

```
message_size (4 bytes) + header + body
```

### From NFRs

- **NFR2:** Use NIO SocketChannel for non-blocking I/O

## Tasks / Subtasks

- [x] Task 1: Parse request length from message_size (AC: #1, #2)
  - [x] Subtask 1.1: Read first 4 bytes as message_size (INT32)
  - [x] Subtask 1.2: Read exactly messageSize bytes for the request
  - [x] Subtask 1.3: Handle partial reads (read until full message)
- [x] Task 2: Process multiple requests sequentially (AC: #1, #2, #3)
  - [x] Subtask 2.1: Loop through all pending requests on connection
  - [x] Subtask 2.2: Route each request to appropriate handler
  - [x] Subtask 2.3: Write response before processing next request

## Dev Notes

### Message Framing

```java
// Read message size first
ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
readFully(channel, sizeBuffer);
int messageSize = sizeBuffer.getInt();

// Read full message
ByteBuffer message = ByteBuffer.allocate(messageSize);
readFully(channel, message);
message.flip();
```

### Keep Connection Alive

- Don't close channel after response
- Continue selecting for OP_READ on same channel

## File List

- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — UPDATE (add sequential request handling)

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- processRequests() loop handles multiple sequential requests on same connection
- Message framing: reads message_size(INT32), then reads exactly that many bytes
- Partial reads handled: buffer.compact() preserves partial data between select() calls
- Connection kept alive; does not close after response
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` -- UPDATED