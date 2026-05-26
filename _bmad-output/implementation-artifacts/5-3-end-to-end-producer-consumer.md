# Story 5.3: End-to-End Producer/Consumer

Status: done

## Story

As a user,
I want to send a message and then consume it back,
so the system works end-to-end.

## Acceptance Criteria

1. **Given** topic "test-topic" exists with 1 partition
   **When** producer sends message "hello" to "test-topic" and consumer polls for messages
   **Then** consumer receives message "hello" with correct offset

2. Producer gets base_offset in response

3. Consumer can seek to offset and receive message

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Layer 6)

- `SimpleKafkaProducer.java` — `send()` — Send to random partition
- `SimpleKafkaConsumer.java` — `poll()` — Poll broker for messages
- `SimpleKafkaConsumer.java` — `seek()` — Seek to offset

### Client Library

- `SimpleKafkaClient.java` — NIO SocketChannel client
- `refreshMetadata()` — Connect to broker, fetch cluster metadata

## Tasks / Subtasks

- [x] Task 1: Implement SimpleKafkaClient (AC: #1, #2, #3)
  - [x] Subtask 1.1: Connect to broker via SocketChannel
  - [x] Subtask 1.2: Send Produce requests and parse responses
  - [x] Subtask 1.3: Send Fetch requests and parse records
- [x] Task 2: Implement SimpleKafkaProducer (AC: #1)
  - [x] Subtask 2.1: Implement send() to topic/partition
  - [x] Subtask 2.2: Return base_offset to caller
- [x] Task 3: Implement SimpleKafkaConsumer (AC: #1)
  - [x] Subtask 3.1: Implement poll() to fetch messages
  - [x] Subtask 3.2: Implement seek() to specific offset
- [x] Task 4: End-to-end test (AC: #1, #2, #3)
  - [x] Subtask 4.1: Create test-topic with 1 partition
  - [x] Subtask 4.2: Produce "hello" message
  - [x] Subtask 4.3: Consumer poll receives "hello"

## Dev Notes

### DRY/SOLID

- Shared protocol code used by both broker handlers and client
- SimpleKafkaClient contains common client logic used by producer/consumer

## File List

- `src/main/java/com/simplekafka/client/SimpleKafkaClient.java` — NEW
- `src/main/java/com/simplekafka/client/SimpleKafkaProducer.java` — NEW
- `src/main/java/com/simplekafka/client/SimpleKafkaConsumer.java` — NEW

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- SimpleKafkaClient: NIO SocketChannel client with send/receive, auto correlation ID
- SimpleKafkaProducer: sends messages with Produce API v11, builds RecordBatch, returns base_offset
- SimpleKafkaConsumer: fetches messages with Fetch API v16, supports seek() to offset
- Producer builds proper RecordBatch with Kafka v2 format (magic=2)
- Consumer parses FetchResponse v16 and extracts record batches
- Shared protocol primitives reused by both client and broker
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/client/SimpleKafkaClient.java` -- NEW
- `src/main/java/com/simplekafka/client/SimpleKafkaProducer.java` -- NEW
- `src/main/java/com/simplekafka/client/SimpleKafkaConsumer.java` -- NEW