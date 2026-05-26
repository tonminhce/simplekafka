# Story 1.2: ApiVersions Response

Status: done

## Story

As a client,
I want to query what API versions the broker supports,
so I know which requests I can make.

## Acceptance Criteria

1. **Given** a connected client
   **When** client sends an ApiVersions request (API key 18)
   **Then** broker responds with message_size(4 bytes) + correlation_id + api_versions array containing keys 18, 75, 0 with their version ranges

2. ApiVersions response must include: API key 18 (versions 0-4), API key 75 (version 0), API key 0 (versions 0-11)

3. Response header format: correlation_id (INT32) + TAG_BUFFER

## Technical Requirements

### From IMPLEMENTATION_PLAN.md

- API key 18: ApiVersions (min: 0, max: 4)
- API key 75: DescribeTopicPartitions (min: 0, max: 0)
- API key 0: Produce (min: 0, max: 11)

### Protocol Format (Layer 1)

- Request header v2: api_key (INT16), api_version (INT16), correlation_id (INT32), client_id (COMPACT_STRING), TAG_BUFFER
- Response header v1: correlation_id (INT32), TAG_BUFFER
- Binary wire format using ByteBuffer for all protocol encoding/decoding

### From NFRs

- **NFR4:** Binary wire format using ByteBuffer for all protocol encoding/decoding
- **NFR8:** Request header v2 format with TAG_BUFFER for forward compatibility
- **NFR9:** Response header v1 format with TAG_BUFFER

## Tasks / Subtasks

- [x] Task 1: Create protocol primitives in shared module (AC: #1, #2, #3)
  - [x] Subtask 1.1: Create Int16, Int32 primitive parsers in shared package
  - [x] Subtask 1.2: Create CompactString parser
  - [x] Subtask 1.3: Create error codes class (ErrorCodes.java)
- [x] Task 2: Implement RequestHeader parsing (AC: #1)
  - [x] Subtask 2.1: Parse api_key, api_version, correlation_id from ByteBuffer
  - [x] Subtask 2.2: Parse client_id as COMPACT_STRING
  - [x] Subtask 2.3: Handle TAG_BUFFER
- [x] Task 3: Implement ApiVersions response (AC: #1, #2, #3)
  - [x] Subtask 3.1: Create ApiVersionsHandler
  - [x] Subtask 3.2: Build api_versions array with correct key/version ranges
  - [x] Subtask 3.3: Serialize response using ByteBuffer

## Dev Notes

### ApiVersions Response Format

```
ResponseHeader v1:
  correlation_id: INT32
  TAG_BUFFER: variable

Body:
  error_code: INT16
  api_versions_count: INT32
  api_versions[]:
    api_key: INT16
    min_version: INT16
    max_version: INT16
```

### DRY/SOLID

- Protocol serialization code goes in shared package (Protocol.java)
- ErrorCodes class in shared for reuse across broker/client

## File List

- `src/main/java/com/simplekafka/shared/protocol/ErrorCodes.java` — NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/Int16.java` — NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/Int32.java` — NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/CompactString.java` — NEW
- `src/main/java/com/simplekafka/shared/protocol/RequestHeader.java` — NEW
- `src/main/java/com/simplekafka/shared/protocol/ResponseHeader.java` — NEW
- `src/main/java/com/simplekafka/broker/handlers/ApiVersionsHandler.java` — NEW

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Created all protocol primitives: Int16, Int32, CompactString, CompactArray, Uuid
- Created ErrorCodes with standard Kafka error constants
- Implemented RequestHeader v2 parsing with TAG_BUFFER skipping
- Implemented ResponseHeader v1 writing
- Created Protocol utility for framing responses
- Created ApiVersionsHandler with support for keys 0, 1, 18, 75
- Updated SimpleKafkaBroker with full request read/dispatch loop
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/shared/protocol/ErrorCodes.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/Int16.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/Int32.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/CompactString.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/CompactArray.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/Uuid.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/RequestHeader.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/ResponseHeader.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/Protocol.java` -- NEW
- `src/main/java/com/simplekafka/broker/handlers/ApiVersionsHandler.java` -- NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` -- UPDATED (added read/dispatch loop)