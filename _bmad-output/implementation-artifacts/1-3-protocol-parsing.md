# Story 1.3: Protocol Parsing

Status: done

## Story

As a developer,
I want the broker to parse request header v2 correctly,
so requests are handled properly.

## Acceptance Criteria

1. **Given** a valid request with header v2 format
   **When** broker receives request
   **Then** broker correctly extracts api_key (INT16), api_version (INT16), correlation_id (INT32), client_id (COMPACT_STRING), and TAG_BUFFER

2. Request format follows Kafka wire protocol: message_size(4 bytes) + header + body

3. Invalid/corrupted header returns error response

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Layer 1)

- `RequestHeader.java` — Request header v2: api_key, api_version, correlation_id, client_id, TAG_BUFFER
- `Protocol.java` — ByteBuffer serialization for all message types

### Protocol Wire Format

```
Request:
  message_size: INT32 (4 bytes)
  header v2:
    api_key: INT16
    api_version: INT16
    correlation_id: INT32
    client_id: COMPACT_STRING (variable length)
    TAG_BUFFER: TAGGED_FIELDS (variable)

Response header v1:
  correlation_id: INT32
  TAG_BUFFER: TAGGED_FIELDS (variable)
```

### From NFRs

- **NFR4:** Binary wire format using ByteBuffer for all protocol encoding/decoding
- **NFR8:** Request header v2 format with TAG_BUFFER for forward compatibility

## Tasks / Subtasks

- [x] Task 1: Implement Protocol.java in shared module (AC: #1, #2)
  - [x] Subtask 1.1: Create ByteBuffer read methods for all primitive types
  - [x] Subtask 1.2: Create ByteBuffer write methods for all primitive types
  - [x] Subtask 1.3: Implement readMessage() and writeMessage() patterns
- [x] Task 2: Implement RequestHeader parsing (AC: #1, #3)
  - [x] Subtask 2.1: Parse all header fields from ByteBuffer
  - [x] Subtask 2.2: Validate header integrity
  - [x] Subtask 2.3: Handle malformed requests with error response

## Dev Notes

### ByteBuffer Pattern

```java
public static RequestHeader parse(ByteBuffer buffer) {
    short apiKey = buffer.getShort();
    short apiVersion = buffer.getShort();
    int correlationId = buffer.getInt();
    String clientId = CompactString.read(buffer);
    // TAG_BUFFER - read remaining tagged fields
    return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
}
```

### DRY/SOLID

- Protocol utilities in shared package, reused by broker and future client
- Single responsibility: RequestHeader parses header only, not entire request

## File List

- `src/main/java/com/simplekafka/shared/protocol/Protocol.java` — NEW
- `src/main/java/com/simplekafka/shared/protocol/RequestHeader.java` — NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/Uuid.java` — NEW (for future topic_id)

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Protocol.java provides framing utilities (frameResponse, writeEmptyTagBuffer)
- RequestHeader v2 parsing complete with TAG_BUFFER skipping
- All primitive types implemented: Int16, Int32, CompactString, CompactArray, Uuid
- Malformed request handling added to processRequests loop
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/shared/protocol/Protocol.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/RequestHeader.java` -- NEW
- `src/main/java/com/simplekafka/shared/protocol/primitives/Uuid.java` -- NEW