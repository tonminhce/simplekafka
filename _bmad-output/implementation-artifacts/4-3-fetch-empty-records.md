# Story 4.3: Fetch Empty Records for Topic with No Messages

Status: done

## Story

As a client,
I want to receive empty records array when topic exists but has no messages at requested offset,
so I can handle empty results.

## Acceptance Criteria

1. **Given** topic "events" exists but offset 100 is past all stored messages
   **When** client sends Fetch for "events" partition 0 at offset 100
   **Then** response returns empty records array

2. No error returned — this is a valid state

3. high_watermark may be 0 or last available offset

## Technical Requirements

### From Story 4.1

- Fetch response format

### From IMPLEMENTATION_PLAN.md

- `Partition.java` — `readMessages()` — Read from offset up to maxBytes

## Tasks / Subtasks

- [x] Task 1: Check offset against available data (AC: #1, #2, #3)
  - [x] Subtask 1.1: Use binary search to find segment for offset
  - [x] Subtask 1.2: If offset > high_watermark, return empty records
  - [x] Subtask 1.3: Set high_watermark in response

## Dev Notes

### Binary Search for Offset

```java
// Find segment containing requested offset
int pos = binarySearch(segments, offset);
// Read from that position
```

## File List

- `src/main/java/com/simplekafka/broker/Partition.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Partition.readMessages() returns empty ByteBuffer when offset is past all data
- FetchHandler returns empty records with high_watermark from partition.getNextOffset()
- No error returned for valid topic with offset past data
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/Partition.java` -- UPDATED
- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` -- UPDATED