# Story 4.1: Fetch Empty Response for No Data

Status: done

## Story

As a client,
I want to receive empty response when topic has no messages,
so I know there's nothing to consume.

## Acceptance Criteria

1. **Given** topic "empty-topic" exists but has no messages
   **When** client sends Fetch request for "empty-topic" partition 0 at offset 0
   **Then** response has empty records array (throttle_time_ms may be 0)

2. Fetch response includes throttle_time_ms

3. Topic_id, partition_index echoed back in response

## Technical Requirements

### From IMPLEMENTATION_PLAN.md

- `FetchHandler.java` — Handle Fetch requests

### Fetch Response v16

```
ResponseHeader v1:
  correlation_id: INT32
  TAG_BUFFER

Body:
  throttle_time_ms: INT32
  responses[]:
    topic_id: UUID
    partitions[]:
      partition_index: INT32
      error_code: INT16
      high_watermark: INT64
      records[]: (empty for this story)
```

## Tasks / Subtasks

- [x] Task 1: Return empty records for topic with no data (AC: #1, #2, #3)
  - [x] Subtask 1.1: Parse FetchRequest v16 (topic_id, partition_index, current_leader_epoch)
  - [x] Subtask 1.2: Check if topic exists, return error 100 if not
  - [x] Subtask 1.3: Return empty records array with throttle_time_ms

## Dev Notes

### Fetch Request v16 Format

```
topic_id: UUID (16 bytes)
partition_index: INT32
current_leader_epoch: INT32
```

## File List

- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` — NEW

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Created FetchHandler for API key 1 (Fetch v16)
- Returns throttle_time_ms=0, empty records array, correct topic_id and partition_index
- FetchRequest v16 parsed: cluster_id, session_id, topic_id, partition_index, fetch_offset, max_bytes
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` -- NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` -- UPDATED (added FetchHandler routing)