# Story 3.2: Produce Invalid Topic Error

Status: done

## Story

As a client,
I want to receive an error when producing to a non-existent topic,
so I know my message wasn't stored.

## Acceptance Criteria

1. **Given** broker has no topic "nonexistent"
   **When** client sends Produce request for topic "nonexistent" partition 0
   **Then** broker returns error_code = 3 (UNKNOWN_TOPIC_OR_PARTITION)

## Technical Requirements

### From Layer 4

- `ProduceHandler.java` — Handle Produce requests
- Validate topic/partition exists via ClusterMetadataStore

### Error Codes

- 3 = UNKNOWN_TOPIC_OR_PARTITION

## Tasks / Subtasks

- [x] Task 1: Validate topic exists in ProduceHandler (AC: #1)
  - [x] Subtask 1.1: Parse ProduceRequest v11
  - [x] Subtask 1.2: Look up topic in ClusterMetadataStore
  - [x] Subtask 1.3: Return error 3 if topic doesn't exist

## Dev Notes

### Produce Request v11 Format

```
RequestHeader v2:
  api_key: 0
  api_version: 11
  correlation_id: INT32
  client_id: COMPACT_STRING
  TAG_BUFFER

Body:
  transactional_id: COMPACT_STRING (null)
  acks: INT16
  timeout_ms: INT32
  topics[]:
    topic: COMPACT_STRING
    partitions[]:
      partition_index: INT32
      record_batch: RecordBatch
```

## File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — NEW

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- ProduceHandler validates topic existence via ClusterMetadataStore
- Returns error_code=3 (UNKNOWN_TOPIC_OR_PARTITION) for unknown topics
- Parses ProduceRequest v11: transactional_id, acks, timeout, topics, partitions
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` -- NEW