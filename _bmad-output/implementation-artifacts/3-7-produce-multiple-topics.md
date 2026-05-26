# Story 3.7: Produce Multiple Topics

Status: done

## Story

As a client,
I want to produce to multiple topics in one request,
so I can minimize round trips.

## Acceptance Criteria

1. **Given** topics "topic-a" and "topic-b" both exist
   **When** client sends Produce request with records for both topics
   **Then** broker writes to respective partition logs and returns responses for both topics

2. Each topic's partitions handled independently

3. Response includes results for all topics

## Technical Requirements

### From Story 3.6

- Multiple partitions handling for same topic

### Produce Request Structure

```
topics[]:
  topic: COMPACT_STRING
  partitions[]:
    partition_index: INT32
    record_batch: RecordBatch
```

## Tasks / Subtasks

- [x] Task 1: Iterate all topics in Produce request (AC: #1, #2, #3)
  - [x] Subtask 1.1: Iterate outer topics array
  - [x] Subtask 1.2: For each topic, iterate partition entries
  - [x] Subtask 1.3: Write to appropriate partition log
- [x] Task 2: Build topic responses (AC: #1, #2, #3)
  - [x] Subtask 2.1: Include topic_name in response
  - [x] Subtask 2.2: Include partition results for each topic

## Dev Notes

- Topics processed in order received from client
- Each topic validated independently

## File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- ProduceHandler processes the outer topics array, then inner partitions array
- Each topic validated independently
- Response includes topic_name and all partition results per topic
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` -- UPDATED