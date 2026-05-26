# Story 3.6: Produce Multiple Partitions

Status: done

## Story

As a client,
I want to produce to multiple partitions of the same topic in one request,
so I can distribute data.

## Acceptance Criteria

1. **Given** topic "data" has 3 partitions
   **When** client sends Produce request with records for partition 0 and partition 2
   **Then** broker writes to both partition log files and returns separate base_offsets per partition

2. Each partition gets its own base_offset sequence

3. Response includes results for all partitions attempted

## Technical Requirements

### From Story 3.4

- Partition.appendRecordBatch for single partition

### Produce Request v11 Structure

```
topics[]:
  topic: COMPACT_STRING
  partitions[]:
    partition_index: INT32
    record_batch: RecordBatch
```

## Tasks / Subtasks

- [x] Task 1: Handle multiple partition entries in Produce request (AC: #1, #2, #3)
  - [x] Subtask 1.1: Iterate all partition entries in request
  - [x] Subtask 1.2: Write to each partition's log file
  - [x] Subtask 1.3: Collect base_offsets for response
- [x] Task 2: Build partition responses (AC: #1, #2, #3)
  - [x] Subtask 2.1: Include partition_index in response
  - [x] Subtask 2.2: Include base_offset for each partition

## Dev Notes

- Each partition has independent offset sequence
- Errors on one partition don't affect other partitions

## File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- ProduceHandler iterates all partitions within each topic
- Each partition has its own Partition object and offset sequence
- Response includes separate results per partition with partition_index and base_offset
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` -- UPDATED