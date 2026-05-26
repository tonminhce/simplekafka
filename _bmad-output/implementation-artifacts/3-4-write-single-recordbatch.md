# Story 3.4: Write Single RecordBatch to Disk

Status: done

## Story

As a client,
I want my message to be persisted to disk,
so messages survive broker restarts.

## Acceptance Criteria

1. **Given** topic "orders" partition 0 exists
   **When** client sends Produce request with 1 record to "orders" partition 0
   **Then** broker writes RecordBatch to /tmp/kraft-combined-logs/orders-0/00000000000000000000.log and assigns base_offset

2. RecordBatch format follows Kafka on-disk format

3. base_offset is sequential per batch

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Layer 3)

- `Partition.java` — `appendRecordBatch()` — Write RecordBatch directly to log file
- On-disk format: `<log-dir>/<topic-name>-<partition-index>/00000000000000000000.log`

### From NFRs

- **NFR7:** Partition logs stored at `<log-dir>/<topic-name>-<partition-index>/00000000000000000000.log`

## Tasks / Subtasks

- [x] Task 1: Implement Partition.appendRecordBatch (AC: #1, #2, #3)
  - [x] Subtask 1.1: Create partition log directory if not exists
  - [x] Subtask 1.2: Open/create log file with partition name format
  - [x] Subtask 1.3: Write RecordBatch to file
  - [x] Subtask 1.4: Assign sequential base_offset
- [x] Task 2: Integrate with ProduceHandler (AC: #1)
  - [x] Subtask 2.1: Call Partition.appendRecordBatch for valid produce
  - [x] Subtask 2.2: Return base_offset in response

## Dev Notes

### Log Directory Structure

```
/tmp/kraft-combined-logs/
  orders-0/
    00000000000000000000.log
    00000000000000000000.index
```

### DRY/SOLID

- LogSegment class handles individual log/index file pairs
- Partition manages segments and append logic

## File List

- `src/main/java/com/simplekafka/broker/Partition.java` — NEW
- `src/main/java/com/simplekafka/broker/LogSegment.java` — NEW
- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Created Partition class with segment-based storage
- Created LogSegment class for individual log/index file pairs
- On-disk format: /tmp/kraft-combined-logs/<topic>-<partition>/00000000000000000000.log
- Partition.appendRecordBatch() writes base_offset(INT64) + RecordBatch bytes
- AtomicLong for thread-safe offset tracking
- ProduceHandler returns base_offset in Produce response
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/Partition.java` -- NEW
- `src/main/java/com/simplekafka/broker/LogSegment.java` -- NEW
- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` -- UPDATED