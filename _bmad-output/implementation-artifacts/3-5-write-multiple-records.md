# Story 3.5: Write Multiple Records in Batch

Status: done

## Story

As a client,
I want to send multiple records in one Produce request,
so I can batch efficiently.

## Acceptance Criteria

1. **Given** topic "events" partition 0 exists
   **When** client sends Produce request with 10 records in one RecordBatch
   **Then** all 10 records are written to disk as a single RecordBatch with sequential offsets 0-9

2. Records within batch have sequential offsets starting from base_offset

3. RecordBatch header includes record count

## Technical Requirements

### RecordBatch Format

```
base_offset: INT64
batch_length: INT32
partition_leader_epoch: INT32
magic: INT8 (2 for v2)
crc: INT32
attributes: INT16
last_offset_delta: INT32
first_timestamp: INT64
max_timestamp: INT64
producer_id: INT64
producer_epoch: INT16
base_sequence: INT32
records_count: INT32
records[]:
  attributes: INT8
  timestamp_delta: VARINT
  offset_delta: VARINT
  key: COMPACT_STRING (null if no key)
  value: COMPACT_STRING
  headers[]: COMPACT_STRING pairs
```

## Tasks / Subtasks

- [x] Task 1: Parse multiple records from RecordBatch (AC: #1, #2, #3)
  - [x] Subtask 1.1: Read records_count from RecordBatch header
  - [x] Subtask 1.2: Iterate and parse each record
  - [x] Subtask 1.3: Calculate offset_delta for each record
- [x] Task 2: Write batch with sequential offsets (AC: #1, #2)
  - [x] Subtask 2.1: Assign base_offset for batch
  - [x] Subtask 2.2: Calculate offset_delta for each record (0, 1, 2, ...)
  - [x] Subtask 2.3: Write entire batch atomically

## Dev Notes

- Use base_sequence for idempotent producers
- CRC covers entire batch except CRC field itself

## File List

- `src/main/java/com/simplekafka/broker/Partition.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Partition.parseRecordsCount() extracts record count from RecordBatch header
- nextOffset advances by recordsCount after each batch
- Entire batch written atomically as raw bytes
- Offsets are sequential: base_offset, base_offset+1, ..., base_offset+N-1
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/Partition.java` -- UPDATED