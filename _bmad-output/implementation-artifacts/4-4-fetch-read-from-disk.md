# Story 4.4: Fetch Read from Disk

Status: done

## Story

As a client,
I want to read messages from disk at a specific offset,
so I can consume historical messages.

## Acceptance Criteria

1. **Given** topic "orders" partition 0 has messages at offsets 50-59 on disk
   **When** client sends Fetch request for "orders" partition 0 at offset 50 with max_bytes 1024
   **Then** broker reads RecordBatch from log file starting at offset 50 and returns records in response

2. Uses index for byte position lookup (offset → position)

3. Returns records in descending offset order (highest offset first per batch)

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Layer 3)

- `Partition.java` — `findPositionForOffset()` — Index-based byte position lookup
- `Partition.java` — `readMessages()` — Read from offset up to maxBytes using binary search
- `LogSegment.java` — Individual log segment (log + index file pair)

### On-Disk Format

```
<log-dir>/<topic-name>-<partition-index>/00000000000000000000.log
```

### NFR3

- Segment-based log storage with index files for offset-to-position lookup

## Tasks / Subtasks

- [x] Task 1: Implement offset-based disk read (AC: #1, #2, #3)
  - [x] Subtask 1.1: Find segment for offset using binary search
  - [x] Subtask 1.2: Use index to find byte position for offset
  - [x] Subtask 1.3: Read RecordBatch from log file
  - [x] Subtask 1.4: Serialize records to FetchResponse
- [x] Task 2: Handle partial reads (AC: #1, #2)
  - [x] Subtask 2.1: Respect max_bytes limit
  - [x] Subtask 2.2: Return only complete RecordBatches

## Dev Notes

### Index Format (Offset Index)

```
offset: INT64 (relative to segment base)
position: INT64 (byte position in log file)
```

### Read Flow

1. Binary search segments to find segment containing offset
2. Index lookup to get byte position
3. Read from position until max_bytes or end of segment
4. Parse RecordBatch and extract records

## File List

- `src/main/java/com/simplekafka/broker/Partition.java` — UPDATE
- `src/main/java/com/simplekafka/broker/LogSegment.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Partition.readMessages(offset, maxBytes) reads from disk using segment-based lookup
- Partition.findSegmentForOffset() searches segments by base offset
- Partition.findPositionForOffset() maps offset to byte position using index
- LogSegment.read(position, maxBytes) reads up to maxBytes from log file
- FetchHandler serializes records as COMPACT_BYTES in FetchResponse v16
- max_bytes limit respected in LogSegment.read()
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/Partition.java` -- UPDATED
- `src/main/java/com/simplekafka/broker/LogSegment.java` -- UPDATED
- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` -- UPDATED