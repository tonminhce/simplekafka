---
baseline_commit: 24021afe23eafa635d724d91f44128c68dbe65c5
---

# Story 6.1: Segment Rolling (Size-Based)

Status: review

## Story

As a broker operator,
I want log segments to roll when they reach the size limit,
so that no single file grows unbounded and disk I/O remains predictable.

## Acceptance Criteria

1. **Given** topic "orders" partition 0 has an active segment
   **When** `appendRecordBatch()` would cause the segment to exceed `SEGMENT_SIZE_LIMIT`
   **Then** a new segment is created at the current `nextOffset` and the batch is written to the new segment

2. **Given** a partition with multiple segments
   **When** `readMessages()` requests an offset in an older segment
   **Then** the correct segment is located and data is returned

3. **Given** a partition with multiple segments
   **When** `findSegmentForOffset()` is called
   **Then** it uses binary search across segments (not linear scan)

4. Old segment is properly closed (FileChannel closed) before creating the new one

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 6, Step 25)

- Enforce `SEGMENT_SIZE_LIMIT` in `Partition.appendRecordBatch()`
- When active segment exceeds limit, call `createNewSegment(nextOffset)`
- Close old segment, open new log/index file pair

### From NFRs

- **NFR3:** Segment-based log storage with index files for offset-to-position lookup
- **DRY:** Reuse existing `LogSegment` class for new segments
- **SOLID:** `Partition` manages segment lifecycle, `LogSegment` handles individual file I/O

## Tasks / Subtasks

- [x] Task 1: Add size check in Partition.appendRecordBatch (AC: #1)
  - [x] Subtask 1.1: Before writing, check if `segment.size() + entrySize > SEGMENT_SIZE_LIMIT`
  - [x] Subtask 1.2: If exceeded, call `createNewSegment(nextOffset)` to roll
  - [x] Subtask 1.3: Write the batch to the new active segment instead
- [x] Task 2: Implement binary search in findSegmentForOffset (AC: #3)
  - [x] Subtask 2.1: Replace linear scan with binary search on segments list
  - [x] Subtask 2.2: Use `LogSegment.getBaseOffset()` as comparison key with overflow-safe mid calculation
- [x] Task 3: Close old segment on roll (AC: #4)
  - [x] Subtask 3.1: Call `oldSegment.close()` before creating new segment
  - [x] Subtask 3.2: Log segment roll event at INFO level
- [x] Task 4: Add test for segment rolling (AC: #1, #2)
  - [x] Subtask 4.1: Test with forced segment rolling via reflection
  - [x] Subtask 4.2: Verify reads span multiple segments

## Dev Notes

### Segment Rolling Flow

```
appendRecordBatch(bytes):
  1. Check activeSegment.size() + entrySize > SEGMENT_SIZE_LIMIT
  2. If yes:
     a. current.close()  ← close old segment
     b. createNewSegment(nextOffset)  ← open new segment
  3. Write to activeSegment()
```

### Edge Cases Reviewed

- **Int overflow**: `entrySize` uses `long` (8L + recordBatchBytes.length) to prevent int overflow when checking size
- **Binary search mid**: Uses `lo + (hi - lo) / 2` instead of `(lo + hi) / 2` to prevent overflow
- **Size check BEFORE write**: Prevents writing to a segment that would exceed the limit
- **Old segment close**: Called BEFORE `createNewSegment()` to avoid file descriptor leak
- **Invalid batch**: Existing validation for <49 bytes and negative records_count preserved

### Files

```
/tmp/kraft-combined-logs/
  orders-0/
    00000000000000000000.log  (closed, full)
    00000000000000000000.index
    00000000000000000042.log  (active)
    00000000000000000042.index
```

## File List

- `src/main/java/com/simplekafka/broker/Partition.java` — MODIFY (segment rolling, binary search)
- `src/test/java/com/simplekafka/SegmentRollingTest.java` — NEW (10 tests)

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Completion Notes

- Added segment size check in `appendRecordBatch()`: checks `current.size() + entrySize > SEGMENT_SIZE_LIMIT` BEFORE writing
- Uses `long entrySize = 8L + recordBatchBytes.length` to prevent int overflow
- Old segment closed before new one created (AC #4)
- Binary search in `findSegmentForOffset()` uses overflow-safe `lo + (hi - lo) / 2`
- Logs segment roll at INFO level with old size and new offset
- 10 new tests: single/multi batch append, segment rolling, multi-segment reads, binary search, edge cases
- Full regression suite: 53 tests, 0 failures

### File List

- `src/main/java/com/simplekafka/broker/Partition.java` -- UPDATED
- `src/test/java/com/simplekafka/SegmentRollingTest.java` -- NEW
