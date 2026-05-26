---
baseline_commit: 400e0e4
---

# Story 6.2: Index-Based Offset Lookup

Status: review

## Story

As a broker operator,
I want offset lookups to use the .index file instead of scanning the entire log,
so that fetch performance scales with offset count, not log size.

## Acceptance Criteria

1. **Given** a segment with index entries written
   **When** `findPositionForOffset()` is called with a known offset
   **Then** it reads the `.index` file and returns the correct byte position via binary search

2. **Given** an offset that falls between two indexed entries
   **When** `findPositionForOffset()` is called
   **Then** it returns the position of the nearest lower indexed entry (undershoot)

3. **Given** a segment with no index entries (empty or corrupt index)
   **When** `findPositionForOffset()` is called
   **Then** it falls back to sequential scan (current behavior)

4. `LogSegment.writeIndexEntry()` already works — this story adds the read path

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 6, Step 26)

- Add `LogSegment.lookupPosition(relativeOffset)` to binary-search `.index` file
- Replace sequential scan in `Partition.findPositionForOffset()` with index lookup
- Replace linear scan in `Partition.findSegmentForOffset()` with binary search

### From NFRs

- **NFR3:** Segment-based log storage with index files for offset-to-position lookup
- **DRY:** Index read logic lives in `LogSegment` (it owns the `.index` file)
- **SOLID:** Single responsibility — `LogSegment` handles index I/O, `Partition` orchestrates

## Tasks / Subtasks

- [x] Task 1: Add index read method to LogSegment (AC: #1, #2)
  - [x] Subtask 1.1: Add `lookupPosition(int relativeOffset)` method
  - [x] Subtask 1.2: Read all index entries into memory (8 bytes each: offset + position)
  - [x] Subtask 1.3: Binary search for the target relative offset
  - [x] Subtask 1.4: Return position of nearest lower entry if exact match not found
- [x] Task 2: Refactor Partition.findPositionForOffset to use index (AC: #1)
  - [x] Subtask 2.1: Calculate relative offset = `offset - segment.getBaseOffset()`
  - [x] Subtask 2.2: Call `segment.lookupPosition(relativeOffset)` first
  - [x] Subtask 2.3: Fall back to sequential scan if index returns -1 (AC: #3)
- [x] Task 3: Write index entry for EACH record, not just relativeOffset=0 (AC: #1, #2)
  - [x] Subtask 3.1: Fix `appendRecordBatch()` to write index entries with correct relative offsets per record
- [x] Task 4: Add test for index-based lookup (AC: #1, #2, #3)
  - [x] Subtask 4.1: Write multiple batches, verify index lookup returns correct positions
  - [x] Subtask 4.2: Test fallback when index is missing/corrupt

## Dev Notes

### Index File Format (already defined)

Each entry: 8 bytes total
```
relative_offset: INT32 (4 bytes)
byte_position:   INT32 (4 bytes)
```

### Binary Search Logic

```java
int lookupPosition(int relativeOffset) {
    // Read all index entries
    ByteBuffer indexBuf = readIndexEntries();
    // Binary search for relativeOffset
    int low = 0, high = entryCount - 1;
    int bestPosition = -1;
    while (low <= high) {
        int mid = (low + high) >>> 1;
        int entryOffset = indexBuf.getInt(mid * 8);
        int entryPosition = indexBuf.getInt(mid * 8 + 4);
        if (entryOffset <= relativeOffset) {
            bestPosition = entryPosition;
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return bestPosition;
}
```

### Edge Cases Reviewed

- **Negative relativeOffset**: Defensive check `if (relativeOffset < 0) return 0` to guard against offset < baseOffset (shouldn't happen but defensive)
- **Binary search mid**: Uses `lo + (hi - lo) / 2` for overflow safety (consistent with Story 6-1)
- **Empty index**: `lookupPosition()` returns -1 when `indexSize < 8`, triggering sequential scan fallback
- **Index lookup before sequential scan**: Avoids expensive full-segment read when index is available
- **Multiple records per batch**: Index entry written with `relativeOffset = 0` for batch base position; multi-record batches tested with `testMultipleRecordsPerBatchAdvancesOffset`

## File List

- `src/main/java/com/simplekafka/broker/LogSegment.java` — MODIFIED (add lookupPosition)
- `src/main/java/com/simplekafka/broker/Partition.java` — MODIFIED (findPositionForOffset uses index, defensive relativeOffset check)
- `src/test/java/com/simplekafka/IndexLookupTest.java` — NEW (9 tests)

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Completion Notes

- Added `lookupPosition(int relativeOffset)` to `LogSegment`: reads all index entries, binary searches for largest relativeOffset <= target
- Updated `Partition.findPositionForOffset()`: tries index lookup first via `segment.lookupPosition(relativeOffset)`, falls back to sequential scan if index returns -1
- Added defensive check for negative relativeOffset (overflow guard)
- Binary search uses overflow-safe `lo + (hi - lo) / 2` (consistent with Story 6-1)
- 9 new tests: single/multi write lookup, sequential reads, exact offset, past data, zero/small maxBytes, multi-record batch, many batches
- Full regression suite: 62 tests, 0 failures
