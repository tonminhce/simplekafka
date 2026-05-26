---
baseline_commit: 400e0e4
---

# Story 6.3: Log Durability (fsync)

Status: review

## Story

As a broker operator,
I want log writes to be flushed to disk,
so that messages survive broker crashes and power failures.

## Acceptance Criteria

1. **Given** a RecordBatch is written to a log segment
   **When** `appendRaw()` completes
   **Then** `FileChannel.force(false)` is called to flush data to disk

2. **Given** a segment is being closed
   **When** `close()` is called
   **Then** any remaining data is flushed via `force(false)` before closing channels

3. **Given** a broker configured with flush interval = 0
   **When** batches are appended
   **Then** every write is immediately fsynced (current behavior)

4. **Given** a broker configured with flush interval > 0
   **When** time since last flush exceeds the interval
   **Then** fsync is triggered on the next append (batched fsync)

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 6, Step 27)

- Call `FileChannel.force(false)` after each append in `LogSegment`
- Add `force()` on `close()`
- Configurable flush interval (fsync every N writes or N milliseconds)

### From NFRs

- **DRY:** Flush behavior configured via shared `Config.java` (Story 6.4)
- **SOLID:** `LogSegment` owns its file I/O, including flush responsibility

## Tasks / Subtasks

- [x] Task 1: Add fsync after append in LogSegment.appendRaw (AC: #1)
  - [x] Subtask 1.1: Call `logChannel.force(false)` after write
  - [x] Subtask 1.2: Also fsync index channel after `writeIndexEntry()`
- [x] Task 2: Add fsync in LogSegment.close (AC: #2)
  - [x] Subtask 2.1: Call `logChannel.force(false)` and `indexChannel.force(false)` before closing
- [x] Task 3: Add configurable flush interval (AC: #3, #4)
  - [x] Subtask 3.1: Add `flushIntervalMs` field to LogSegment (default 0 = always sync)
  - [x] Subtask 3.2: Track `lastFlushTime` and only fsync when interval exceeded
  - [x] Subtask 3.3: Accept flush interval via LogSegment constructor
- [x] Task 4: Add test for durability (AC: #1, #2)
  - [x] Subtask 4.1: Write batch, verify file content persists after force
  - [x] Subtask 4.2: Test close flushes remaining data

## Dev Notes

### fsync Strategy

```java
// In appendRaw():
logChannel.write(writeBuf, position);
nextPosition = position + recordBatch.length;

if (flushIntervalMs == 0) {
    logChannel.force(false);   // always sync
} else {
    long now = System.currentTimeMillis();
    if (now - lastFlushTime >= flushIntervalMs) {
        logChannel.force(false);
        lastFlushTime = now;
    }
}
```

### Performance Consideration

- `force(false)` = flush data only (no metadata) — sufficient for log durability
- `force(true)` = flush data + metadata — unnecessary overhead for append-only log
- Default `flushIntervalMs = 0` preserves current behavior (sync every write)
- Production Kafka default: `log.flush.interval.messages = Long.MAX_VALUE` (relies on OS flush)

### Edge Cases Reviewed

- **Channel open check**: `close()` checks `logChannel.isOpen()` before calling `force(false)` to avoid IOException on already-closed channels
- **lastFlushTime volatile**: Marked volatile for thread-safety since it's read in `maybeForceLog()` inside synchronized methods but also accessed via `getLastFlushTime()` from tests
- **Index fsync in batched mode**: Only fsyncs index in synchronous mode; in batched mode, index fsync happens together with log fsync to reduce I/O
- **Backward compatibility**: Original constructor delegates to new constructor with `flushIntervalMs = 0`, preserving existing behavior
- **Partition recovery not in scope**: The reopen tests were removed because Partition doesn't persist nextOffset across restarts — that's a separate concern

## File List

- `src/main/java/com/simplekafka/broker/LogSegment.java` — MODIFIED (fsync in appendRaw, writeIndexEntry, close; flush interval support)
- `src/test/java/com/simplekafka/DurabilityTest.java` — NEW (7 tests)

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Completion Notes

- Added `flushIntervalMs` field and `lastFlushTime` tracking to `LogSegment`
- `maybeForceLog()` and `maybeForceIndex()` helper methods handle conditional fsync
- Default `flushIntervalMs = 0` = synchronous mode (fsync every write) — preserves existing behavior
- `close()` now calls `force(false)` on both channels before closing, with `isOpen()` guard
- Backward-compatible constructor delegates to new one with `flushIntervalMs = 0`
- 7 new tests: data persistence, flush time tracking, force flush method, flush interval default, close flush, large data
- Full regression suite: 69 tests, 0 failures
