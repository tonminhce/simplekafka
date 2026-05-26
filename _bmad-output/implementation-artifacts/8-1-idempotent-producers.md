# Story 8.1: Idempotent Producers

Status: backlog

## Story

As a producer,
I want the broker to detect and discard duplicate messages,
so that exactly-once semantics are maintained even on retries.

## Acceptance Criteria

1. **Given** a producer with `producer_id=100` and `base_sequence=5`
   **When** it sends a RecordBatch with `producer_id=100, base_sequence=5`
   **Then** the batch is written to the log

2. **Given** a producer retries the same batch (`producer_id=100, base_sequence=5`)
   **When** the broker receives the duplicate
   **Then** the broker discards it and returns the original `base_offset` (no double-write)

3. **Given** a producer sends `base_sequence=5` followed by `base_sequence=6`
   **When** both are received in order
   **Then** both are written sequentially

4. **Given** a producer sends `base_sequence=5` then `base_sequence=7` (gap)
   **When** the broker detects the gap
   **Then** batch 7 is rejected with `INVALID_PRODUCER_EPOCH` or `OUT_OF_ORDER_SEQUENCE_NUMBER`

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 8, Step 34)

- Sequence number tracking per producer ID
- Deduplication on broker side

### From NFRs

- **DRY:** Deduplication state stored in a shared `ProducerSequenceCache`
- **SOLID:** `ProducerSequenceCache` is single responsibility — track sequences per producer

## Tasks / Subtasks

- [ ] Task 1: Create ProducerSequenceCache (AC: #1, #2, #3, #4)
  - [ ] Subtask 1.1: Create `shared/ProducerSequenceCache.java`
  - [ ] Subtask 1.2: Map: `producer_id → (lastSequence, lastOffset)`
  - [ ] Subtask 1.3: `checkAndTrack(producerId, sequence, offset)` → returns true if new, false if duplicate
  - [ ] Subtask 1.4: Thread-safe (ConcurrentHashMap)
- [ ] Task 2: Update ProduceHandler to check sequences (AC: #1, #2, #4)
  - [ ] Subtask 2.1: Extract `producer_id` and `base_sequence` from RecordBatch header
  - [ ] Subtask 2.2: If `producer_id > 0` → check `ProducerSequenceCache`
  - [ ] Subtask 2.3: If duplicate → skip write, return cached offset
  - [ ] Subtask 2.4: If gap → return `OUT_OF_ORDER_SEQUENCE_NUMBER` error
  - [ ] Subtask 2.5: If `producer_id == 0` → skip deduplication (non-idempotent producer)
- [ ] Task 3: Add error code for out-of-order sequence
  - [ ] Subtask 3.1: Add `OUT_OF_ORDER_SEQUENCE_NUMBER = 45` to `ErrorCodes.java`
- [ ] Task 4: Add test for idempotent producers (AC: #1, #2, #3, #4)
  - [ ] Subtask 4.1: Produce batch → verify written
  - [ ] Subtask 4.2: Produce same batch again → verify duplicate discarded
  - [ ] Subtask 4.3: Produce out-of-order → verify rejected

## Dev Notes

### RecordBatch Fields (already parsed in ProduceHandler)

```
Offset  Field
0       base_offset (INT64) — assigned by broker
8       batch_length (INT32)
12      partition_leader_epoch (INT32)
16      magic (INT8) — always 2
17      crc (INT32)
21      attributes (INT16)
23      last_offset_delta (INT32)
27      first_timestamp (INT64)
35      max_timestamp (INT64)
43      producer_id (INT64)  ← we need this
51      producer_epoch (INT16)
53      base_sequence (INT32) ← we need this
57      records_count (INT32)
```

### Deduplication Logic

```java
public synchronized boolean checkAndTrack(long producerId, int sequence, long offset) {
    long[] last = cache.get(producerId);
    if (last == null) {
        cache.put(producerId, new long[]{sequence, offset});
        return true; // new producer
    }
    int lastSeq = (int) last[0];
    if (sequence == lastSeq) {
        return false; // duplicate — skip
    }
    if (sequence < lastSeq) {
        return false; // stale — skip
    }
    if (sequence > lastSeq + 1) {
        throw new OutOfOrderSequenceException(); // gap
    }
    cache.put(producerId, new long[]{sequence, offset});
    return true; // valid next sequence
}
```

## File List

- `src/main/java/com/simplekafka/shared/ProducerSequenceCache.java` — NEW
- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — MODIFY (sequence check)
- `src/main/java/com/simplekafka/shared/ErrorCodes.java` — MODIFY (add OUT_OF_ORDER_SEQUENCE_NUMBER)
- `src/test/java/com/simplekafka/IdempotentProducerTest.java` — NEW
