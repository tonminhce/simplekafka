# Contributing to SimpleKafka

## Coding Standards

### Defensive Coding Patterns

SimpleKafka prioritizes correctness and robustness. The following patterns are required:

#### 1. Overflow-Safe Binary Search

Always use overflow-safe midpoint calculation:

```java
// WRONG: can overflow when lo and hi are large
int mid = (lo + hi) / 2;

// CORRECT: overflow-safe
int mid = lo + (hi - lo) / 2;
```

This pattern appears in:
- `Partition.findSegmentForOffset()` — binary search across segments
- `LogSegment.lookupPosition()` — binary search across index entries

#### 2. Channel Force Guards

Always check `isOpen()` before calling `force()`:

```java
// WRONG: may throw IOException if channel already closed
logChannel.force(false);

// CORRECT: guard against closed channel
if (logChannel != null && logChannel.isOpen()) {
    logChannel.force(false);
}
```

This pattern appears in:
- `LogSegment.forceFlush()`
- `LogSegment.close()`

#### 3. Size Arithmetic with `long`

When calculating sizes that may exceed `Integer.MAX_VALUE`, use `long`:

```java
// WRONG: can overflow for large batches
long entrySize = 8 + recordBatchBytes.length;

// CORRECT: use long for size calculations
long entrySize = 8L + recordBatchBytes.length;
```

This pattern appears in `Partition.appendRecordBatch()`.

#### 4. Negative Offset Defense

In offset lookup, guard against negative relative offsets:

```java
private int findPositionForOffset(LogSegment segment, long offset) throws IOException {
    // Guard: relative offset cannot be negative
    if (offset < segment.getBaseOffset()) {
        return 0; // start of segment
    }
    int relativeOffset = (int) (offset - segment.getBaseOffset());
    // ...
}
```

This pattern appears in `Partition.findPositionForOffset()`.

---

## Test Utilities

### RecordBatch Test Helpers (`KafkaWireHelpers`)

Use the test helpers in `KafkaWireHelpers` instead of duplicating batch-building logic:

```java
import com.simplekafka.KafkaWireHelpers;

// Build a 1-record batch with 10-byte value
byte[] batch = KafkaWireHelpers.buildTestRecordBatch(1, 10);

// Build a batch with multiple records
byte[] batch = KafkaWireHelpers.buildTestRecordBatch(5, 0); // 5 records, 0-byte values

// Build a batch with a specific string value
byte[] batch = KafkaWireHelpers.buildTestRecordBatch("hello world");
```

**Why use these helpers?**
- Ensures test batches match the exact format `Partition.appendRecordBatch()` writes
- Prevents BufferOverflow issues when broker parses test data
- Single source of truth — changes propagate to all test classes

### Available Helpers

| Method | Description |
|--------|-------------|
| `buildTestRecordBatch(recordsCount, valueSize)` | Multi-record batch with fixed-value padding |
| `buildTestRecordBatch(recordsCount)` | Single-record batch with 0-byte value |
| `buildTestRecordBatch(String value)` | Single-record batch with string value |
| `buildTestRecordBatch(recordsCount, valueSize, byte[] valueBytes)` | Full control over value bytes |

---

## Architecture

### Key Components

| Package | Class | Responsibility |
|---------|-------|----------------|
| `broker` | `SimpleKafkaBroker` | TCP server, virtual threads, request dispatch |
| `broker` | `Partition` | Partition log management, segment lifecycle |
| `broker` | `LogSegment` | Single segment: `.log` + `.index` file I/O |
| `handlers` | `ProduceHandler` | Write RecordBatch to partition |
| `handlers` | `FetchHandler` | Read from specific offset |
| `metadata` | `ClusterMetadataStore` | In-memory topic/partition registry |
| `metadata` | `ClusterMetadataLog` | `__cluster_metadata` persistence |
| `shared` | `Config` | Configuration (brokerId, port, logDir, flushIntervalMs) |

### On-Disk Layout

```
<log-dir>/
  __cluster_metadata-0/
    00000000000000000000.log
    00000000000000000000.index
  <topic-name>-<partition-index>/
    00000000000000000000.log   (segment base offset 0)
    00000000000000000000.index
    00000000000000001000.log   (segment base offset 1000)
    00000000000000001000.index
```

### RecordBatch Format

Each batch entry in `.log` files:

```
base_offset (INT64) + RecordBatch:
  batch_length (INT32)
  partition_leader_epoch (INT32)
  magic (INT8)
  crc (INT32)
  attributes (INT16)
  last_offset_delta (INT32)
  first_timestamp (INT64)
  max_timestamp (INT64)
  producer_id (INT64)
  producer_epoch (INT16)
  base_sequence (INT32)
  records_count (INT32)
  records[] (varint-encoded key/value pairs)
```

### Index Entry Format

Each entry in `.index` files (8 bytes):

```
relative_offset (INT32) + byte_position (INT32)
```

---

## Running Tests

```bash
mvn test
```

All 76 tests must pass before committing. Run specific test classes:

```bash
mvn test -Dtest=SegmentRollingTest
mvn test -Dtest=IndexLookupTest
mvn test -Dtest=DurabilityTest
mvn test -Dtest=ConfigTest
```

---

## Git Workflow

1. **Before editing any symbol**, run `gitnexus_impact` to assess blast radius
2. **Before committing**, run `gitnexus_detect_changes` to verify scope
3. **After committing**, re-index: `npx gitnexus analyze`

See `.claude/skills/gitnexus/` for GitNexus CLI documentation.