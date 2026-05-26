# Story 5.1: Persist Topic/Partition Creation Records

Status: done

## Story

As a system,
I want topic and partition creation to be recorded in __cluster_metadata log,
so metadata survives broker restarts.

## Acceptance Criteria

1. **Given** admin creates topic "orders" with 3 partitions
   **When** creation completes
   **Then** __cluster_metadata-0 log contains TOPIC_RECORD (topic_name, topic_id) and 3 PARTITION_RECORDS

2. Log stored at /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log

3. Records include: partition_id, topic_id, topic_name, leader, replicas, isr

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Layer 2)

- `ClusterMetadataLog.java` — Read/write __cluster_metadata log file
- `TopicRecord.java` — TOPIC_RECORD payload parsing
- `PartitionRecord.java` — PARTITION_RECORD payload parsing

### Record Types

- TOPIC_RECORD: topic_name, topic_id (UUID)
- PARTITION_RECORD: partition_id, topic_id, leader, replicas, isr

## Tasks / Subtasks

- [x] Task 1: Implement ClusterMetadataLog (AC: #1, #2, #3)
  - [x] Subtask 1.1: Create/open __cluster_metadata partition log
  - [x] Subtask 1.2: Implement writeRecord() for TOPIC_RECORD and PARTITION_RECORD
  - [x] Subtask 1.3: Implement readRecords() to replay log on startup
- [x] Task 2: Integrate with topic creation (AC: #1)
  - [x] Subtask 2.1: Write TOPIC_RECORD when topic created
  - [x] Subtask 2.2: Write PARTITION_RECORD for each partition

## Dev Notes

### __cluster_metadata Log Location

```
/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
```

### DRY/SOLID

- Record parsing in shared package (TopicRecord, PartitionRecord)
- ClusterMetadataLog handles log I/O, records are plain data objects

## File List

- `src/main/java/com/simplekafka/metadata/ClusterMetadataLog.java` — NEW
- `src/main/java/com/simplekafka/metadata/TopicRecord.java` — NEW
- `src/main/java/com/simplekafka/metadata/PartitionRecord.java` — NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Created ClusterMetadataLog for reading/writing __cluster_metadata log
- Log stored at /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
- writeTopicRecord() and writePartitionRecord() persist to disk
- readRecords() replays log on startup, returns TopicRecord and PartitionRecord objects
- loadInto() populates ClusterMetadataStore from persisted records
- SimpleKafkaBroker.createTopic() writes both TOPIC_RECORD and PARTITION_RECORD
- Broker calls loadMetadata() on startup to restore state
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/metadata/ClusterMetadataLog.java` -- NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` -- UPDATED (added createTopic, loadMetadata)