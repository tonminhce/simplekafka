# Story 2.2: Existing Topic Metadata Response

Status: done

## Story

As a client,
I want to receive correct metadata for an existing topic,
so I know topic_id, partitions, and leader info.

## Acceptance Criteria

1. **Given** topic "my-topic" exists with topic_id (UUID) and 3 partitions in __cluster_metadata
   **When** client sends DescribeTopicPartitions for "my-topic"
   **Then** broker returns topic_id (16-byte UUID), partition_count = 3, each partition has partition_index, leader_id, leader_epoch, replicas, isr

2. Topic ID returned as 16-byte UUID (not topic name)

3. Partition metadata includes: partition_index, leader_id, leader_epoch, replica_nodes, isr_nodes

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Layer 2)

- `ClusterMetadataStore.java` — In-memory cache of cluster metadata
  - `getTopic(topicName)` — Get topic metadata
  - `getTopicById(uuid)` — Get topic by UUID
  - `getPartition(topicId, partitionIndex)` — Get partition metadata

### From Layer 4

- `DescribeTopicPartitionsHandler.java` — Return topic metadata

### Topics sorted alphabetically (Story 2.4)

## Tasks / Subtasks

- [x] Task 1: Implement ClusterMetadataStore (AC: #1, #2, #3)
  - [x] Subtask 1.1: Load metadata from __cluster_metadata log
  - [x] Subtask 1.2: Implement getTopic(topicName)
  - [x] Subtask 1.3: Implement getPartition(topicId, partitionIndex)
  - [x] Subtask 1.4: Cache topic_id as UUID
- [x] Task 2: Build DescribeTopicPartitions response (AC: #1, #2, #3)
  - [x] Subtask 2.1: Query ClusterMetadataStore for topic
  - [x] Subtask 2.2: Serialize topic_id as 16-byte UUID
  - [x] Subtask 2.3: Include all partition metadata

## Dev Notes

### DescribeTopicPartitions Response Format

```
Topic metadata:
  error_code: INT16
  topic_id: UUID (16 bytes)
  topic_name: COMPACT_STRING
  partitions_count: INT32
  partitions[]:
    partition_index: INT32
    leader_id: INT32
    leader_epoch: INT32
    replica_nodes: COMPACT_ARRAY[INT32]
    isr_nodes: COMPACT_ARRAY[INT32]
```

### DRY/SOLID

- UUID handling in shared package (Uuid.java from Story 1.3)
- ClusterMetadataStore single responsibility: metadata management

## File List

- `src/main/java/com/simplekafka/metadata/ClusterMetadataStore.java` — NEW
- `src/main/java/com/simplekafka/metadata/TopicRecord.java` — NEW
- `src/main/java/com/simplekafka/metadata/PartitionRecord.java` — NEW
- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- ClusterMetadataStore stores TopicRecord and PartitionRecord with lookup by name and UUID
- TopicRecord and PartitionRecord are immutable value objects
- DescribeTopicPartitionsHandler serializes topic_id as 16-byte UUID
- Partition metadata includes: partition_index, leader_id, leader_epoch, replicas, isr
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/metadata/ClusterMetadataStore.java` -- NEW
- `src/main/java/com/simplekafka/metadata/TopicRecord.java` -- NEW
- `src/main/java/com/simplekafka/metadata/PartitionRecord.java` -- NEW
- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` -- NEW