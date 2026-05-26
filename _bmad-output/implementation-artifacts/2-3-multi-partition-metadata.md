# Story 2.3: Multi-Partition Topic Metadata

Status: done

## Story

As a client,
I want metadata for all partitions of a topic,
so I know which broker is leader for each partition.

## Acceptance Criteria

1. **Given** topic has 5 partitions with different leaders
   **When** client requests metadata for this topic
   **Then** response contains all 5 partition entries with correct leader_epoch for each

2. Each partition entry includes unique partition_index

3. Leaders may differ across partitions (some partitions might have different leader_id)

## Technical Requirements

### From Story 2.2

- ClusterMetadataStore.getPartition(topicId, partitionIndex)

### Partition Record Fields

- partition_id, topic_id, topic_name, leader, replicas, isr

## Tasks / Subtasks

- [x] Task 1: Return all partition entries (AC: #1, #2, #3)
  - [x] Subtask 1.1: Iterate all partitions from ClusterMetadataStore
  - [x] Subtask 1.2: Include partition_index for each
  - [x] Subtask 1.3: Include leader_id per partition entry

## Dev Notes

- Same response format as Story 2.2, just multiple partitions
- ClusterMetadataStore must support iterating all partitions for a topic

## File List

- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- ClusterMetadataStore.getPartitions(topicId) returns all partitions sorted by partition index
- DescribeTopicPartitionsHandler writes all partition entries in the response
- Each partition entry includes partition_index, leader_id, leader_epoch, replicas, isr
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/metadata/ClusterMetadataStore.java` -- NEW
- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` -- UPDATED