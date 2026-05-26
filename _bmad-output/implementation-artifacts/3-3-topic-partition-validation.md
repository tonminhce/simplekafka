# Story 3.3: Topic/Partition Validation

Status: done

## Story

As a developer,
I want the broker to validate topic/partition via ClusterMetadataStore before writing,
so invalid writes are rejected.

## Acceptance Criteria

1. **Given** ClusterMetadataStore has topic "test-topic" with partition 0 but not partition 5
   **When** Produce request arrives for "test-topic" partition 5
   **Then** broker rejects with error_code = 3

2. Validation happens before any disk write

## Technical Requirements

### From Story 2.2

- ClusterMetadataStore.getPartition(topicId, partitionIndex)

### DRY/SOLID

- Same validation logic used by both DescribeTopicPartitionsHandler and ProduceHandler

## Tasks / Subtasks

- [x] Task 1: Validate partition exists (AC: #1, #2)
  - [x] Subtask 1.1: Look up topic_id from topic name
  - [x] Subtask 1.2: Check if partition_index exists for that topic
  - [x] Subtask 1.3: Return error 3 if partition not found

## Dev Notes

### Validation Flow

```java
TopicMetadata topic = metadataStore.getTopic(topicName);
if (topic == null) {
    return error(UNKNOWN_TOPIC_OR_PARTITION);
}
PartitionMetadata partition = metadataStore.getPartition(topic.getTopicId(), partitionIndex);
if (partition == null) {
    return error(UNKNOWN_TOPIC_OR_PARTITION);
}
```

## File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- ProduceHandler validates both topic and partition existence
- Uses ClusterMetadataStore.getTopic() then getPartition()
- Returns error 3 if either topic or partition is not found
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` -- UPDATED