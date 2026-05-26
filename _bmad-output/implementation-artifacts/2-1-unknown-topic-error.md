# Story 2.1: Unknown Topic Error Response

Status: done

## Story

As a client,
I want to receive an error when querying an unknown topic,
so I know the topic doesn't exist.

## Acceptance Criteria

1. **Given** broker has no topic "foo"
   **When** client sends DescribeTopicPartitions request for topic "foo"
   **Then** broker returns error_code = 3 (UNKNOWN_TOPIC_OR_PARTITION), topic_id = zeros, partitions = empty

2. Response header v1 with TAG_BUFFER included

3. Topic ID = 16 zero bytes for unknown topic

## Technical Requirements

### From IMPLEMENTATION_PLAN.md

- Error code 3 = UNKNOWN_TOPIC_OR_PARTITION
- DescribeTopicPartitions (API key 75, version 0)

### From Layer 4 (Request Handling)

- `DescribeTopicPartitionsHandler.java` — Handle unknown topics (error 3)

### From NFRs

- **NFR9:** Response header v1 format with TAG_BUFFER

## Tasks / Subtasks

- [x] Task 1: Create DescribeTopicPartitionsHandler (AC: #1, #2, #3)
  - [x] Subtask 1.1: Parse DescribeTopicPartitions request
  - [x] Subtask 1.2: Look up topic in ClusterMetadataStore
  - [x] Subtask 1.3: Return error 3 for unknown topics
  - [x] Subtask 1.4: Build response with empty topic_id and partitions array

## Dev Notes

### Error Response Format

```
ResponseHeader v1:
  correlation_id: INT32
  TAG_BUFFER

Body:
  throttle_time_ms: INT32
  topics_count: INT32
  topics[]:
    error_code: INT16 (3 = UNKNOWN_TOPIC_OR_PARTITION)
    topic_id: UUID (16 bytes, zeros for unknown)
    topic_name: COMPACT_STRING
    partitions_count: INT32 (0 for unknown topic)
    partitions[]: (empty)
```

## File List

- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` — NEW

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Created DescribeTopicPartitionsHandler with full request parsing and response building
- Returns error_code=3, topic_id=zeros, empty partitions for unknown topics
- ClusterMetadataStore created for topic/partition lookups
- Broker dispatches API key 75 to DescribeTopicPartitionsHandler
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` -- NEW
- `src/main/java/com/simplekafka/metadata/ClusterMetadataStore.java` -- NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` -- UPDATED