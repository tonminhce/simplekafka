# Story 4.2: Fetch Unknown Topic Error

Status: done

## Story

As a client,
I want to receive error for unknown topic in Fetch,
so I know the topic doesn't exist.

## Acceptance Criteria

1. **Given** topic "unknown-topic" does not exist
   **When** client sends Fetch request with topic_id for "unknown-topic"
   **Then** broker returns error_code = 100 (UNKNOWNTOPICID)

2. Error 100 is distinct from error 3 (UNKNOWN_TOPIC_OR_PARTITION)

3. Partition results empty when topic unknown

## Technical Requirements

### Error Codes

- 3 = UNKNOWN_TOPIC_OR_PARTITION
- 100 = UNKNOWNTOPICID

### From Story 2.1

- ClusterMetadataStore.getTopicById(uuid)

## Tasks / Subtasks

- [x] Task 1: Check topic_id existence (AC: #1, #2, #3)
  - [x] Subtask 1.1: Look up topic_id in ClusterMetadataStore
  - [x] Subtask 1.2: Return error 100 if not found
  - [x] Subtask 1.3: Continue with partition fetch if topic exists

## Dev Notes

- UNKNOWNTOPICID (100) is used for Fetch with topic_id that doesn't exist
- UNKNOWN_TOPIC_OR_PARTITION (3) is used for DescribeTopicPartitions with topic name that doesn't exist

## File List

- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- FetchHandler uses ClusterMetadataStore.getTopicById(uuid) for UUID-based lookup
- Returns error_code=100 (UNKNOWNTOPICID) when topic_id is not found
- Error 100 distinct from error 3 (UNKNOWN_TOPIC_OR_PARTITION) used by DescribeTopicPartitions
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` -- UPDATED