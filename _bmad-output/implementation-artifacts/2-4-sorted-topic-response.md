# Story 2.4: Sorted Topic Response

Status: done

## Story

As a client,
I want topics returned in alphabetical order,
so responses are predictable.

## Acceptance Criteria

1. **Given** broker has topics "zeta", "alpha", "beta"
   **When** client requests DescribeTopicPartitions with all three topics
   **Then** broker returns topics in order: alpha, beta, zeta

2. Sort order is case-sensitive alphabetical (standard String sort)

3. Topics that don't exist are not included in response (only existing topics are sorted)

## Technical Requirements

### From IMPLEMENTATION_PLAN.md

- Topics must be sorted alphabetically

## Tasks / Subtasks

- [x] Task 1: Sort topics before building response (AC: #1, #2, #3)
  - [x] Subtask 1.1: Collect existing topics from ClusterMetadataStore
  - [x] Subtask 1.2: Sort using String.compareTo (alphabetical)
  - [x] Subtask 1.3: Build response in sorted order

## Dev Notes

### Sorting Logic

```java
List<String> sortedTopics = topics.stream()
    .sorted()
    .collect(Collectors.toList());
```

## File List

- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` — UPDATE

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Topics sorted alphabetically using String::compareTo before building response
- DescribeTopicPartitionsHandler.buildResponseBody() sorts requested topics
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` -- UPDATED