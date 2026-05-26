# Story 3.1: Produce API in ApiVersions

Status: done

## Story

As a client,
I want to know the broker supports Produce API,
so I can send messages.

## Acceptance Criteria

1. **Given** a client querying ApiVersions
   **When** client receives ApiVersions response
   **Then** response includes API key 0 (Produce) with version range 0-11

## Technical Requirements

### From IMPLEMENTATION_PLAN.md

- API key 0: Produce (min: 0, max: 11)
- API key 18: ApiVersions (min: 0, max: 4)
- API key 75: DescribeTopicPartitions (min: 0, max: 0)

### Story 1.2 Already Covers

- ApiVersions response format
- Adding API keys to the response array

## Tasks / Subtasks

- [x] Task 1: Add Produce (key 0) to ApiVersions response (AC: #1)
  - [x] Subtask 1.1: Ensure ApiVersionsHandler includes key 0 with versions 0-11

## Dev Notes

- This story may be trivial if Story 1.2 was implemented correctly with all keys
- Verify API key 0 is in the api_versions array

## File List

- `src/main/java/com/simplekafka/broker/handlers/ApiVersionsHandler.java` — UPDATE (if needed)

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Debug Log References

### Completion Notes List

- Already implemented in Story 1.2: ApiVersionsHandler includes key 0 (Produce) with versions 0-11
- Also includes key 1 (Fetch) with versions 0-16
- Build: mvn clean package SUCCESS

### File List

- `src/main/java/com/simplekafka/broker/handlers/ApiVersionsHandler.java` -- VERIFIED (no changes needed)