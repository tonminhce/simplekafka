# Story 7.4: Partition Leader Election

Status: backlog

## Story

As a cluster,
I want to elect a partition leader with epoch-based fencing,
so that only one broker acts as leader for a given partition at any time.

## Acceptance Criteria

1. **Given** the controller decides broker B should be the new leader for partition "orders-0"
   **When** a `LeaderAndIsr` request is sent to broker B
   **Then** broker B becomes leader and starts accepting produce/fetch requests

2. **Given** broker B is the new leader with epoch=3
   **When** broker A (old leader with epoch=2) tries to serve a produce request
   **Then** the request is rejected with `STALE_LEADER_EPOCH_CODE` (error 83)

3. **Given** a leader receives a `LeaderAndIsr` request with its own broker ID
   **When** it is already the leader
   **Then** it updates the epoch and ISR list but continues as leader

4. **Given** a broker receives a `LeaderAndIsr` request to become follower
   **When** it was previously the leader
   **Then** it stops accepting produce requests and starts the follower fetch loop

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 7, Step 32)

- Epoch-based leader election
- Leader fencing on epoch mismatch
- `LeaderAndIsr` request handling

### From NFRs

- **DRY:** Reuse `InternalApiKeys` and `InterBrokerClient` from Story 7.2
- **SOLID:** `PartitionLeadership` manages leader state per partition

## Tasks / Subtasks

- [ ] Task 1: Create PartitionLeadership state manager (AC: #1, #2)
  - [ ] Subtask 1.1: Create `broker/replication/PartitionLeadership.java`
  - [ ] Subtask 1.2: Track `leaderBrokerId`, `leaderEpoch`, `isLeader` per partition
  - [ ] Subtask 1.3: Add `becomeLeader(epoch, isr)` and `becomeFollower(leaderId, epoch)` methods
  - [ ] Subtask 1.4: Validate epoch on every produce/fetch — reject stale epochs
- [ ] Task 2: Implement LeaderAndIsrHandler (AC: #1, #3, #4)
  - [ ] Subtask 2.1: Parse `LeaderAndIsrRequest` (topic, partition, leader, epoch, isr)
  - [ ] Subtask 2.2: If this broker is leader → call `becomeLeader()`
  - [ ] Subtask 2.3: If this broker is follower → call `becomeFollower()` + start fetcher
  - [ ] Subtask 2.4: Build `LeaderAndIsrResponse` with error codes
- [ ] Task 3: Add epoch validation to ProduceHandler and FetchHandler (AC: #2)
  - [ ] Subtask 3.1: Check `PartitionLeadership.isLeaderForEpoch(partition, epoch)`
  - [ ] Subtask 3.2: Return `STALE_LEADER_EPOCH_CODE` if epoch mismatch
- [ ] Task 4: Add integration test (AC: #1, #2, #4)
  - [ ] Subtask 4.1: Test leader transition from broker A to broker B
  - [ ] Subtask 4.2: Test stale epoch rejection
  - [ ] Subtask 4.3: Test leader-to-follower transition starts fetch loop

## Dev Notes

### LeaderAndIsrRequest Wire Format

```
api_key: INT16 (62 = LEADER_AND_ISR)
api_version: INT16 (0)

Body:
  controller_id: INT32
  controller_epoch: INT32
  partition_states_count: COMPACT_ARRAY
    topic_name: COMPACT_STRING
    partition_index: INT32
    leader_broker_id: INT32
    leader_epoch: INT32
    isr_count: INT32
    isr[]: INT32
    TAG_BUFFER
  TAG_BUFFER
```

### Epoch Validation Flow

```java
// In ProduceHandler / FetchHandler
PartitionLeadership pl = partitionLeadership.get(partitionKey);
if (pl == null || !pl.isLeader()) {
    return error(NOT_LEADER_OR_FOLLOWER);
}
if (requestEpoch < pl.getLeaderEpoch()) {
    return error(STALE_LEADER_EPOCH_CODE);
}
```

## File List

- `src/main/java/com/simplekafka/broker/replication/PartitionLeadership.java` — NEW
- `src/main/java/com/simplekafka/broker/replication/LeaderAndIsrHandler.java` — NEW
- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — MODIFY (epoch check)
- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` — MODIFY (epoch check)
- `src/test/java/com/simplekafka/LeaderElectionTest.java` — NEW
