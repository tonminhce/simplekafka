# Story 7.3: ISR Replication (Follower Pull)

Status: backlog

## Story

As a partition follower,
I want to pull records from the leader and maintain ISR membership,
so that data is replicated across brokers for fault tolerance.

## Acceptance Criteria

1. **Given** broker A is leader for partition "orders-0" and broker B is a follower
   **When** broker B sends a `ReplicaFetchRequest` to broker A
   **Then** broker A returns records starting from the follower's fetch offset

2. **Given** a follower has caught up to the leader's LEO (log end offset)
   **When** the leader evaluates ISR membership
   **Then** the follower is included in the ISR set

3. **Given** a follower has fallen behind by more than `replica.lag.max.messages`
   **When** the leader evaluates ISR membership
   **Then** the follower is removed from the ISR set

4. **Given** a produce request with `acks=-1` (all)
   **When** all ISR members have acknowledged
   **Then** the leader responds to the producer with success

5. **Given** a produce request with `acks=1` (leader only)
   **When** the leader writes to its log
   **Then** the leader responds immediately without waiting for followers

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 7, Step 31)

- Follower pulls records from leader
- ISR set management (add/remove followers)
- `acks=-1` waits for ISR sync

### From NFRs

- **DRY:** Reuse `Partition.readMessages()` for serving follower fetch requests
- **SOLID:** `ReplicaManager` manages ISR state, `Partition` manages log storage

## Tasks / Subtasks

- [ ] Task 1: Create ReplicaManager to track ISR state (AC: #2, #3)
  - [ ] Subtask 1.1: Create `broker/replication/ReplicaManager.java`
  - [ ] Subtask 1.2: Track `isr` set per partition (broker IDs)
  - [ ] Subtask 1.3: Track `followerOffsets` per partition (broker → offset)
  - [ ] Subtask 1.4: Add `updateFollowerOffset()` and `evaluateIsr()` methods
- [ ] Task 2: Implement ReplicaFetchHandler on leader (AC: #1)
  - [ ] Subtask 2.1: Parse `ReplicaFetchRequest` (topic, partition, fetchOffset)
  - [ ] Subtask 2.2: Read records from `Partition.readMessages(fetchOffset, maxBytes)`
  - [ ] Subtask 2.3: Build `ReplicaFetchResponse` with records and highWatermark
  - [ ] Subtask 2.4: Update follower offset in ReplicaManager
- [ ] Task 3: Implement follower pull loop (AC: #1)
  - [ ] Subtask 3.1: Background virtual thread per partition to periodically fetch from leader
  - [ ] Subtask 3.2: Write fetched records to local partition log
  - [ ] Subtask 3.3: Track `leaderEpoch` for fencing
- [ ] Task 4: Update ProduceHandler for acks=-1 (AC: #4, #5)
  - [ ] Subtask 4.1: When `acks=-1`, wait for all ISR members to catch up (with timeout)
  - [ ] Subtask 4.2: When `acks=1`, respond immediately after leader write
  - [ ] Subtask 4.3: When `acks=0`, respond immediately without waiting
- [ ] Task 5: Add integration test (AC: #1, #4)
  - [ ] Subtask 5.1: Leader + follower, produce with acks=-1, verify replication
  - [ ] Subtask 5.2: Test ISR shrinking when follower falls behind

## Dev Notes

### ReplicaFetchRequest Wire Format

```
api_key: INT16 (60 = FETCH_REPLICA)
api_version: INT16 (0)
correlation_id: INT32
client_id: COMPACT_STRING (broker ID as string)

Body:
  fetch_offset: INT64
  max_bytes: INT32
  topics_count: COMPACT_ARRAY
    topic_name: COMPACT_STRING
    partitions_count: COMPACT_ARRAY
      partition_index: INT32
      fetch_offset: INT64
      TAG_BUFFER
    TAG_BUFFER
  TAG_BUFFER
```

### ISR Evaluation Logic

```java
void evaluateIsr(String topicPartition) {
    long leaderOffset = partition.getNextOffset();
    Set<Integer> currentIsr = isr.get(topicPartition);

    for (int brokerId : currentIsr) {
        long followerOffset = followerOffsets.get(brokerId);
        if (leaderOffset - followerOffset > replicaLagMaxMessages) {
            currentIsr.remove(brokerId);
            // Update metadata
        }
    }

    // Check if any non-ISR replica has caught up
    for (int brokerId : allReplicas) {
        if (!currentIsr.contains(brokerId)) {
            long followerOffset = followerOffsets.get(brokerId);
            if (leaderOffset - followerOffset <= replicaLagMaxMessages) {
                currentIsr.add(brokerId);
            }
        }
    }
}
```

## File List

- `src/main/java/com/simplekafka/broker/replication/ReplicaManager.java` — NEW
- `src/main/java/com/simplekafka/broker/replication/ReplicaFetchHandler.java` — NEW
- `src/main/java/com/simplekafka/broker/replication/FollowerFetcher.java` — NEW
- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — MODIFY (acks=-1)
- `src/test/java/com/simplekafka/ReplicationTest.java` — NEW
