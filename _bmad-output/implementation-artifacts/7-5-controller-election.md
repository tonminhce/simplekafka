# Story 7.5: Real Controller Election

Status: backlog

## Story

As a cluster,
I want brokers to elect a controller via KRaft quorum voting,
so that the controller role is determined dynamically, not hardcoded.

## Acceptance Criteria

1. **Given** 3 brokers configured as a KRaft quorum
   **When** the cluster starts
   **Then** one broker is elected controller via quorum voting

2. **Given** the controller broker crashes
   **When** the remaining brokers detect the failure
   **Then** a new controller is elected from the remaining brokers

3. **Given** broker A is the controller
   **When** it receives a topic creation request
   **Then** it assigns partition leaders across all brokers and sends `LeaderAndIsr` requests

4. **Given** a broker is NOT the controller
   **When** it receives a topic creation request
   **Then** it forwards the request to the current controller or returns `NOT_CONTROLLER` error

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 7, Step 33)

- Multi-broker controller quorum
- Controller failover

### From NFRs

- **DRY:** Reuse `ClusterMetadataLog` for persisting controller state
- **SOLID:** `ControllerStateManager` is single responsibility — manage controller identity

## Tasks / Subtasks

- [ ] Task 1: Implement KRaft-style quorum voting (AC: #1)
  - [ ] Subtask 1.1: Create `broker/replication/ControllerStateManager.java`
  - [ ] Subtask 1.2: On startup, each broker votes for itself if it has the lowest ID among reachable voters
  - [ ] Subtask 1.3: Write ` ElectControllerRecord` to metadata log
  - [ ] Subtask 1.4: Announce controller identity to other brokers
- [ ] Task 2: Implement controller failure detection (AC: #2)
  - [ ] Subtask 2.1: Heartbeat between controller and brokers (via `InterBrokerClient`)
  - [ ] Subtask 2.2: If heartbeat missed N times → trigger re-election
  - [ ] Subtask 2.3: Increment `controllerEpoch` on each new election
- [ ] Task 3: Update SimpleKafkaBroker.createTopic for multi-broker (AC: #3)
  - [ ] Subtask 3.1: Controller assigns partition leaders round-robin across brokers
  - [ ] Subtask 3.2: Send `LeaderAndIsr` requests to all affected brokers
  - [ ] Subtask 3.3: Non-controller returns `NOT_CONTROLLER` error (AC: #4)
- [ ] Task 4: Add integration test (AC: #1, #2, #3)
  - [ ] Subtask 4.1: Start 3 brokers, verify controller election
  - [ ] Subtask 4.2: Kill controller, verify re-election
  - [ ] Subtask 4.3: Create topic on non-controller, verify forwarding

## Dev Notes

### Simplified KRaft Election

For this implementation, we use a simplified election:
- Each broker has a configured `broker.id` (numeric)
- On startup, brokers exchange `ControllerHeartbeat` requests
- The broker with the lowest ID among reachable voters wins
- This avoids implementing full Raft consensus (overkill for this project)

### Controller Assignment Logic

```java
// Round-robin partition assignment
BrokerInfo assignLeader(int partitionIndex, List<BrokerInfo> brokers) {
    return brokers.get(partitionIndex % brokers.size());
}

List<Integer> assignReplicas(int partitionIndex, List<BrokerInfo> brokers, short replicationFactor) {
    // Simple round-robin replica assignment
    List<Integer> replicas = new ArrayList<>();
    int startIndex = partitionIndex % brokers.size();
    for (int i = 0; i < replicationFactor; i++) {
        replicas.add(brokers.get((startIndex + i) % brokers.size()).getBrokerId());
    }
    return replicas;
}
```

## File List

- `src/main/java/com/simplekafka/broker/replication/ControllerStateManager.java` — NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — MODIFY (real election, multi-broker createTopic)
- `src/main/java/com/simplekafka/metadata/ClusterMetadataLog.java` — MODIFY (ElectControllerRecord)
- `src/test/java/com/simplekafka/ControllerElectionTest.java` — NEW
