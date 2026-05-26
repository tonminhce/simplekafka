---
baseline_commit: 9f60b2c7b5d6ee0368d69a1ca5e9eddf6de1f0b1
---

# Story 7.2: Inter-Broker Communication

Status: backlog

## Story

As a broker,
I want to communicate with other brokers in the cluster,
so that I can exchange replication requests and metadata updates.

## Acceptance Criteria

1. **Given** broker A (port 9092) and broker B (port 9093) are running
   **When** broker A sends an internal request to broker B
   **Then** the request is received and a response is returned

2. **Given** a broker receives a request on its internal port
   **When** the request has a recognized internal API key
   **Then** it dispatches to the appropriate internal handler

3. **Given** broker A cannot reach broker B
   **When** a connection attempt fails
   **Then** the error is logged and retried with backoff (no crash)

4. Internal requests use the same wire protocol framing as client requests

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 7, Step 30)

- Internal broker-to-broker channel
- Replication pull request protocol

### Design Decisions

- Reuse existing `SimpleKafkaClient` for outbound connections (DRY)
- Internal API keys: 60+ range (not conflicting with client APIs 0, 1, 18, 75)
- New internal handler registry in `SimpleKafkaBroker`

## Tasks / Subtasks

- [ ] Task 1: Define internal API keys and request/response formats (AC: #2, #4)
  - [ ] Subtask 1.1: Add `InternalApiKeys` constants in `shared/` (e.g., `FETCH_REPLICA=60`, `UPDATE_METADATA=61`, `LEADER_AND_ISR=62`)
  - [ ] Subtask 1.2: Define `InternalRequestHeader` (reuse existing header format)
  - [ ] Subtask 1.3: Define wire format for `ReplicaFetchRequest` / `ReplicaFetchResponse`
- [ ] Task 2: Create InterBrokerClient for outbound requests (AC: #1)
  - [ ] Subtask 2.1: Wrap `SimpleKafkaClient` with broker-targeted connection pool
  - [ ] Subtask 2.2: Add retry logic with exponential backoff (AC: #3)
  - [ ] Subtask 2.3: Connection caching — keep connections alive to known brokers
- [ ] Task 3: Add internal handler routing in SimpleKafkaBroker (AC: #2)
  - [ ] Subtask 3.1: Extend `dispatchRequest()` to handle internal API keys
  - [ ] Subtask 3.2: Create `InternalRequestHandler` interface in `broker/replication/`
- [ ] Task 4: Add integration test for inter-broker communication (AC: #1, #3)
  - [ ] Subtask 4.1: Start two brokers, send internal request, verify response
  - [ ] Subtask 4.2: Test connection failure handling

## Dev Notes

### Internal API Keys

```java
package com.simplekafka.shared;

public final class InternalApiKeys {
    public static final short FETCH_REPLICA = 60;
    public static final short UPDATE_METADATA = 61;
    public static final short LEADER_AND_ISR = 62;
    public static final short STOP_REPLICA = 63;

    private InternalApiKeys() {}
}
```

### Package Structure

```
broker/replication/
  InterBrokerClient.java      — outbound connections to other brokers
  InternalRequestHandler.java — interface for internal request handlers
  ReplicaFetchHandler.java    — handles FETCH_REPLICA (Story 7.3)
```

## File List

- `src/main/java/com/simplekafka/shared/InternalApiKeys.java` — NEW
- `src/main/java/com/simplekafka/broker/replication/InterBrokerClient.java` — NEW
- `src/main/java/com/simplekafka/broker/replication/InternalRequestHandler.java` — NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — MODIFY (internal routing)
- `src/test/java/com/simplekafka/InterBrokerTest.java` — NEW
