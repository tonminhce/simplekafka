# Story 8.2: Transaction Protocol

Status: backlog

## Story

As a transactional producer,
I want to initiate, add partitions to, and commit/abort transactions,
so that messages across multiple partitions are atomically visible or not visible to consumers.

## Acceptance Criteria

1. **Given** a producer sends `InitProducerId` with `transactional_id="tx-producer-1"`
   **When** the transaction coordinator processes it
   **Then** a new `producer_id` and `producer_epoch` are assigned and returned

2. **Given** a producer with an active transaction
   **When** it sends `AddPartitionsToTxn` for partitions "orders-0" and "orders-1"
   **Then** those partitions are enrolled in the transaction

3. **Given** a producer sends `EndTxn` with `committed=true`
   **When** the transaction coordinator processes it
   **Then** all records in the transaction are marked committed and visible to consumers

4. **Given** a producer sends `EndTxn` with `committed=false` (abort)
   **When** the transaction coordinator processes it
   **Then** all records in the transaction are marked aborted and invisible to consumers

5. **Given** a producer sends a RecordBatch with `transactional_id` set
   **When** the broker processes it
   **Then** the batch is appended with transactional markers (not visible until commit)

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 8, Step 35)

- `InitProducerId` handler
- `AddPartitionsToTxn` handler
- `EndTxn` (commit/abort) handler

### From NFRs

- **DRY:** Reuse `ProducerSequenceCache` from Story 8.1
- **SOLID:** `TransactionCoordinator` is single responsibility — manage transaction lifecycle

## Tasks / Subtasks

- [ ] Task 1: Add API keys for transaction requests (AC: #1, #2, #3)
  - [ ] Subtask 1.1: Add to `ApiVersionsHandler`: `InitProducerId=66`, `AddPartitionsToTxn=67`, `EndTxn=68`
  - [ ] Subtask 1.2: Add wire format parsers for each request/response
- [ ] Task 2: Create TransactionCoordinator (AC: #1, #2, #3, #4)
  - [ ] Subtask 2.1: Create `broker/transaction/TransactionCoordinator.java`
  - [ ] Subtask 2.2: Map: `transactional_id → TransactionState`
  - [ ] Subtask 2.3: `initProducerId(txnId)` → assign producer_id + epoch
  - [ ] Subtask 2.4: `addPartitions(txnId, partitions)` → enroll partitions
  - [ ] Subtask 2.5: `endTxn(txnId, committed)` → mark all partitions committed/aborted
- [ ] Task 3: Create InitProducerIdHandler (AC: #1)
  - [ ] Subtask 3.1: Parse request (transactional_id, transaction_timeout_ms)
  - [ ] Subtask 3.2: Call `TransactionCoordinator.initProducerId()`
  - [ ] Subtask 3.3: Build response (producer_id, producer_epoch)
- [ ] Task 4: Create AddPartitionsToTxnHandler (AC: #2)
  - [ ] Subtask 4.1: Parse request (transactional_id, producer_id, producer_epoch, partitions)
  - [ ] Subtask 4.2: Call `TransactionCoordinator.addPartitions()`
- [ ] Task 5: Create EndTxnHandler (AC: #3, #4)
  - [ ] Subtask 5.1: Parse request (transactional_id, producer_id, producer_epoch, committed)
  - [ ] Subtask 5.2: Call `TransactionCoordinator.endTxn()`
  - [ ] Subtask 5.3: Write commit/abort markers to enrolled partitions
- [ ] Task 6: Update FetchHandler for transactional visibility (AC: #3, #4)
  - [ ] Subtask 6.1: Skip records from aborted transactions
  - [ ] Subtask 6.2: `last_stable_offset` = lowest offset of open transactions
- [ ] Task 7: Add test for transaction lifecycle (AC: #1, #2, #3, #4)
  - [ ] Subtask 7.1: InitProducerId → produce → commit → verify visible
  - [ ] Subtask 7.2: InitProducerId → produce → abort → verify invisible

## Dev Notes

### Transaction State Machine

```
EMPTY → ONGOING → PREPARE_COMMIT → COMPLETE_COMMIT
                 → PREPARE_ABORT  → COMPLETE_ABORT
```

### API Key Registration

```java
// In ApiVersionsHandler
new ApiVersionEntry((short) 66, (short) 0, (short) 3),  // InitProducerId
new ApiVersionEntry((short) 67, (short) 0, (short) 3),  // AddPartitionsToTxn
new ApiVersionEntry((short) 68, (short) 0, (short) 2),  // EndTxn
```

### Package Structure

```
broker/transaction/
  TransactionCoordinator.java  — manage transaction lifecycle
  TransactionState.java        — enum: EMPTY, ONGOING, PREPARE_COMMIT, COMPLETE_COMMIT, etc.
  InitProducerIdHandler.java   — handle API key 66
  AddPartitionsToTxnHandler.java — handle API key 67
  EndTxnHandler.java           — handle API key 68
```

## File List

- `src/main/java/com/simplekafka/broker/transaction/TransactionCoordinator.java` — NEW
- `src/main/java/com/simplekafka/broker/transaction/TransactionState.java` — NEW
- `src/main/java/com/simplekafka/broker/transaction/InitProducerIdHandler.java` — NEW
- `src/main/java/com/simplekafka/broker/transaction/AddPartitionsToTxnHandler.java` — NEW
- `src/main/java/com/simplekafka/broker/transaction/EndTxnHandler.java` — NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — MODIFY (register handlers)
- `src/main/java/com/simplekafka/broker/handlers/FetchHandler.java` — MODIFY (transactional visibility)
- `src/test/java/com/simplekafka/TransactionTest.java` — NEW
