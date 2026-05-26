# Story 8.3: Transaction Log Persistence

Status: backlog

## Story

As a transaction coordinator,
I want to persist transaction state to a `__transaction_state` log,
so that transactions survive broker restarts and can be recovered.

## Acceptance Criteria

1. **Given** a transaction transitions to `ONGOING`
   **When** the state change is committed
   **Then** a `TxnRecord` is written to `__transaction_state-0/00000000000000000000.log`

2. **Given** a broker restarts after a crash
   **When** it loads the transaction log
   **Then** all in-progress transactions are recovered and their state is restored

3. **Given** a transaction in `PREPARE_COMMIT` state at crash time
   **When** the broker recovers
   **Then** the transaction completes its commit (no partial state left)

4. **Given** a transaction in `PREPARE_ABORT` state at crash time
   **When** the broker recovers
   **Then** the transaction completes its abort

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 8, Step 36)

- `__transaction_state` topic
- Transaction state persistence and recovery

### From NFRs

- **DRY:** Reuse `LogSegment` / `Partition` for log storage (same format as data logs)
- **DRY:** Reuse `ClusterMetadataLog` read/write pattern for record parsing
- **SOLID:** `TransactionLog` is single responsibility — persist and recover transaction state

## Tasks / Subtasks

- [ ] Task 1: Create TransactionLog (AC: #1)
  - [ ] Subtask 1.1: Create `broker/transaction/TransactionLog.java`
  - [ ] Subtask 1.2: Define `TxnRecord` format (type, transactional_id, producer_id, producer_epoch, state, partitions)
  - [ ] Subtask 1.3: `writeTxnRecord(TxnRecord)` — append to `__transaction_state-0` partition
  - [ ] Subtask 1.4: Use existing `Partition` class for storage (reuse)
- [ ] Task 2: Implement recovery on startup (AC: #2, #3, #4)
  - [ ] Subtask 2.1: `recoverTransactions()` — read all records from transaction log
  - [ ] Subtask 2.2: Rebuild in-memory state from records
  - [ ] Subtask 2.3: Complete any `PREPARE_COMMIT` → `COMPLETE_COMMIT`
  - [ ] Subtask 2.4: Complete any `PREPARE_ABORT` → `COMPLETE_ABORT`
  - [ ] Subtask 2.5: Expire any `ONGOING` transactions older than `transaction.timeout.ms`
- [ ] Task 3: Integrate with TransactionCoordinator (AC: #1)
  - [ ] Subtask 3.1: Write record on every state transition
  - [ ] Subtask 3.2: Load recovered state on startup
  - [ ] Subtask 3.3: Periodic cleanup of completed transactions (compaction)
- [ ] Task 4: Add test for transaction log persistence (AC: #2, #3)
  - [ ] Subtask 4.1: Write transactions, restart coordinator, verify recovery
  - [ ] Subtask 4.2: Test crash during PREPARE_COMMIT → verify completion

## Dev Notes

### TxnRecord Format

```
type: INT8 (1=INIT, 2=ADD_PARTITIONS, 3=COMMIT, 4=ABORT)
transactional_id: COMPACT_STRING
producer_id: INT64
producer_epoch: INT16
state: INT8 (0=EMPTY, 1=ONGOING, 2=PREPARE_COMMIT, 3=COMPLETE_COMMIT, 4=PREPARE_ABORT, 5=COMPLETE_ABORT)
partitions_count: INT32
partitions[]:
  topic_name: COMPACT_STRING
  partition_index: INT32
timestamp: INT64
```

### Recovery Flow

```java
void recover() {
    List<TxnRecord> records = transactionLog.readAll();

    // Group by transactional_id, take last record per transaction
    Map<String, TxnRecord> latestByTxnId = new LinkedHashMap<>();
    for (TxnRecord r : records) {
        latestByTxnId.put(r.transactionalId(), r);
    }

    for (TxnRecord r : latestByTxnId.values()) {
        switch (r.state()) {
            case ONGOING -> {
                // Check timeout
                if (isExpired(r)) expireTransaction(r);
                else restoreOngoing(r);
            }
            case PREPARE_COMMIT -> completeCommit(r);
            case PREPARE_ABORT -> completeAbort(r);
            case COMPLETE_COMMIT, COMPLETE_ABORT -> cleanup(r);
        }
    }
}
```

### Log Location

```
<log.dirs>/__transaction_state-0/
  00000000000000000000.log
  00000000000000000000.index
```

## File List

- `src/main/java/com/simplekafka/broker/transaction/TransactionLog.java` — NEW
- `src/main/java/com/simplekafka/broker/transaction/TxnRecord.java` — NEW
- `src/main/java/com/simplekafka/broker/transaction/TransactionCoordinator.java` — MODIFY (integrate log)
- `src/test/java/com/simplekafka/TransactionLogTest.java` — NEW
