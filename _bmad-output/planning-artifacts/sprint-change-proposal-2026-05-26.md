---
generated: 2026-05-26
project: simplekafka
trigger: Post-implementation review gap analysis
scope: major
---

# Sprint Change Proposal — simplekafka Production Readiness

## Section 1: Issue Summary

**Problem Statement:** Post-implementation review of Epics 1-5 (24-step Kafka challenge) revealed significant gaps between the current minimal broker and a production-grade Kafka implementation. Constants and data structures exist (SEGMENT_SIZE_LIMIT, index files, ISR fields) but the enforcing logic was never implemented. The broker works end-to-end for single-broker, single-partition scenarios but cannot handle segment overflow, efficient offset lookups, multi-broker replication, transactions, or security.

**Discovery Context:** Review performed after Epic 5 completion using GitNexus code intelligence (539 symbols, 2099 relationships, 46 execution flows) + manual source code inspection.

**Evidence:**
- `Partition.java:25` — `SEGMENT_SIZE_LIMIT` defined, never referenced in logic
- `Partition.java:197` — `findSegmentForOffset()` is linear scan (comment says "binary search")
- `Partition.java:217` — `findPositionForOffset()` reads entire segment, ignores `.index` file
- `LogSegment.java:100` — `writeIndexEntry()` writes to `.index` but no read method exists
- `ProduceHandler.java:38` — hardcoded `LOG_DIR = "/tmp/kraft-combined-logs"`
- `SimpleKafkaBroker.java:112` — `electController()` sets flag blindly, no election
- No `fsync()` calls in `LogSegment` — data loss risk on crash
- No auth, no ACL, no SASL — all connections accepted unconditionally

## Section 2: Impact Analysis

### Epic Impact

| Existing Epic | Impact | Action |
|---------------|--------|--------|
| Epic 1: Broker Startup & Protocol | None | No changes |
| Epic 2: Topic Metadata Discovery | None | No changes |
| Epic 3: Message Production | None | No changes |
| Epic 4: Message Consumption | None | No changes |
| Epic 5: Cluster Management | None | No changes |

### New Epics Required

| New Epic | Priority | Stories | Dependencies |
|----------|----------|---------|--------------|
| Epic 6: Storage Robustness & Performance | P0 | 4 | None (foundation) |
| Epic 7: Multi-Broker & Replication | P1 | 5 | Epic 6 |
| Epic 8: Transactional Producers | P2 | 3 | Epic 7 |
| Epic 9: Security | P2 | 2 | None (independent) |

### Artifact Conflicts

| Artifact | Impact | Action |
|----------|--------|--------|
| IMPLEMENTATION_PLAN.md | Missing Phase 6 | Add Phase 6 section |
| epics.md | Missing Epic 6-9 | Add new epic sections with FR coverage |
| sprint-status.yaml | Missing Epic 6-9 entries | Add with status `backlog` |
| ErrorCodes.java | Missing error codes for replication/auth | Add new constants |

### Technical Impact

| Area | Change Type | Files Affected |
|------|-------------|----------------|
| Partition.java | MODIFY | Segment rolling, index lookup |
| LogSegment.java | MODIFY | Index read, fsync |
| ProduceHandler.java | MODIFY | Configurable log dir |
| ClusterMetadataLog.java | MODIFY | Configurable log dir |
| SimpleKafkaBroker.java | MODIFY | Config injection, multi-broker |
| shared/Config.java | NEW | Configuration management |
| shared/ErrorCodes.java | MODIFY | New error codes |
| broker/replication/ | NEW PACKAGE | Follower, ISR, leader election |
| shared/auth/ | NEW PACKAGE | SASL, ACL |

### Coding Convention Compliance

All new code follows established conventions:
- **DRY** — shared utilities stay in `com.simplekafka.shared` (primitives, ErrorCodes, Protocol, Config)
- **SOLID** — single responsibility per class (LogSegment handles I/O, Partition manages segments, Config handles settings)
- **No external dependencies** — pure Java standard library only

## Section 3: Recommended Approach

**Selected Path: Direct Adjustment (Option 1)**

Add 4 new epics (14 stories) to the existing project. No rollbacks needed. Existing Epics 1-5 are stable and complete.

**Rationale:**
- All completed work remains valid
- New features are purely additive
- Clear dependency chain (6→7→8, 9 is independent)
- No breaking changes to existing APIs or wire protocol
- Effort: Medium-High, Risk: Low

**Trade-offs Considered:**
- Alternative: Skip to multi-broker first → Rejected: Storage bugs (no segment rolling, no fsync) would cause data loss in multi-broker scenario
- Alternative: Do security first → Rejected: Security without proper storage and replication is premature

## Section 4: Detailed Change Proposals

### Proposal 1: Update epics.md — Add Epic 6-9

**File:** `_bmad-output/planning-artifacts/epics.md`

**ADD after Epic 5 section:**

```markdown
### Epic 6: Storage Robustness & Performance
Fix foundation-level storage issues — segment rolling, index lookups, durability, and configuration.

**FRs covered:** FR25 (segment rolling), FR26 (index-based lookup), FR27 (fsync durability), FR28 (configurable log dir)

### Epic 7: Multi-Broker & Replication
Enable multi-broker deployment with partition replication, ISR management, and leader election.

**FRs covered:** FR29 (multi-broker config), FR30 (inter-broker communication), FR31 (ISR replication), FR32 (leader election), FR33 (real controller election)

### Epic 8: Transactional Producers
Implement idempotent producers and transactional produce/commit/abort protocol.

**FRs covered:** FR34 (idempotent producers), FR35 (transaction protocol), FR36 (transaction log)

### Epic 9: Security
Add authentication (SASL) and authorization (ACL) layers.

**FRs covered:** FR37 (SASL authentication), FR38 (ACL authorization)
```

**ADD to FR Coverage Map:**

```markdown
FR25: Epic 6 - Enforce SEGMENT_SIZE_LIMIT, roll to new segment when exceeded
FR26: Epic 6 - Read .index file for binary search offset-to-position lookup
FR27: Epic 6 - fsync after log writes, flush on close
FR28: Epic 6 - Configurable log directory from config file/constructor
FR29: Epic 7 - Multi-broker configuration with broker.id, listeners, log.dirs
FR30: Epic 7 - Inter-broker communication channel for replication pulls
FR31: Epic 7 - Follower pull replication, ISR set management
FR32: Epic 7 - Epoch-based leader election with fencing
FR33: Epic 7 - Real controller election among multiple brokers
FR34: Epic 8 - Sequence number tracking and deduplication
FR35: Epic 8 - InitProducerId, AddPartitionsToTxn, EndTxn handlers
FR36: Epic 8 - Transaction state log persistence
FR37: Epic 9 - SASL authentication framework
FR38: Epic 9 - ACL-based authorization per request
```

### Proposal 2: Update sprint-status.yaml — Add Epic 6-9

**File:** `_bmad-output/implementation-artifacts/sprint-status.yaml`

**ADD after epic-5 section:**

```yaml
  # Epic 6: Storage Robustness & Performance
  epic-6: backlog
  6-1-segment-rolling: backlog
  6-2-index-offset-lookup: backlog
  6-3-durability-fsync: backlog
  6-4-configurable-log-dir: backlog
  epic-6-retrospective: optional

  # Epic 7: Multi-Broker & Replication
  epic-7: backlog
  7-1-multi-broker-config: backlog
  7-2-inter-broker-communication: backlog
  7-3-isr-replication: backlog
  7-4-leader-election: backlog
  7-5-controller-election: backlog
  epic-7-retrospective: optional

  # Epic 8: Transactional Producers
  epic-8: backlog
  8-1-idempotent-producers: backlog
  8-2-transaction-protocol: backlog
  8-3-transaction-log: backlog
  epic-8-retrospective: optional

  # Epic 9: Security
  epic-9: backlog
  9-1-sasl-authentication: backlog
  9-2-acl-authorization: backlog
  epic-9-retrospective: optional
```

### Proposal 3: Update IMPLEMENTATION_PLAN.md — Add Phase 6

**File:** `IMPLEMENTATION_PLAN.md`

**ADD after Phase 5 section:**

```markdown
### Phase 6: Storage Robustness (P0)

22. **Segment rolling (size-based)**
    - Enforce SEGMENT_SIZE_LIMIT in Partition.appendRecordBatch()
    - When active segment exceeds limit, call createNewSegment(nextOffset)
    - Close old segment, open new log/index file pair

23. **Index-based offset lookup**
    - Add LogSegment.lookupPosition(relativeOffset) to binary-search .index file
    - Replace sequential scan in Partition.findPositionForOffset() with index lookup
    - Replace linear scan in Partition.findSegmentForOffset() with binary search

24. **Log durability (fsync)**
    - Call FileChannel.force(false) after each append in LogSegment
    - Add force() on close()
    - Configurable flush interval (fsync every N writes or N milliseconds)

25. **Configurable log directory**
    - Extract LOG_DIR from ProduceHandler/ClusterMetadataLog into shared/Config.java
    - Accept log directory via constructor or properties file
    - Update SimpleKafkaBroker to pass config through

### Phase 7: Multi-Broker & Replication (P1)

26. **Multi-broker configuration**
    - shared/Config.java with broker.id, listeners, log.dirs, zookeeper.connect
    - Properties file parsing
    - Per-broker identity

27. **Inter-broker communication**
    - Internal broker-to-broker channel
    - Replication pull request protocol

28. **ISR replication**
    - Follower pulls records from leader
    - ISR set management (add/remove followers)
    - acks=-1 waits for ISR sync

29. **Partition leader election**
    - Epoch-based leader election
    - Leader fencing on epoch mismatch
    - LeaderAndIsr request handling

30. **Real controller election**
    - Multi-broker controller quorum
    - Controller failover

### Phase 8: Transactional Producers (P2)

31. **Idempotent producers**
    - Sequence number tracking per producer ID
    - Deduplication on broker side

32. **Transaction protocol**
    - InitProducerId handler
    - AddPartitionsToTxn handler
    - EndTxn (commit/abort) handler

33. **Transaction log**
    - __transaction_state topic
    - Transaction state persistence and recovery

### Phase 9: Security (P2)

34. **SASL authentication**
    - Authentication framework in shared/auth/
    - Plain/SASL mechanism support

35. **ACL authorization**
    - ACL storage and lookup
    - Per-request authorization check
```

### Proposal 4: Update ErrorCodes.java — Add New Error Codes

**File:** `src/main/java/com/simplekafka/shared/ErrorCodes.java`

**ADD new constants:**

```java
// Replication errors
public static final short NOT_LEADER_OR_FOLLOWER = 6;
public static final short REPLICA_NOT_AVAILABLE = 9;
public static final short STALE_CONTROLLER_EPOCH = 11;
public static final short STALE_LEADER_EPOCH_CODE = 83;
public static final short NOT_ENOUGH_REPLICAS = 19;

// Transaction errors
public static final short INVALID_PRODUCER_EPOCH = 47;
public static final short PRODUCER_FENCED = 40;
public static final short TRANSACTIONAL_ID_NOT_FOUND = 48;
public static final short INVALID_TXN_STATE = 49;

// Security errors
public static final short SASL_AUTHENTICATION_FAILED = 58;
public static final short TOPIC_AUTHORIZATION_FAILED = 29;
public static final short CLUSTER_AUTHORIZATION_FAILED = 31;
```

## Section 5: Implementation Handoff

### Change Scope: **Major**

4 new epics, 14 stories, touches core storage layer and adds entirely new subsystems (replication, transactions, security).

### Handoff Plan

| Phase | Route To | Deliverables |
|-------|----------|-------------|
| Epic 6 (P0) | Developer agent (Amelia) | Fix storage bugs first — foundation for everything else |
| Epic 7 (P1) | Developer + Architect | New subsystem — needs design review before implementation |
| Epic 8 (P2) | Developer | After Epic 7 — transaction coordinator builds on replication |
| Epic 9 (P2) | Developer | Independent — can run in parallel with Epic 8 |

### Success Criteria

- [ ] Epic 6: All existing tests pass + new tests for segment rolling, index lookup, fsync, config
- [ ] Epic 7: Multi-broker integration test with leader failover
- [ ] Epic 8: Transactional produce + commit/abort end-to-end test
- [ ] Epic 9: Auth challenge + ACL deny test

### Recommended Next Step

Run `/bmad-sprint-planning` in a fresh context window to plan Epic 6 execution.
