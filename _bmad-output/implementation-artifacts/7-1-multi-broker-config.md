---
baseline_commit: 9f60b2c7b5d6ee0368d69a1ca5e9eddf6de1f0b1
---

# Story 7.1: Multi-Broker Configuration

Status: backlog

## Story

As a cluster operator,
I want to configure multiple brokers with unique identities,
so that each broker knows its own ID, listeners, and log directories.

## Acceptance Criteria

1. **Given** a `server.properties` with `broker.id=2`, `listeners=PLAINTEXT://localhost:9093`, `log.dirs=/var/lib/kafka/broker2`
   **When** the broker starts
   **Then** it reads these values and uses them for identity and storage

2. **Given** a config with `broker.id=1`
   **When** `BrokerInfo` is created
   **Then** the broker ID, host, and port match the config

3. **Given** a seed broker list `localhost:9092,localhost:9093,localhost:9094`
   **When** config is loaded
   **Then** the broker knows about other brokers in the cluster

4. **Given** no config file is provided
   **When** a broker starts with defaults
   **Then** it behaves exactly as before (backward compatible)

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 7, Step 29)

- `shared/Config.java` with `broker.id`, `listeners`, `log.dirs`, `seed.brokers`
- Properties file parsing
- Per-broker identity

### From NFRs

- **DRY:** Extend existing `shared/Config.java` (created in Story 6.4) with broker cluster fields
- **SOLID:** Config holds data, BrokerInfo is an immutable value object
- **Backward compatible:** Existing single-broker setup works unchanged

## Tasks / Subtasks

- [x] Task 1: Extend Config.java with cluster fields (AC: #1, #3)
  - [x] Subtask 1.1: Add `seedBrokers` field (`List<BrokerInfo>`)
  - [x] Subtask 1.2: Parse `seed.brokers` from properties (comma-separated `host:port` list)
  - [x] Subtask 1.3: Add `controller.quorum.voters` for KRaft quorum config
- [x] Task 2: Update BrokerInfo to be created from Config (AC: #2)
  - [x] Subtask 2.1: `BrokerInfo.fromConfig(Config)` factory method
  - [x] Subtask 2.2: Ensure host/port parsed from `listeners` property
- [x] Task 3: Update SimpleKafkaBroker startup (AC: #1, #4)
  - [x] Subtask 3.1: Load config from properties file path in `main()`
  - [x] Subtask 3.2: Fall back to defaults if no file provided
  - [x] Subtask 3.3: Pass seed brokers to replication subsystem
- [x] Task 4: Add test for multi-broker config (AC: #1, #3)
  - [x] Subtask 4.1: Test properties file parsing with multiple brokers
  - [x] Subtask 4.2: Test backward compatibility with no config file

## Dev Notes

### Config Properties Format

```properties
# server.properties
broker.id=1
listeners=PLAINTEXT://localhost:9092
log.dirs=/tmp/kraft-combined-logs
log.flush.interval.ms=0

# Cluster configuration
seed.brokers=localhost:9092,localhost:9093,localhost:9094
controller.quorum.voters=1@localhost:9092,2@localhost:9093
```

### Dependency

- Requires Story 6.4 (Config.java) to be completed first

## File List

- `src/main/java/com/simplekafka/shared/Config.java` — MODIFY (add cluster fields)
- `src/main/java/com/simplekafka/broker/BrokerInfo.java` — MODIFY (fromConfig factory)
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — MODIFY (load config)
- `src/test/java/com/simplekafka/MultiBrokerConfigTest.java` — NEW

## Dev Agent Record

### Agent Model Used

claude-haiku-4-5-20250601

### Completion Notes

- Extended `Config.java` with `seedBrokers` (List<BrokerInfo>), `listeners`, and `controllerQuorumVoters` (List<QuorumVoter>)
- Added `QuorumVoter` inner class with `parse(String)` for `id@host:port` format
- Added `Config.getBrokerInfo()` factory method returning BrokerInfo from config
- Added `BrokerInfo.fromConfig(Config)` factory method
- Updated `SimpleKafkaBroker(Config)` to use `config.getBrokerInfo()` instead of direct constructor
- 10 new tests in `MultiBrokerConfigTest`: single broker props, BrokerInfo from config, seed brokers, quorum voters, QuorumVoter.parse, defaults, backward compat, empty seeds, IPv6
- All 86 tests pass (no regressions)
- Fixed variable shadowing: renamed `colonIdx` variables to `listenersColonIdx`, `listenersHostColonIdx`, `seedColonIdx`
