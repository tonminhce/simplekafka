---
baseline_commit: 400e0e4
---

# Story 6.4: Configurable Log Directory

Status: review

## Story

As a broker operator,
I want to configure the log directory via properties file or constructor,
so that I can store logs in a location appropriate for my deployment environment.

## Acceptance Criteria

1. **Given** a `server.properties` file with `log.dirs=/var/lib/kafka/logs`
   **When** the broker starts
   **Then** all partition logs are stored under `/var/lib/kafka/logs`

2. **Given** a `SimpleKafkaBroker(port, brokerId)` constructor call
   **When** no config file is provided
   **Then** the default log directory `/tmp/kraft-combined-logs` is used (backward compatible)

3. **Given** a `Config` instance
   **When** any component needs the log directory
   **Then** it calls `config.getLogDir()` from the shared config (DRY)

4. **Given** a properties file with `log.dirs` missing
   **When** config is loaded
   **Then** the default `/tmp/kraft-combined-logs` is used

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 6, Step 28)

- Extract `LOG_DIR` from `ProduceHandler`/`ClusterMetadataLog` into `shared/Config.java`
- Accept log directory via constructor or properties file
- Update `SimpleKafkaBroker` to pass config through to all components

### From NFRs

- **DRY:** Single source of truth for configuration — `shared/Config.java`
- **SOLID:** Config is a single responsibility — hold and provide configuration values
- **Backward compatible:** Existing code works without config file

## Tasks / Subtasks

- [x] Task 1: Create shared/Config.java (AC: #3)
  - [x] Subtask 1.1: Add `brokerId`, `port`, `logDir`, `flushIntervalMs` fields
  - [x] Subtask 1.2: Add defaults: `logDir="/tmp/kraft-combined-logs"`, `flushIntervalMs=0`
  - [x] Subtask 1.3: Add `Config.fromProperties(File)` loader
  - [x] Subtask 1.4: Add `Config()` default constructor with sensible defaults
- [x] Task 2: Update SimpleKafkaBroker to accept Config (AC: #1, #2)
  - [x] Subtask 2.1: Add `SimpleKafkaBroker(Config)` constructor
  - [x] Subtask 2.2: Keep existing constructors for backward compatibility
  - [x] Subtask 2.3: Pass `logDir` to `ProduceHandler`, `FetchHandler`, `ClusterMetadataLog`
- [x] Task 3: Remove hardcoded LOG_DIR from ProduceHandler (AC: #3)
  - [x] Subtask 3.1: Accept `logDir` via constructor instead of hardcoded constant
  - [x] Subtask 3.2: Use `logDir` field everywhere
- [x] Task 4: Remove hardcoded path from ClusterMetadataLog (AC: #3)
  - [x] Subtask 4.1: Accept `logDir` via constructor
  - [x] Subtask 4.2: Build metadata path from `logDir + "/__cluster_metadata-0/..."`
- [x] Task 5: Update tests (AC: #4)
  - [x] Subtask 5.1: Create ConfigTest for properties file loading
  - [x] Subtask 5.2: Test default values, custom values, missing properties

## Dev Notes

### Config.java Skeleton

```java
package com.simplekafka.shared;

public class Config {
    private int brokerId = 1;
    private int port = 9092;
    private String logDir = "/tmp/kraft-combined-logs";
    private long flushIntervalMs = 0;

    public Config() {}
    public Config(int brokerId, int port, String logDir) { ... }
    public static Config fromProperties(File file) throws IOException { ... }
    // getters and setters
}
```

### Edge Cases Reviewed

- **Backward compatibility**: `SimpleKafkaBroker(port)` and `SimpleKafkaBroker(port, brokerId)` still work — they delegate to `SimpleKafkaBroker(Config)` with default logDir
- **ClusterMetadataLog(String)**: Changed parameter from `metadataDir` (full path) to `logDir` (base) — automatically appends `/__cluster_metadata-0/`
- **ProduceHandler backward compat**: Original `ProduceHandler(metadataStore)` constructor preserved with default logDir
- **Properties parsing**: Port extracted from `listeners` format using `lastIndexOf(':')` to handle hostnames with colons (e.g., `[::1]:9092`)
- **No file overwrite**: `fromProperties()` doesn't modify the properties file

## File List

- `src/main/java/com/simplekafka/shared/Config.java` — NEW
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — MODIFIED (Config-based constructor)
- `src/main/java/com/simplekafka/broker/handlers/ProduceHandler.java` — MODIFIED (logDir via constructor)
- `src/main/java/com/simplekafka/metadata/ClusterMetadataLog.java` — MODIFIED (logDir-based constructor)
- `src/main/java/com/simplekafka/Main.java` — MODIFIED (--config flag support)
- `src/test/java/com/simplekafka/ConfigTest.java` — NEW (7 tests)

## Dev Agent Record

### Agent Model Used

claude-opus-4-6

### Completion Notes

- Created `shared/Config.java` with `brokerId`, `port`, `logDir`, `flushIntervalMs` fields
- `Config.fromProperties(File)` supports `broker.id`, `listeners`, `log.dirs`, `log.flush.interval.ms`
- `SimpleKafkaBroker` has 3 constructors: `(port)`, `(port, brokerId)`, `(Config)` — all backward-compatible
- Removed hardcoded `LOG_DIR` from `ProduceHandler`, replaced with constructor-injected `logDir`
- `ClusterMetadataLog(String logDir)` now takes base log dir, appends `/__cluster_metadata-0/` internally
- `Main.java` supports `--config server.properties` flag
- 7 new Config tests: defaults, parameterized constructor, setters, properties loading, missing properties
- Full regression suite: 76 tests, 0 failures
