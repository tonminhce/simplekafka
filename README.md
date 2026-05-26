# SimpleKafka

A Kafka-like distributed messaging system implemented in **pure Java** — no external dependencies, no ZooKeeper, no external libraries beyond the JDK.

Designed to understand the Kafka wire protocol and log-structured storage from first principles.

---

## Table of Contents

- [Theory: How Kafka Works](#theory-how-kafka-works)
- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [Demo](#demo)

---

## Theory: How Kafka Works

### What is Kafka?

Apache Kafka is a distributed, partitioned, replicated commit log service. It provides the messaging functionality of a publish-subscribe system but is optimized for high-throughput, low-latency event streaming.

### Core Concepts

#### 1. Topics and Partitions

A **topic** is a named channel for messages. Topics are **partitioned** — each partition is an ordered, immutable sequence of records.

```
Topic: "orders"
├── Partition 0 ── [offset 0, 1, 2, 3, ...]
├── Partition 1 ── [offset 0, 1, 2, ...]
└── Partition 2 ── [offset 0, 1, ...]
```

Each record in a partition has a unique integer **offset**. Offsets are per-partition — partition 0 offset 5 is different from partition 1 offset 5.

#### 2. Producers and Consumers

- **Producer**: Publishes messages to a topic partition. Kafka guarantees that all messages in a partition are ordered by their offset.
- **Consumer**: Subscribes to topics and reads messages. Consumers track their position (offset) in each partition independently.

#### 3. Broker and Cluster

A **broker** is a Kafka server that stores data. A **cluster** has multiple brokers. One broker is elected as the **controller** to manage cluster state (like partition leadership).

#### 4. The Write Path (Produce)

```
Producer → TCP Connection → Broker.handleClient()
                            → RequestHeader.parse()
                            → ProduceHandler.handle()
                            → Partition.appendRecordBatch()
                            → LogSegment.appendRaw()
                            → Data written to .log file
                            → Index entry written to .index file
```

1. Producer connects via TCP and sends a framed request: `size(4 bytes) + header + body`
2. Broker parses the Kafka wire protocol (API key 0 for Produce)
3. RecordBatch is validated (minimum size, records_count >= 0)
4. Data is prepended with `base_offset` (8 bytes) and written to disk
5. Broker returns `base_offset` to the producer

#### 5. The Read Path (Fetch)

```
Consumer → FetchRequest (offset, maxBytes)
        → Broker scans log sequentially to find position
        → Returns record batches from that offset
        → Consumer parses RecordBatch headers
        → Extracts key/value from each record
```

#### 6. On-Disk Format (RecordBatch)

Each entry in the log file:

```
base_offset (INT64) + RecordBatch:
  batch_length (INT32)
  partition_leader_epoch (INT32)
  magic (INT8)
  crc (INT32)
  attributes (INT16)
  last_offset_delta (INT32)
  first_timestamp (INT64)
  max_timestamp (INT64)
  producer_id (INT64)
  producer_epoch (INT16)
  base_sequence (INT32)
  records_count (INT32)
  records[] (varint-encoded key/value pairs)
```

#### 7. Kafka Wire Protocol

All communication uses a binary TCP protocol. Key concepts:

- **Framing**: `message_size (INT32)` + `correlation_id (INT32)` + `body`
- **Compact Arrays**: `[count+1 (varint)] [items...]` — count is `length + 1` so null/empty is distinguished
- **Compact Strings**: `[length+1 (varint)] [bytes]` — same null/empty pattern
- **Varints**: Variable-length integer encoding for space efficiency

#### 8. API Keys

| API Key | Name | Purpose |
|---------|------|--------|
| 0 | Produce | Write messages to a topic |
| 1 | Fetch | Read messages from a topic |
| 18 | ApiVersions | Negotiate protocol version |
| 75 | DescribeTopicPartitions | Discover topic metadata |

#### 9. KRaft (取代 ZooKeeper)

KRaft is Kafka's built-in consensus protocol that replaces ZooKeeper. The **controller** is elected among brokers and manages cluster metadata in a special log (`__cluster_metadata`). This log stores:
- **TOPIC_RECORD**: topic name + UUID
- **PARTITION_RECORD**: partition ID, leader, replicas, ISR

---

## Architecture

```
.------------------------.     .--------------------------------.
|   SimpleKafkaBroker    |     |           Handlers             |
|                        |     |  ApiVersions  |  Produce       |
| .--------------------. |     |  Fetch        |  DescribeTopic |  
| |   BrokerInfo       | |     '--------------------------------'  
| |  (id/host/port)    | |
| '--------------------' |     .--------------------------------.
|                        |     |         Partition              |
| .--------------------. |     |  (segments: .log + .index)     |
| |ClusterMetadataStore| |     '--------------------------------'
| | (in-memory cache)  | |
| '--------------------' |     .--------------------------------.
|                        |     |        LogSegment              |
| .--------------------. |     |  (RandomAccessFile+FileChannel)|
| |ClusterMetadataLog  | |     '--------------------------------'
| |(__cluster_metadata)| |
| '--------------------' |
'------------------------'

.------------------------.
|  SimpleKafkaClient     |
| .----------. .--------.|
| | Producer | |Consumer||
| '----------' '--------'|
'------------------------'

.----------------------------------.
|            Shared                |
| RequestHeader | ResponseHeader   |
| Protocol | ErrorCodes            |
| CompactString | CompactArray     |
| Int16 | Int32 | Uuid             |
'----------------------------------'
```
### Layer Descriptions

| Layer | Package | Responsibility |
|-------|---------|----------------|
| **Broker** | `com.simplekafka.broker` | TCP server, virtual threads, request dispatch |
| **Handlers** | `com.simplekafka.broker.handlers` | Protocol-specific request processing |
| **Metadata** | `com.simplekafka.metadata` | Topic/partition registry, KRaft log |
| **Client** | `com.simplekafka.client` | Producer and Consumer APIs |
| **Shared** | `com.simplekafka.shared` | Protocol primitives, error codes, framing |

### Data Flow

```
TCP Connection
    │
    ▼
SimpleKafkaBroker.handleClient()
    │
    ▼
RequestHeader.parse() ──► dispatchRequest(apiKey)
    │
    ├──► ApiVersionsHandler (key 18)
    ├──► DescribeTopicPartitionsHandler (key 75)
    ├──► ProduceHandler (key 0) ──► Partition.appendRecordBatch()
    │                                     │
    │                                     ▼
    │                               LogSegment.appendRaw()
    │                                     │
    │                                     ▼
    │                               FileChannel.write()
    │
    └──► FetchHandler (key 1) ──► Partition.readMessages()
                                      │
                                      ▼
                                LogSegment.read()
```

---

## Features

### Implemented

- **TCP Server** with Java virtual threads (JDK 21+)
- **Connection limiting** via Semaphore (max 1000 concurrent)
- **Max request size** protection (100MB limit to prevent OOM DoS)
- **ApiVersions** — protocol version negotiation (keys 0, 1, 18, 75)
- **DescribeTopicPartitions** — topic metadata discovery
- **Produce** — write RecordBatch to partitioned log
- **Fetch** — read from specific offset with sequential scan
- **KRaft-style metadata** — controller election, `__cluster_metadata` log
- **Segment-based log storage** — `.log` data files + `.index` offset maps
- **Producer API** — `SimpleKafkaProducer.send()`
- **Consumer API** — `SimpleKafkaConsumer.poll()` with offset tracking
- **Protocol correctness** — INT64 for log_start_offset, bounded varint decoding
- **Error codes** — UNKNOWN_TOPIC_OR_PARTITION, UNKNOWN_SERVER_ERROR, UNSUPPORTED_VERSION
- **43 unit tests** covering broker startup, topic discovery, produce, fetch, and end-to-end

### Not Yet Implemented

- Segment rolling (size-based — `SEGMENT_SIZE_LIMIT` is defined but not enforced)
- Index-based offset lookup (currently sequential scan)
- Replication and ISR (In-Sync Replicas)
- Transactional producers
- Partition leader election (all partitions assume current broker is leader)
- Authentication and authorization
- Configurable log directory (hardcoded to `/tmp/kraft-combined-logs`)

---

## Project Structure

```
src/main/java/com/simplekafka/
├── Main.java                      # Entry point
├── Demo.java                      # Demo: produce + consume
│
├── broker/
│   ├── SimpleKafkaBroker.java     # TCP server, virtual threads, dispatch
│   ├── BrokerInfo.java            # Broker metadata (id, host, port)
│   ├── Partition.java              # Partition log management
│   ├── LogSegment.java            # Single segment (.log + .index)
│   └── handlers/
│       ├── ApiVersionsHandler.java     # Protocol negotiation
│       ├── DescribeTopicPartitionsHandler.java
│       ├── FetchHandler.java           # Read records
│       └── ProduceHandler.java        # Write records
│
├── client/
│   ├── SimpleKafkaClient.java     # Low-level NIO TCP client
│   ├── SimpleKafkaProducer.java    # High-level produce API
│   └── SimpleKafkaConsumer.java    # High-level consume API
│
├── metadata/
│   ├── ClusterMetadataStore.java   # In-memory topic/partition registry
│   ├── ClusterMetadataLog.java     # __cluster_metadata log
│   ├── TopicRecord.java            # Topic name + UUID
│   └── PartitionRecord.java        # Partition metadata
│
└── shared/
    ├── ErrorCodes.java             # Kafka error codes
    ├── Protocol.java               # Framing (request/response)
    ├── RequestHeader.java         # API key + version + correlation ID
    ├── ResponseHeader.java        # correlation_id + TAG_BUFFER
    └── primitives/
        ├── CompactString.java     # COMPACT_STRING encoding
        ├── CompactArray.java       # COMPACT_ARRAY encoding
        ├── Int16.java, Int32.java  # Fixed-width integers
        └── Uuid.java              # UUID read/write

src/test/java/com/simplekafka/
├── AbstractBrokerTest.java        # Shared test infrastructure
├── BrokerStartupTest.java         # Epic 1: broker startup + protocol
├── TopicDiscoveryTest.java        # Epic 2: topic metadata
├── ProduceTest.java               # Epic 3: message production
├── FetchTest.java                 # Epic 4: message consumption
├── EndToEndTest.java              # Epic 5: full integration
└── KafkaWireHelpers.java          # Wire protocol test utilities
```

---

## Getting Started

### Prerequisites

- **JDK 21+** (requires virtual threads)
- **Maven 3.9+**

### Build

```bash
mvn compile
```

### Run Broker

```bash
mvn exec:java -Dexec.mainClass=com.simplekafka.Main
```

The broker starts on port 9092 by default, binds as controller, and waits for connections.

### Run Demo

```bash
mvn exec:java -Dexec.mainClass=com.simplekafka.Demo
```

Output:
```
[Producer] Sent to partition 0 at offset 0
[Producer] Sent to partition 0 at offset 1
[Producer] Sent to partition 0 at offset 2
[Producer] Sent to partition 0 at offset 3
[Consumer] Poll 1 batch(es), 4 records: message-0, message-1, message-2, message-3
[Consumer] Done — consumed 4 records
```

---

## Running Tests

```bash
mvn test
```

**43 tests** across 5 test classes:

| Test Class | Tests | Epic |
|------------|-------|------|
| `BrokerStartupTest` | 11 | Epic 1: Broker Startup & Protocol |
| `TopicDiscoveryTest` | 8 | Epic 2: Topic Metadata Discovery |
| `ProduceTest` | 11 | Epic 3: Message Production |
| `FetchTest` | 8 | Epic 4: Message Consumption |
| `EndToEndTest` | 5 | Epic 5: Cluster Management & Integration |

---

## Demo

The `Demo.java` shows a full produce-consume cycle:

1. Clean log directory (prevent stale data from previous runs)
2. Start broker
3. Create topic `"demo-topic"` with 1 partition
4. `SimpleKafkaProducer` sends 4 messages
5. `SimpleKafkaConsumer` polls and receives all 4 messages
6. Output is parsed and printed as readable strings

```java
// Produce
SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", port, "demo");
producer.initialize();
long offset = producer.send("demo-topic", 0, null, "message-0");

// Consume
SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", port, "demo");
consumer.initialize();
consumer.seek(0);
List<byte[]> batches = consumer.poll(topicId, 0, 65536);
String records = new String(batches.get(0), StandardCharsets.UTF_8);
```

---

## Wire Protocol Reference

### Request Framing

```
message_size (INT32) = header_size + body_size
header:
  api_key (INT16)
  api_version (INT16)
  correlation_id (INT32)
  client_id (COMPACT_STRING)
  TAG_BUFFER
body: api-specific
```

### Response Framing

```
message_size (INT32) = header_size + body_size
header:
  correlation_id (INT32)
  TAG_BUFFER
body: api-specific
```

### Key Types

| Type | Encoding |
|------|----------|
| `COMPACT_STRING` | varint(length+1) + bytes |
| `COMPACT_ARRAY` | varint(count+1) + items |
| `UUID` | 16 bytes (msb + lsb) |
| `INT32` | 4 bytes big-endian |
| `INT64` | 8 bytes big-endian |
| `varint` | 7 bits per byte, high bit = continuation |

---

## Error Codes

| Code | Name | When |
|------|------|------|
| 0 | NONE | Success |
| -1 | UNKNOWN_SERVER_ERROR | I/O failures, unexpected errors |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Topic/partition does not exist |
| 35 | UNSUPPORTED_VERSION | Requested API version out of range |
| 100 | UNKNOWN_TOPIC_ID | Topic UUID not found in metadata |