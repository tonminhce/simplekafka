# Build Your Own Kafka - Implementation Plan (Java 25)

## Project Overview
A simplified Kafka-like distributed messaging system built from scratch in pure Java.
Based on: Sample code + 24-step Kafka challenge (KRaft mode)

---

## Architecture Layers

### Layer 0: Protocol Primitives & Error Codes (NEW)
**Purpose:** Low-level protocol parsing primitives and standard error codes

| File | Description |
|------|-------------|
| `com/simplekafka/protocol/ErrorCodes.java` | Standard Kafka error codes |
| | `UNKNOWN_TOPIC_OR_PARTITION = 3` |
| | `UNSUPPORTED_VERSION = 35` |
| | `UNKNOWNTOPICID = 100` |
| `com/simplekafka/protocol/primitives/` | Primitive type parsers |
| | `Int16`, `Int32`, `CompactString`, `Uuid`, `CompacetArray`... |

### Layer 1: Protocol & Data Models
**Purpose:** Binary wire format encoding/decoding and shared value objects

| File | Description |
|------|-------------|
| `com/simplekafka/broker/BrokerInfo.java` | Immutable value object: id, host, port |
| `com/simplekafka/protocol/RequestHeader.java` | Request header v2: api_key, api_version, correlation_id, client_id, TAG_BUFFER |
| `com/simplekafka/protocol/ResponseHeader.java` | Response header v0 (correlation_id only) and v1 (+ TAG_BUFFER) |
| `com/simplekafka/protocol/Protocol.java` | ByteBuffer serialization for all message types |

**API Keys (for ApiVersions response):**
- API key 18: ApiVersions (min: 0, max: 4)
- API key 75: DescribeTopicPartitions (min: 0, max: 0)
- API key 0: Produce (min: 0, max: 11)
- API key 1: Fetch (min: 0, max: 16)

### Layer 2: Cluster Metadata Store (NEW - KRaft Mode)
**Purpose:** __cluster_metadata topic for topic/partition metadata (KRaft取代ZooKeeper)

| File | Description |
|------|-------------|
| `com/simplekafka/metadata/ClusterMetadataLog.java` | Read/write __cluster_metadata log file |
| | Path: `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log` |
| `com/simplekafka/metadata/TopicRecord.java` | TOPIC_RECORD payload parsing |
| `com/simplekafka/metadata/PartitionRecord.java` | PARTITION_RECORD payload parsing |
| | Fields: partition_id, topic_id, topic_name, leader, replicas, isr |
| `com/simplekafka/metadata/ClusterMetadataStore.java` | In-memory cache of cluster metadata |
| | `loadMetadata()` - Load from log file |
| | `getTopic(topicName)` - Get topic metadata |
| | `getTopicById(uuid)` - Get topic by UUID |
| | `getPartition(topicId, partitionIndex)` - Get partition metadata |

### Layer 3: Storage Layer
**Purpose:** Partition log management with segment-based storage and index-based offset lookup

| File | Key Classes/Functions |
|------|---------------------|
| `com/simplekafka/broker/Partition.java` | `Partition` class with segment-based log management |
| | `append()` - Append message, create new segment when full, update index |
| | `appendRecordBatch()` - Write RecordBatch directly to log file |
| | `readMessages()` - Read from offset up to maxBytes using binary search |
| | `findSegmentForOffset()` - Binary search to locate segment by offset |
| | `findPositionForOffset()` - Index-based byte position lookup |
| | `initialize()` - Load existing segments from disk |
| | `createNewSegment()` - Create log/index file pair |
| `com/simplekafka/broker/LogSegment.java` | Individual log segment (log + index file pair) |

### Layer 4: Request Handling (NEW - More detailed)
**Purpose:** Request routing and handling

| File | Key Functions |
|------|---------------|
| `com/simplekafka/broker/RequestHandler.java` | Route requests by api_key to appropriate handler |
| `com/simplekafka/broker/handlers/ApiVersionsHandler.java` | Return supported API versions |
| `com/simplekafka/broker/handlers/DescribeTopicPartitionsHandler.java` | Return topic metadata |
| | Handle unknown topics (error 3), single topic, multi-topic |
| | Topics must be sorted alphabetically |
| `com/simplekafka/broker/handlers/FetchHandler.java` | Handle Fetch requests |
| | Parse FetchRequest v16 (topic_id, partition_index, current_leader_epoch) |
| | Return FetchResponse v16 with records from disk |
| `com/simplekafka/broker/handlers/ProduceHandler.java` | Handle Produce requests |
| | Parse ProduceRequest v11 (RecordBatch with records) |
| | Validate topic/partition exists via ClusterMetadataStore |
| | Write RecordBatch to partition log file |
| | Assign offsets (base_offset per batch) |

### Layer 5: Broker Server
**Purpose:** Main broker server handling client connections

| File | Key Functions |
|------|---------------|
| `com/simplekafka/broker/SimpleKafkaBroker.java` | `SimpleKafkaBroker` - main entry point |
| | `start()` - Initialize NIO server socket, load metadata, accept connections |
| | `handleClientConnection()` - Process client requests |
| | `electController()` - KRaft-style controller election |
| | `loadTopic()` - Load topic from ClusterMetadataStore |
| | `handleProduceRequest()` - Append to leader, replicate to followers |
| | `handleFetchRequest()` - Read messages at offset |
| | `handleMetadataRequest()` - Return topic/partition/broker info |
| | `handleCreateTopicRequest()` - Create partitions with assignment |
| | `createTopic()` - Create topic, notify brokers |

### Layer 6: Client Library
**Purpose:** High-level producer/consumer APIs

| File | Key Functions |
|------|---------------|
| `com/simplekafka/client/SimpleKafkaClient.java` | NIO SocketChannel client |
| | `refreshMetadata()` - Connect to broker, fetch cluster metadata |
| | `createTopic()` - Create topic with partition count/replication factor |
| | `send()` - Send message to topic/partition via leader |
| | `fetch()` - Fetch messages from topic/partition at offset |
| | `getTopicMetadata()` - Return cached topic metadata |
| `com/simplekafka/client/SimpleKafkaProducer.java` | `SimpleKafkaProducer` |
| | `initialize()` - Connect to broker, optionally create topic |
| | `send()` - Send to random partition |
| `com/simplekafka/client/SimpleKafkaConsumer.java` | `SimpleKafkaConsumer` |
| | `initialize()` - Connect to broker |
| | `poll()` - Poll broker for messages |
| | `startConsuming()` - Background thread with handler callback |
| | `seek()` - Seek to offset |

---

## Key Concepts

### 1. Protocol Format (ByteBuffer - Kafka Wire)
**Request:**
```
message_size (4 bytes) + header + body
```
**Request Header v2:**
```
api_key: INT16 (2 bytes)
api_version: INT16 (2 bytes)
correlation_id: INT32 (4 bytes)
client_id: COMPACT_STRING (variable)
TAG_BUFFER: TAGGED_FIELDS (variable)
```
**Response Header v1:**
```
correlation_id: INT32 (4 bytes)
TAG_BUFFER: TAGGED_FIELDS (variable)
```

### 2. Error Codes (Standard Kafka)
```
0  = NO_ERROR
3  = UNKNOWN_TOPIC_OR_PARTITION
35 = UNSUPPORTED_VERSION
100 = UNKNOWNTOPICID
```

### 3. DescribeTopicPartitions Response
- **Topics sorted alphabetically**
- **Topic ID = UUID (16 bytes)** not topic name
- **Partition metadata:** partition_index, leader_id, leader_epoch, replica_nodes, isr_nodes
- **Unknown topic:** error_code = 3, topic_id = zeros, partitions = empty

### 4. Fetch Request/Response (v16)
**Fetch Request:**
- topic_id: UUID (16 bytes)
- partition_index: INT32
- current_leader_epoch: INT32

**Fetch Response:**
- throttle_time_ms: INT32
- responses[]: per-topic results with records[]

### 5. Produce Request/Response (v11)
**Produce Request:**
- RecordBatch containing multiple records
- Each RecordBatch written directly to partition log file
- base_offset assigned sequentially per batch

**On-disk format:**
```
<log-dir>/<topic-name>-<partition-index>/00000000000000000000.log
```

### 6. Cluster Metadata Log (__cluster_metadata)
**Location:** `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`

**Record types:**
- TOPIC_RECORD: topic_name, topic_id (UUID)
- PARTITION_RECORD: partition_id, topic_id, leader, replicas, isr, etc.

---

## Implementation Order (24-Step Challenge Mapping)

### Phase 1: Basic Server & Protocol (Steps 1-6)
1. **Response with hardcoded correlation_id** (Step 7)
    - Send 8 bytes: message_size(4) + correlation_id=7(4)

2. **Parse request correlation_id** (Step 2-3)
    - Extract from request header v2

3. **ApiVersions response with error_code** (Step 3-4)
    - Handle UNSUPPORTED_VERSION (35)

4. **Concurrent requests handling** (Step 5)
    - Multiple clients connecting simultaneously

5. **Multiple sequential requests on same connection** (Step 6)
    - Keep connection alive, handle multiple requests

### Phase 2: DescribeTopicPartitions (Steps 7-12)
6. **ApiVersions entry for DescribeTopicPartitions** (Step 1, 7)
    - Add API key 75 to api_keys array

7. **DescribeTopicPartitions for unknown topic** (Step 8)
    - error_code = 3 (UNKNOWN_TOPIC_OR_PARTITION)
    - Response header v1 with TAG_BUFFER

8. **DescribeTopicPartitions for existing topic** (Step 9)
    - Read __cluster_metadata log
    - Return topic_id (UUID), partition info

9. **Multi-partition topic** (Step 10)
    - Multiple partition entries in response

10. **Multi-topic request (sorted alphabetically)** (Step 11)
    - Multiple topics, sorted by name

### Phase 3: Fetch API (Steps 12-17)
11. **Fetch response - empty topics** (Step 13)
    - Return empty responses array

12. **Fetch response - unknown topic** (Step 14)
    - error_code = 100 (UNKNOWNTOPICID)

13. **Fetch response - topic with no messages** (Step 15)
    - Empty records array

14. **Fetch response - read from disk** (Step 16-17)
    - Read RecordBatch from partition log file

### Phase 4: Produce API (Steps 18-24)
15. **ApiVersions entry for Produce** (Step 18)
    - Add API key 0 to api_keys array

16. **Produce response - invalid topic/partition** (Step 19)
    - error_code = 3

17. **Produce response - valid topic/partition** (Step 20)
    - Validate via __cluster_metadata

18. **Produce single record to disk** (Step 21)
    - Write RecordBatch to partition log

19. **Produce multiple records** (Step 22)
    - Multiple records in one batch

20. **Produce multiple partitions of same topic** (Step 23)
    - Multiple partition entries in response

21. **Produce multiple topics** (Step 24)
    - Multiple topics in one request

### Phase 5: Full System Integration
22. **Topic/Partition creation records** in metadata log
23. **Controller election** (KRaft-style)
24. **Client producer/consumer** end-to-end

---

## Testing

### Compile
```bash
mvn clean package
```

### Run Broker
```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker /tmp/server.properties
```

### Test with nc
```bash
# ApiVersions request
echo -n "0000001a0012000467890abc00096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C

# DescribeTopicPartitions request
echo -n "00000020004b00000000000700096b61666b612d636c69000204666f6f0000000064ff00" | xxd -r -p | nc localhost 9092 | hexdump -C
```

---

## File Structure (After Implementation)

```
com/simplekafka/
├── protocol/
│   ├── ErrorCodes.java
│   ├── RequestHeader.java
│   ├── ResponseHeader.java
│   ├── primitives/
│   │   ├── Int16.java
│   │   ├── Int32.java
│   │   ├── CompactString.java
│   │   ├── Uuid.java
│   │   └── CompactArray.java
│   └── Protocol.java
├── metadata/
│   ├── ClusterMetadataLog.java
│   ├── TopicRecord.java
│   ├── PartitionRecord.java
│   └── ClusterMetadataStore.java
├── broker/
│   ├── BrokerInfo.java
│   ├── Partition.java
│   ├── LogSegment.java
│   ├── ZookeeperClient.java
│   ├── SimpleKafkaBroker.java
│   └── handlers/
│       ├── ApiVersionsHandler.java
│       ├── DescribeTopicPartitionsHandler.java
│       ├── FetchHandler.java
│       └── ProduceHandler.java
└── client/
    ├── SimpleKafkaClient.java
    ├── SimpleKafkaProducer.java
    └── SimpleKafkaConsumer.java
```