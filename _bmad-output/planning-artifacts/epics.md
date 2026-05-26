---
stepsCompleted: ["step-01-validate-prerequisites", "step-02-design-epics"]
inputDocuments: ["IMPLEMENTATION_PLAN.md"]
---

# simplekafka - Epic Breakdown

## Overview

This document provides the complete epic and story breakdown for simplekafka, decomposing the requirements from IMPLEMENTATION_PLAN.md into implementable stories.

## Requirements Inventory

### Functional Requirements

FR1: Broker must respond to ApiVersions request with supported API versions (API key 18, versions 0-4)
FR2: Broker must respond to ApiVersions request with UNSUPPORTED_VERSION error (35) for unknown API keys
FR3: Broker must parse request header v2 (api_key, api_version, correlation_id, client_id, TAG_BUFFER)
FR4: Broker must handle multiple concurrent client connections simultaneously
FR5: Broker must handle multiple sequential requests on the same TCP connection
FR6: Broker must include DescribeTopicPartitions (API key 75, version 0) in ApiVersions response
FR7: Broker must return UNKNOWN_TOPIC_OR_PARTITION error (3) for unknown topic names in DescribeTopicPartitions
FR8: Broker must return topic metadata (topic_id as UUID, partition info) for existing topics from __cluster_metadata
FR9: Broker must return metadata for multi-partition topics with sorted partition entries
FR10: Broker must return topics sorted alphabetically in DescribeTopicPartitions response
FR11: Broker must return empty responses array in Fetch response for topics with no data
FR12: Broker must return UNKNOWNTOPICID error (100) for unknown topic_id in Fetch request
FR13: Broker must return empty records array for topics with no messages in Fetch response
FR14: Broker must read RecordBatch from partition log file for Fetch request with valid topic/partition
FR15: Broker must include Produce (API key 0, versions 0-11) in ApiVersions response
FR16: Broker must return UNKNOWN_TOPIC_OR_PARTITION error (3) for invalid topic/partition in Produce request
FR17: Broker must validate topic/partition exists via ClusterMetadataStore before Produce
FR18: Broker must write single RecordBatch to partition log file on Produce
FR19: Broker must write multiple records in one RecordBatch to partition log
FR20: Broker must handle Produce request with multiple partitions of the same topic
FR21: Broker must handle Produce request with multiple topics
FR22: Broker must persist topic/partition creation records to __cluster_metadata log
FR23: Broker must implement KRaft-style controller election
FR24: Client must send and receive messages end-to-end (producer/consumer)

### NonFunctional Requirements

NFR1: Pure Java implementation with no external dependencies (only Java standard library)
NFR2: Use NIO SocketChannel for non-blocking I/O
NFR3: Segment-based log storage with index files for offset-to-position lookup
NFR4: Binary wire format using ByteBuffer for all protocol encoding/decoding
NFR5: KRaft mode replaces ZooKeeper for cluster metadata management
NFR6: __cluster_metadata topic stored at /tmp/kraft-combined-logs/__cluster_metadata-0/
NFR7: Partition logs stored at <log-dir>/<topic-name>-<partition-index>/00000000000000000000.log
NFR8: Request header v2 format with TAG_BUFFER for forward compatibility
NFR9: Response header v1 format with TAG_BUFFER

### Additional Requirements

- Implement Layer 0: Protocol primitives (Int16, Int32, CompactString, Uuid, CompactArray) and standard error codes
- Implement Layer 1: Protocol & data models (RequestHeader, ResponseHeader, Protocol, BrokerInfo)
- Implement Layer 2: Cluster metadata store (__cluster_metadata topic, TopicRecord, PartitionRecord, ClusterMetadataStore)
- Implement Layer 3: Storage layer (Partition class with segment-based log management, LogSegment)
- Implement Layer 4: Request handlers (ApiVersionsHandler, DescribeTopicPartitionsHandler, FetchHandler, ProduceHandler)
- Implement Layer 5: Broker server (SimpleKafkaBroker with start(), handleClientConnection(), electController())
- Implement Layer 6: Client library (SimpleKafkaClient, SimpleKafkaProducer, SimpleKafkaConsumer)
- Binary protocol format: message_size(4 bytes) + header + body
- On-disk log format: <log-dir>/<topic-name>-<partition-index>/00000000000000000000.log

### UX Design Requirements

(no UX design document - pure backend messaging system)

### FR Coverage Map

(to be filled during epic design)

## Epic List

### Epic 1: Broker Startup & Protocol Negotiation
Users can start the broker and clients can query supported API versions.

**FRs covered:** FR1, FR2, FR3, FR4, FR5, FR6

### Epic 2: Topic Metadata Discovery
Clients can discover topics, partition assignments, and broker leadership.

**FRs covered:** FR7, FR8, FR9, FR10

### Epic 3: Message Production
Clients can produce and persist messages to topics and partitions.

**FRs covered:** FR15, FR16, FR17, FR18, FR19, FR20, FR21

### Epic 4: Message Consumption
Clients can fetch and consume messages at specific offsets from disk.

**FRs covered:** FR11, FR12, FR13, FR14

### Epic 5: Cluster Management & Integration
Topics persist with creation records, controller elected via KRaft, end-to-end producer/consumer works.

**FRs covered:** FR22, FR23, FR24

### FR Coverage Map

FR1: Epic 1 - Broker responds to ApiVersions with supported API versions
FR2: Epic 1 - Broker returns UNSUPPORTED_VERSION error for unknown API keys
FR3: Epic 1 - Broker parses request header v2 with correlation_id and client_id
FR4: Epic 1 - Broker handles multiple concurrent client connections
FR5: Epic 1 - Broker handles multiple sequential requests on same connection
FR6: Epic 1 - Broker includes DescribeTopicPartitions (key 75) in ApiVersions response
FR7: Epic 2 - Broker returns UNKNOWN_TOPIC_OR_PARTITION error for unknown topics
FR8: Epic 2 - Broker returns topic metadata from __cluster_metadata store
FR9: Epic 2 - Broker returns metadata for multi-partition topics
FR10: Epic 2 - Broker returns topics sorted alphabetically
FR11: Epic 4 - Broker returns empty responses for topics with no data
FR12: Epic 4 - Broker returns UNKNOWNTOPICID error for unknown topic_id
FR13: Epic 4 - Broker returns empty records for topics with no messages
FR14: Epic 4 - Broker reads RecordBatch from partition log file
FR15: Epic 3 - Broker includes Produce (key 0) in ApiVersions response
FR16: Epic 3 - Broker returns error for invalid topic/partition in Produce
FR17: Epic 3 - Broker validates topic/partition via ClusterMetadataStore
FR18: Epic 3 - Broker writes single RecordBatch to partition log
FR19: Epic 3 - Broker writes multiple records in one RecordBatch
FR20: Epic 3 - Broker handles Produce with multiple partitions
FR21: Epic 3 - Broker handles Produce with multiple topics
FR22: Epic 5 - Broker persists topic/partition creation records to __cluster_metadata
FR23: Epic 5 - Broker implements KRaft-style controller election
FR24: Epic 5 - End-to-end producer/consumer works
