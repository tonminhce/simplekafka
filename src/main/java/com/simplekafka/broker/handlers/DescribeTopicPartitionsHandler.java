package com.simplekafka.broker.handlers;

import com.simplekafka.metadata.ClusterMetadataStore;
import com.simplekafka.metadata.PartitionRecord;
import com.simplekafka.metadata.TopicRecord;
import com.simplekafka.shared.ErrorCodes;
import com.simplekafka.shared.Protocol;
import com.simplekafka.shared.RequestHeader;
import com.simplekafka.shared.ResponseHeader;
import com.simplekafka.shared.primitives.CompactArray;
import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int16;
import com.simplekafka.shared.primitives.Int32;
import com.simplekafka.shared.primitives.Uuid;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles DescribeTopicPartitions requests (API key 75).
 * <p>
 * Supports:
 * - Unknown topics (error code 3)
 * - Existing topic metadata with UUID and partitions
 * - Multi-partition topics
 * - Alphabetical sorting of topics
 */
public class DescribeTopicPartitionsHandler {

    private final ClusterMetadataStore metadataStore;

    public DescribeTopicPartitionsHandler(ClusterMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Processes a DescribeTopicPartitions request and returns the framed response.
     *
     * @param header      the parsed request header
     * @param requestBody the remaining body bytes after the header
     * @return ByteBuffer ready to be written to the channel
     */
    public ByteBuffer handle(RequestHeader header, ByteBuffer requestBody) {
        List<String> requestedTopics = parseRequestedTopics(requestBody);
        // Skip cursor and TAG_BUFFER at end of request body.
        ByteBuffer body = buildResponseBody(requestedTopics);
        ResponseHeader responseHeader = new ResponseHeader(header.getCorrelationId());
        return Protocol.frameResponse(responseHeader, body);
    }

    /**
     * Parses the topic names from the request body.
     * <p>
     * Request body layout (after header):
     * topics_count (COMPACT_ARRAY length)
     * topics[]: topic_name (COMPACT_STRING) + TAG_BUFFER
     * response_cursor (COMPACT_NULLABLE_STRING)
     * TAG_BUFFER
     */
    private List<String> parseRequestedTopics(ByteBuffer body) {
        int topicCount = CompactArray.readCount(body);
        List<String> topics = new ArrayList<>(topicCount);

        for (int i = 0; i < topicCount; i++) {
            String topicName = CompactString.read(body);
            topics.add(topicName);
            // Skip per-topic TAG_BUFFER.
            skipTaggedFields(body);
        }

        // Read response_cursor (nullable compact string).
        CompactString.read(body);

        // Skip trailing TAG_BUFFER.
        skipTaggedFields(body);

        return topics;
    }

    /**
     * Builds the DescribeTopicPartitions response body.
     * <p>
     * Layout:
     * throttle_time_ms (INT32)
     * topics_count (COMPACT_ARRAY length)
     * topics[]:
     *   error_code (INT16)
     *   topic_id (UUID 16 bytes)
     *   topic_name (COMPACT_STRING)
     *   partitions_count (COMPACT_ARRAY length)
     *   partitions[]:
     *     error_code (INT16)
     *     partition_index (INT32)
     *     leader_id (INT32)
     *     leader_epoch (INT32)
     *     replica_nodes (COMPACT_ARRAY[INT32])
     *     isr_nodes (COMPACT_ARRAY[INT32])
     *     TAG_BUFFER
     *   topic_authorized_operations (INT32)
     *   TAG_BUFFER
     * next_cursor (COMPACT_NULLABLE_STRING)
     * TAG_BUFFER
     */
    private ByteBuffer buildResponseBody(List<String> requestedTopics) {
        // Sort requested topics alphabetically (Story 2.4).
        List<String> sortedTopics = new ArrayList<>(requestedTopics);
        sortedTopics.sort(String::compareTo);

        // First pass: compute size.
        int size = computeBodySize(sortedTopics);
        ByteBuffer body = ByteBuffer.allocate(size);

        // throttle_time_ms
        Int32.write(body, 0);

        // topics array
        CompactArray.writeCount(body, sortedTopics.size());
        for (String topicName : sortedTopics) {
            writeTopicEntry(body, topicName);
        }

        // next_cursor (null)
        CompactString.write(body, null);

        // trailing TAG_BUFFER
        Protocol.writeEmptyTagBuffer(body);

        body.flip();
        return body;
    }

    /**
     * Writes a single topic entry into the response buffer.
     */
    private void writeTopicEntry(ByteBuffer buf, String topicName) {
        TopicRecord topic = metadataStore.getTopic(topicName);

        if (topic == null) {
            // Unknown topic (Story 2.1).
            Int16.write(buf, ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION);
            Uuid.writeZero(buf);
            CompactString.write(buf, topicName);
            CompactArray.writeCount(buf, 0); // empty partitions
            Int32.write(buf, 0); // topic_authorized_operations
            Protocol.writeEmptyTagBuffer(buf);
        } else {
            // Existing topic (Stories 2.2, 2.3).
            Int16.write(buf, ErrorCodes.NONE);
            Uuid.write(buf, topic.getTopicId());
            CompactString.write(buf, topic.getTopicName());

            List<PartitionRecord> partitions = metadataStore.getPartitions(topic.getTopicId());
            CompactArray.writeCount(buf, partitions.size());

            for (PartitionRecord partition : partitions) {
                writePartitionEntry(buf, partition);
            }

            Int32.write(buf, 0); // topic_authorized_operations
            Protocol.writeEmptyTagBuffer(buf);
        }
    }

    /**
     * Writes a single partition entry into the response buffer.
     */
    private void writePartitionEntry(ByteBuffer buf, PartitionRecord partition) {
        Int16.write(buf, ErrorCodes.NONE); // error_code
        Int32.write(buf, partition.getPartitionId()); // partition_index
        Int32.write(buf, partition.getLeaderId()); // leader_id
        Int32.write(buf, partition.getLeaderEpoch()); // leader_epoch

        // replica_nodes (COMPACT_ARRAY of INT32)
        List<Integer> replicas = partition.getReplicas();
        CompactArray.writeCount(buf, replicas.size());
        for (int replica : replicas) {
            Int32.write(buf, replica);
        }

        // isr_nodes (COMPACT_ARRAY of INT32)
        List<Integer> isr = partition.getIsr();
        CompactArray.writeCount(buf, isr.size());
        for (int node : isr) {
            Int32.write(buf, node);
        }

        Protocol.writeEmptyTagBuffer(buf); // TAG_BUFFER per partition
    }

    private int computeBodySize(List<String> topics) {
        int size = Int32.size(); // throttle_time_ms
        size += CompactArray.sizeCount(topics.size());

        for (String topicName : topics) {
            size += computeTopicEntrySize(topicName);
        }

        size += 1; // next_cursor (null = 1 byte for 0)
        size += 1; // trailing TAG_BUFFER
        return size;
    }

    private int computeTopicEntrySize(String topicName) {
        int size = Int16.size(); // error_code
        size += Uuid.size(); // topic_id
        size += CompactString.size(topicName); // topic_name

        TopicRecord topic = metadataStore.getTopic(topicName);
        if (topic == null) {
            size += CompactArray.sizeCount(0); // partitions
            size += Int32.size(); // topic_authorized_operations
            size += 1; // TAG_BUFFER
        } else {
            List<PartitionRecord> partitions = metadataStore.getPartitions(topic.getTopicId());
            size += CompactArray.sizeCount(partitions.size());
            for (PartitionRecord p : partitions) {
                size += computePartitionEntrySize(p);
            }
            size += Int32.size(); // topic_authorized_operations
            size += 1; // TAG_BUFFER
        }
        return size;
    }

    private int computePartitionEntrySize(PartitionRecord partition) {
        int size = Int16.size(); // error_code
        size += Int32.size(); // partition_index
        size += Int32.size(); // leader_id
        size += Int32.size(); // leader_epoch
        size += CompactArray.sizeCount(partition.getReplicas().size()) + partition.getReplicas().size() * Int32.size();
        size += CompactArray.sizeCount(partition.getIsr().size()) + partition.getIsr().size() * Int32.size();
        size += 1; // TAG_BUFFER
        return size;
    }

    /**
     * Skips a TAG_BUFFER (tagged fields) section.
     */
    private void skipTaggedFields(ByteBuffer buffer) {
        int numTags = CompactString.readUnsignedVarint(buffer);
        for (int i = 0; i < numTags; i++) {
            CompactString.readUnsignedVarint(buffer); // tagId
            int tagSize = CompactString.readUnsignedVarint(buffer);
            buffer.position(buffer.position() + tagSize);
        }
    }
}
