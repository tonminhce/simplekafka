package com.simplekafka.broker.handlers;

import com.simplekafka.broker.Partition;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles Produce requests (API key 0, version 11).
 * <p>
 * Supports:
 * - Invalid topic/partition errors (error code 3)
 * - Topic/partition validation via ClusterMetadataStore
 * - Writing single and multiple RecordBatch entries to disk
 * - Multiple partitions per topic
 * - Multiple topics per request
 */
public class ProduceHandler {

    private static final Logger LOGGER = Logger.getLogger(ProduceHandler.class.getName());
    private static final String LOG_DIR = "/tmp/kraft-combined-logs";

    private final ClusterMetadataStore metadataStore;
    private final Map<String, Partition> partitions = new HashMap<>();

    public ProduceHandler(ClusterMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Processes a Produce request and returns the framed response.
     *
     * @param header      the parsed request header
     * @param requestBody the remaining body bytes after the header
     * @return ByteBuffer ready to be written to the channel
     */
    public ByteBuffer handle(RequestHeader header, ByteBuffer requestBody) {
        // Parse ProduceRequest v11.
        CompactString.read(requestBody); // transactional_id (nullable)
        short acks = Int16.read(requestBody);
        int timeoutMs = Int32.read(requestBody);

        // Parse topics array.
        int topicCount = CompactArray.readCount(requestBody);
        List<TopicProduceResult> results = new ArrayList<>();

        for (int t = 0; t < topicCount; t++) {
            String topicName = CompactString.read(requestBody);
            skipTaggedFields(requestBody); // per-topic TAG_BUFFER

            // Parse partitions array.
            int partitionCount = CompactArray.readCount(requestBody);
            List<PartitionProduceResult> partitionResults = new ArrayList<>();

            for (int p = 0; p < partitionCount; p++) {
                int partitionIndex = Int32.read(requestBody);
                // Read the RecordBatch as raw bytes.
                // The record_batch is a COMPACT_BYTES field: varint length + bytes.
                byte[] recordBatch = readCompactBytes(requestBody);
                skipTaggedFields(requestBody); // per-partition TAG_BUFFER

                PartitionProduceResult result = produceToPartition(topicName, partitionIndex, recordBatch);
                partitionResults.add(result);
            }

            results.add(new TopicProduceResult(topicName, partitionResults));
        }

        // Build response.
        ByteBuffer body = buildResponseBody(results);
        ResponseHeader responseHeader = new ResponseHeader(header.getCorrelationId());
        return Protocol.frameResponse(responseHeader, body);
    }

    /**
     * Produces a record batch to the specified topic-partition.
     * Validates topic and partition existence before writing.
     */
    private PartitionProduceResult produceToPartition(String topicName, int partitionIndex, byte[] recordBatch) {
        // Validate topic exists.
        TopicRecord topic = metadataStore.getTopic(topicName);
        if (topic == null) {
            return new PartitionProduceResult(partitionIndex, ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION, -1);
        }

        // Validate partition exists.
        PartitionRecord partitionMeta = metadataStore.getPartition(topic.getTopicId(), partitionIndex);
        if (partitionMeta == null) {
            return new PartitionProduceResult(partitionIndex, ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION, -1);
        }

        // Get or create the Partition object.
        String partitionKey = topicName + "-" + partitionIndex;
        Partition partition = partitions.get(partitionKey);
        if (partition == null) {
            try {
                partition = new Partition(topicName, partitionIndex, LOG_DIR);
                partitions.put(partitionKey, partition);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to create partition " + partitionKey, e);
                return new PartitionProduceResult(partitionIndex, ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION, -1);
            }
        }

        // Write the RecordBatch.
        try {
            long baseOffset = partition.appendRecordBatch(recordBatch);
            return new PartitionProduceResult(partitionIndex, ErrorCodes.NONE, baseOffset);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to write to partition " + partitionKey, e);
            return new PartitionProduceResult(partitionIndex, ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION, -1);
        }
    }

    /**
     * Reads a COMPACT_BYTES field (varint length + bytes).
     */
    private byte[] readCompactBytes(ByteBuffer buf) {
        int length = CompactString.readUnsignedVarint(buf) - 1;
        if (length <= 0) {
            return new byte[0];
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return bytes;
    }

    /**
     * Builds the Produce response body.
     * <p>
     * Layout:
     * topics_count (COMPACT_ARRAY length)
     * topics[]:
     *   topic_name (COMPACT_STRING)
     *   partitions_count (COMPACT_ARRAY length)
     *   partitions[]:
     *     partition_index (INT32)
     *     error_code (INT16)
     *     base_offset (INT64)
     *     TAG_BUFFER
     *   TAG_BUFFER
     * TAG_BUFFER
     */
    private ByteBuffer buildResponseBody(List<TopicProduceResult> results) {
        int size = computeBodySize(results);
        ByteBuffer body = ByteBuffer.allocate(size);

        // topics array
        CompactArray.writeCount(body, results.size());
        for (TopicProduceResult topicResult : results) {
            CompactString.write(body, topicResult.topicName);

            CompactArray.writeCount(body, topicResult.partitionResults.size());
            for (PartitionProduceResult partResult : topicResult.partitionResults) {
                Int32.write(body, partResult.partitionIndex);
                Int16.write(body, partResult.errorCode);
                body.putLong(partResult.baseOffset);
                Protocol.writeEmptyTagBuffer(body); // per-partition TAG_BUFFER
            }

            Protocol.writeEmptyTagBuffer(body); // per-topic TAG_BUFFER
        }

        Protocol.writeEmptyTagBuffer(body); // trailing TAG_BUFFER

        body.flip();
        return body;
    }

    private int computeBodySize(List<TopicProduceResult> results) {
        int size = CompactArray.sizeCount(results.size());
        for (TopicProduceResult topicResult : results) {
            size += CompactString.size(topicResult.topicName);
            size += CompactArray.sizeCount(topicResult.partitionResults.size());
            for (PartitionProduceResult ignored : topicResult.partitionResults) {
                size += Int32.size() + Int16.size() + 8 + 1; // partitionIndex + errorCode + baseOffset + TAG
            }
            size += 1; // per-topic TAG_BUFFER
        }
        size += 1; // trailing TAG_BUFFER
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

    /**
     * Gets a Partition by topic name and partition index (for use by FetchHandler).
     */
    public Partition getPartition(String topicName, int partitionIndex) {
        return partitions.get(topicName + "-" + partitionIndex);
    }

    /**
     * Result of producing to a single partition.
     */
    record PartitionProduceResult(int partitionIndex, short errorCode, long baseOffset) {
    }

    /**
     * Result of producing to all partitions of a topic.
     */
    record TopicProduceResult(String topicName, List<PartitionProduceResult> partitionResults) {
    }
}
