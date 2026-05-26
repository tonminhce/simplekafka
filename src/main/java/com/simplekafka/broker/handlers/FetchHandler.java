package com.simplekafka.broker.handlers;

import com.simplekafka.broker.Partition;
import com.simplekafka.metadata.ClusterMetadataStore;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles Fetch requests (API key 1, version 16).
 * <p>
 * Supports:
 * - Empty responses (no data for the topic)
 * - Unknown topic_id errors (error code 100)
 * - Empty records at offset past data
 * - Reading records from disk at a specific offset
 */
public class FetchHandler {

    private static final Logger LOGGER = Logger.getLogger(FetchHandler.class.getName());

    private final ClusterMetadataStore metadataStore;
    private final ProduceHandler produceHandler;

    public FetchHandler(ClusterMetadataStore metadataStore, ProduceHandler produceHandler) {
        this.metadataStore = metadataStore;
        this.produceHandler = produceHandler;
    }

    /**
     * Processes a Fetch request and returns the framed response.
     *
     * @param header      the parsed request header
     * @param requestBody the remaining body bytes after the header
     * @return ByteBuffer ready to be written to the channel
     */
    public ByteBuffer handle(RequestHeader header, ByteBuffer requestBody) {
        // Parse FetchRequest v16.
        int clusterId = Int32.read(requestBody); // cluster_id (nullable INT32, -1 for null)
        int sessionId = Int32.read(requestBody); // session_id
        int topicCount = CompactArray.readCount(requestBody);

        List<FetchTopicResult> results = new ArrayList<>();

        for (int t = 0; t < topicCount; t++) {
            UUID topicId = Uuid.read(requestBody); // topic_id
            int partitionCount = CompactArray.readCount(requestBody);

            List<FetchPartitionResult> partitionResults = new ArrayList<>();

            for (int p = 0; p < partitionCount; p++) {
                int partitionIndex = Int32.read(requestBody);
                int currentLeaderEpoch = Int32.read(requestBody);
                long fetchOffset = requestBody.getLong(); // fetch_offset
                int lastFetchedEpoch = Int32.read(requestBody); // last_fetched_epoch
                int logStartOffset = Int32.read(requestBody); // log_start_offset
                int partitionMaxBytes = Int32.read(requestBody); // partition_max_bytes

                FetchPartitionResult result = fetchFromPartition(topicId, partitionIndex, fetchOffset, partitionMaxBytes);
                partitionResults.add(result);

                skipTaggedFields(requestBody);
            }

            skipTaggedFields(requestBody);
            results.add(new FetchTopicResult(topicId, partitionResults));
        }

        // Build response.
        ByteBuffer body = buildResponseBody(results);
        ResponseHeader responseHeader = new ResponseHeader(header.getCorrelationId());
        return Protocol.frameResponse(responseHeader, body);
    }

    /**
     * Fetches records from a specific topic-partition.
     */
    private FetchPartitionResult fetchFromPartition(UUID topicId, int partitionIndex, long fetchOffset, int maxBytes) {
        // Look up topic by UUID (Story 4.2).
        TopicRecord topic = metadataStore.getTopicById(topicId);
        if (topic == null) {
            return new FetchPartitionResult(partitionIndex, ErrorCodes.UNKNOWN_TOPIC_ID, 0, new byte[0]);
        }

        // Get the Partition object from ProduceHandler.
        Partition partition = produceHandler.getPartition(topic.getTopicName(), partitionIndex);
        if (partition == null) {
            // Topic exists but no data has been written to this partition yet.
            return new FetchPartitionResult(partitionIndex, ErrorCodes.NONE, 0, new byte[0]);
        }

        // Read records from disk (Story 4.4).
        try {
            ByteBuffer records = partition.readMessages(fetchOffset, maxBytes);
            long highWatermark = partition.getNextOffset();

            byte[] recordsBytes = new byte[records.remaining()];
            records.get(recordsBytes);

            return new FetchPartitionResult(partitionIndex, ErrorCodes.NONE, highWatermark, recordsBytes);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to read from partition " + topic.getTopicName() + "-" + partitionIndex, e);
            return new FetchPartitionResult(partitionIndex, ErrorCodes.UNKNOWN_TOPIC_ID, 0, new byte[0]);
        }
    }

    /**
     * Builds the FetchResponse v16 body.
     * <p>
     * Layout:
     * throttle_time_ms (INT32)
     * error_code (INT16)
     * session_id (INT32)
     * responses_count (COMPACT_ARRAY length)
     * responses[]:
     *   topic_id (UUID 16 bytes)
     *   partitions_count (COMPACT_ARRAY length)
     *   partitions[]:
     *     partition_index (INT32)
     *     error_code (INT16)
     *     high_watermark (INT64)
     *     last_stable_offset (INT64)
     *     log_start_offset (INT64)
     *     records (COMPACT_BYTES)
     *     TAG_BUFFER
     *   TAG_BUFFER
     * TAG_BUFFER
     */
    private ByteBuffer buildResponseBody(List<FetchTopicResult> results) {
        int size = computeBodySize(results);
        ByteBuffer body = ByteBuffer.allocate(size);

        // throttle_time_ms
        Int32.write(body, 0);
        // error_code
        Int16.write(body, ErrorCodes.NONE);
        // session_id
        Int32.write(body, 0);

        // responses array
        CompactArray.writeCount(body, results.size());
        for (FetchTopicResult topicResult : results) {
            Uuid.write(body, topicResult.topicId);

            CompactArray.writeCount(body, topicResult.partitionResults.size());
            for (FetchPartitionResult partResult : topicResult.partitionResults) {
                Int32.write(body, partResult.partitionIndex);
                Int16.write(body, partResult.errorCode);
                body.putLong(partResult.highWatermark);
                body.putLong(partResult.highWatermark); // last_stable_offset = high_watermark
                body.putLong(0L); // log_start_offset

                // Records as COMPACT_BYTES
                writeCompactBytes(body, partResult.records);

                // Aborted transactions (empty)
                CompactArray.writeCount(body, 0);

                // preferred_read_replica (INT32, -1 for none)
                Int32.write(body, -1);

                Protocol.writeEmptyTagBuffer(body);
            }

            Protocol.writeEmptyTagBuffer(body);
        }

        Protocol.writeEmptyTagBuffer(body);

        body.flip();
        return body;
    }

    private int computeBodySize(List<FetchTopicResult> results) {
        int size = Int32.size() + Int16.size() + Int32.size(); // throttle_time_ms + error_code + session_id
        size += CompactArray.sizeCount(results.size());

        for (FetchTopicResult topicResult : results) {
            size += Uuid.size(); // topic_id
            size += CompactArray.sizeCount(topicResult.partitionResults.size());

            for (FetchPartitionResult partResult : topicResult.partitionResults) {
                size += Int32.size() + Int16.size() + 8 + 8 + 8; // partitionIndex + errorCode + highWatermark + lastStable + logStart
                size += computeCompactBytesSize(partResult.records.length);
                size += CompactArray.sizeCount(0); // aborted transactions
                size += Int32.size(); // preferred_read_replica
                size += 1; // TAG_BUFFER
            }
            size += 1; // per-topic TAG_BUFFER
        }
        size += 1; // trailing TAG_BUFFER
        return size;
    }

    /**
     * Writes a COMPACT_BYTES field (varint length + data).
     */
    private void writeCompactBytes(ByteBuffer buf, byte[] data) {
        if (data == null || data.length == 0) {
            CompactString.writeUnsignedVarint(buf, 1); // length = 0 means null, 1 means 0 bytes
            return;
        }
        CompactString.writeUnsignedVarint(buf, data.length + 1);
        buf.put(data);
    }

    private int computeCompactBytesSize(int dataLength) {
        if (dataLength == 0) {
            return CompactString.sizeUnsignedVarint(1);
        }
        return CompactString.sizeUnsignedVarint(dataLength + 1) + dataLength;
    }

    private void skipTaggedFields(ByteBuffer buffer) {
        int numTags = CompactString.readUnsignedVarint(buffer);
        for (int i = 0; i < numTags; i++) {
            CompactString.readUnsignedVarint(buffer); // tagId
            int tagSize = CompactString.readUnsignedVarint(buffer);
            buffer.position(buffer.position() + tagSize);
        }
    }

    /**
     * Result of fetching from a partition.
     */
    record FetchPartitionResult(int partitionIndex, short errorCode, long highWatermark, byte[] records) {
    }

    /**
     * Result of fetching from all partitions of a topic.
     */
    record FetchTopicResult(UUID topicId, List<FetchPartitionResult> partitionResults) {
    }
}
