package com.simplekafka.client;

import com.simplekafka.shared.primitives.CompactArray;
import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int16;
import com.simplekafka.shared.primitives.Int32;
import com.simplekafka.shared.primitives.Uuid;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * High-level consumer that fetches messages from a SimpleKafka broker.
 */
public class SimpleKafkaConsumer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private static final short FETCH_API_KEY = 1;
    private static final short FETCH_API_VERSION = 16;

    private final SimpleKafkaClient client;
    private final String clientId;
    private long currentOffset;

    public SimpleKafkaConsumer(String host, int port, String clientId) {
        this.client = new SimpleKafkaClient(host, port);
        this.clientId = clientId;
        this.currentOffset = 0;
    }

    /**
     * Connects to the broker.
     */
    public void initialize() throws IOException {
        client.connect();
        LOGGER.info("Consumer connected to broker");
    }

    /**
     * Seeks to a specific offset.
     */
    public void seek(long offset) {
        this.currentOffset = offset;
    }

    /**
     * Polls for messages and returns parsed record values as strings.
     *
     * @param topicId        the topic UUID
     * @param partitionIndex the partition index
     * @param maxBytes       maximum bytes to fetch
     * @return list of record values as strings
     */
    public List<String> pollValues(UUID topicId, int partitionIndex, int maxBytes) throws IOException {
        List<byte[]> rawBatches = poll(topicId, partitionIndex, maxBytes);
        List<String> values = new ArrayList<>();

        for (byte[] batch : rawBatches) {
            values.addAll(parseRecordBatch(batch));
        }
        return values;
    }

    /**
     * Polls for messages from the specified topic and partition.
     *
     * @param topicId        the topic UUID
     * @param partitionIndex the partition index
     * @param maxBytes       maximum bytes to fetch
     * @return list of byte arrays, each representing a raw record batch
     */
    public List<byte[]> poll(UUID topicId, int partitionIndex, int maxBytes) throws IOException {
        ByteBuffer body = buildFetchBody(topicId, partitionIndex, currentOffset, maxBytes);

        ByteBuffer response = client.sendRequest(FETCH_API_KEY, FETCH_API_VERSION, clientId, body);
        return parseFetchResponse(response);
    }

    /**
     * Parses a RecordBatch and extracts individual record values as strings.
     *
     * RecordBatch layout:
     *   base_offset: INT64 (8)
     *   batch_length: INT32 (4)
     *   partition_leader_epoch: INT32 (4)
     *   magic: INT8 (1)
     *   crc: INT32 (4)
     *   attributes: INT16 (2)
     *   last_offset_delta: INT32 (4)
     *   first_timestamp: INT64 (8)
     *   max_timestamp: INT64 (8)
     *   producer_id: INT64 (8)
     *   producer_epoch: INT16 (2)
     *   base_sequence: INT32 (4)
     *   records_count: INT32 (4)
     *   records[]:
     *     attributes: INT8
     *     timestamp_delta: varint
     *     offset_delta: varint
     *     key_length: varint (0 = null, N+1 = N bytes)
     *     key: bytes
     *     value_length: varint (N+1 = N bytes)
     *     value: bytes
     *     headers_count: varint
     */
    private List<String> parseRecordBatch(byte[] data) {
        List<String> values = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.wrap(data);

        // On-disk format: multiple entries of [base_offset(INT64) + RecordBatch bytes]
        while (buf.hasRemaining()) {
            // Check minimum: base_offset(8) + batch_length(4) + header(45) = 57 bytes minimum
            if (buf.remaining() < 57) {
                break; // Truncated or incomplete batch
            }

            // Read base_offset
            buf.getLong(); // base_offset

            // RecordBatch header (45 bytes before records_count)
            int batchLength = buf.getInt();  // batch_length
            buf.getInt();  // partition_leader_epoch
            buf.get();     // magic
            buf.getInt();  // crc
            buf.getShort();// attributes
            buf.getInt();  // last_offset_delta
            buf.getLong(); // first_timestamp
            buf.getLong(); // max_timestamp
            buf.getLong(); // producer_id
            buf.getShort();// producer_epoch
            buf.getInt();  // base_sequence
            int recordsCount = buf.getInt(); // records_count

            for (int i = 0; i < recordsCount; i++) {
                buf.get(); // record attributes
                readVarint(buf); // timestamp_delta
                readVarint(buf); // offset_delta

                // Key
                int keyLen = readVarint(buf);
                if (keyLen > 0) {
                    buf.position(buf.position() + keyLen - 1);
                }

                // Value
                int valueLen = readVarint(buf);
                if (valueLen > 0) {
                    int actualLen = valueLen - 1;
                    byte[] valueBytes = new byte[actualLen];
                    buf.get(valueBytes);
                    values.add(new String(valueBytes, StandardCharsets.UTF_8));
                }

                readVarint(buf); // headers count
            }
        }

        return values;
    }

    private int readVarint(ByteBuffer buf) {
        return CompactString.readUnsignedVarint(buf);
    }

    /**
     * Builds the FetchRequest v16 body.
     */
    private ByteBuffer buildFetchBody(UUID topicId, int partitionIndex, long fetchOffset, int maxBytes) {
        int size = Int32.size() + // cluster_id (-1 for null)
                Int32.size() + // session_id
                CompactArray.sizeCount(1) + // topics count
                Uuid.size() + // topic_id
                CompactArray.sizeCount(1) + // partitions count
                Int32.size() + // partition_index
                Int32.size() + // current_leader_epoch
                8 + // fetch_offset
                Int32.size() + // last_fetched_epoch
                8 + // log_start_offset (INT64)
                Int32.size() + // partition_max_bytes
                1; // per-partition TAG_BUFFER + per-topic TAG_BUFFER

        // We need extra for the per-topic and trailing tag buffers.
        size += 1 + 1; // per-topic and trailing TAG_BUFFER

        ByteBuffer body = ByteBuffer.allocate(size);

        // cluster_id (-1 for null)
        Int32.write(body, -1);
        // session_id
        Int32.write(body, 0);

        // topics array
        CompactArray.writeCount(body, 1);
        Uuid.write(body, topicId);

        // partitions array
        CompactArray.writeCount(body, 1);
        Int32.write(body, partitionIndex);
        Int32.write(body, 0); // current_leader_epoch
        body.putLong(fetchOffset); // fetch_offset
        Int32.write(body, -1); // last_fetched_epoch
        body.putLong(0L); // log_start_offset (INT64)
        Int32.write(body, maxBytes); // partition_max_bytes
        body.put((byte) 0); // per-partition TAG_BUFFER

        body.put((byte) 0); // per-topic TAG_BUFFER
        body.put((byte) 0); // trailing TAG_BUFFER

        body.flip();
        return body;
    }

    /**
     * Parses the FetchResponse v16 and extracts record batches.
     */
    private List<byte[]> parseFetchResponse(ByteBuffer response) {
        List<byte[]> batches = new ArrayList<>();

        // throttle_time_ms
        Int32.read(response);
        // error_code
        Int16.read(response);
        // session_id
        Int32.read(response);

        // responses array
        int topicCount = CompactArray.readCount(response);
        for (int t = 0; t < topicCount; t++) {
            Uuid.read(response); // topic_id

            int partitionCount = CompactArray.readCount(response);
            for (int p = 0; p < partitionCount; p++) {
                Int32.read(response); // partition_index
                short errorCode = Int16.read(response); // error_code
                long highWatermark = response.getLong(); // high_watermark
                response.getLong(); // last_stable_offset
                response.getLong(); // log_start_offset

                // records (COMPACT_BYTES)
                int recordsLength = CompactString.readUnsignedVarint(response) - 1;
                if (recordsLength > 0) {
                    byte[] records = new byte[recordsLength];
                    response.get(records);
                    batches.add(records);

                    // Advance offset.
                    currentOffset = highWatermark;
                }

                // Aborted transactions (empty array)
                CompactArray.readCount(response);
                // preferred_read_replica
                Int32.read(response);
                skipTagBuffer(response);
            }
            skipTagBuffer(response);
        }
        skipTagBuffer(response);

        return batches;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    private void skipTagBuffer(ByteBuffer buf) {
        int numTags = CompactString.readUnsignedVarint(buf);
        for (int i = 0; i < numTags; i++) {
            CompactString.readUnsignedVarint(buf);
            int size = CompactString.readUnsignedVarint(buf);
            buf.position(buf.position() + size);
        }
    }

    /**
     * Disconnects from the broker.
     */
    public void close() {
        client.disconnect();
    }
}
