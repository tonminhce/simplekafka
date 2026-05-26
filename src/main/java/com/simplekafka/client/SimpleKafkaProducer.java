package com.simplekafka.client;

import com.simplekafka.shared.primitives.CompactArray;
import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int16;
import com.simplekafka.shared.primitives.Int32;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.logging.Logger;

/**
 * High-level producer that sends messages to a SimpleKafka broker.
 */
public class SimpleKafkaProducer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaProducer.class.getName());
    private static final short PRODUCE_API_KEY = 0;
    private static final short PRODUCE_API_VERSION = 11;

    private final SimpleKafkaClient client;
    private final String clientId;
    private final Random partitionRandom = new Random();

    public SimpleKafkaProducer(String host, int port, String clientId) {
        this.client = new SimpleKafkaClient(host, port);
        this.clientId = clientId;
    }

    /**
     * Connects to the broker.
     */
    public void initialize() throws IOException {
        client.connect();
        LOGGER.info("Producer connected to broker");
    }

    /**
     * Sends a message to the specified topic and partition.
     *
     * @param topic     the topic name
     * @param partition the partition index (-1 for random)
     * @param key       the message key (nullable)
     * @param value     the message value
     * @return the base_offset assigned to the record
     */
    public long send(String topic, int partition, String key, String value) throws IOException {
        if (partition < 0) {
            partition = 0; // simplified: default to partition 0
        }

        byte[] recordBatch = buildRecordBatch(key, value);
        ByteBuffer body = buildProduceBody(topic, partition, recordBatch);

        ByteBuffer response = client.sendRequest(PRODUCE_API_KEY, PRODUCE_API_VERSION, clientId, body);
        return parseProduceResponse(response);
    }

    /**
     * Sends a message to a random partition of the specified topic.
     */
    public long send(String topic, String key, String value) throws IOException {
        return send(topic, -1, key, value);
    }

    /**
     * Builds a simple RecordBatch with a single record.
     */
    private byte[] buildRecordBatch(String key, String value) {
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        // Record encoding
        int recordSize = 1; // attributes
        recordSize += sizeVarint(0); // timestamp_delta
        recordSize += sizeVarint(0); // offset_delta
        recordSize += keyBytes != null ?
                CompactString.sizeUnsignedVarint(keyBytes.length + 1) + keyBytes.length :
                1; // null key
        recordSize += CompactString.sizeUnsignedVarint(valueBytes.length + 1) + valueBytes.length;
        recordSize += 1; // headers count (0)

        // RecordBatch header: 49 bytes + records
        int batchSize = 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + recordSize;
        ByteBuffer batch = ByteBuffer.allocate(batchSize);

        // batch_length
        batch.putInt(batchSize - 4);
        // partition_leader_epoch
        batch.putInt(0);
        // magic
        batch.put((byte) 2);
        // crc (placeholder)
        batch.putInt(0);
        // attributes
        batch.putShort((short) 0);
        // last_offset_delta
        batch.putInt(0);
        // first_timestamp
        batch.putLong(System.currentTimeMillis());
        // max_timestamp
        batch.putLong(System.currentTimeMillis());
        // producer_id
        batch.putLong(0);
        // producer_epoch
        batch.putShort((short) 0);
        // base_sequence
        batch.putInt(0);
        // records_count
        batch.putInt(1);

        // Record
        batch.put((byte) 0); // attributes
        writeVarint(batch, 0); // timestamp_delta
        writeVarint(batch, 0); // offset_delta
        if (keyBytes != null) {
            writeVarint(batch, keyBytes.length + 1);
            batch.put(keyBytes);
        } else {
            writeVarint(batch, 0); // null
        }
        writeVarint(batch, valueBytes.length + 1);
        batch.put(valueBytes);
        batch.put((byte) 0); // headers count

        batch.flip();
        byte[] result = new byte[batch.remaining()];
        batch.get(result);
        return result;
    }

    /**
     * Builds the ProduceRequest v11 body.
     */
    private ByteBuffer buildProduceBody(String topic, int partition, byte[] recordBatch) {
        int size = CompactString.size(null) + // transactional_id
                Int16.size() + // acks
                Int32.size() + // timeout_ms
                CompactArray.sizeCount(1) + // topics count
                CompactString.size(topic) + // topic name
                1 + // per-topic TAG_BUFFER
                CompactArray.sizeCount(1) + // partitions count
                Int32.size() + // partition_index
                CompactString.sizeUnsignedVarint(recordBatch.length + 1) + recordBatch.length +
                1; // per-partition TAG_BUFFER

        ByteBuffer body = ByteBuffer.allocate(size);

        // transactional_id (null)
        CompactString.write(body, null);
        // acks
        Int16.write(body, (short) -1);
        // timeout_ms
        Int32.write(body, 30000);

        // topics array
        CompactArray.writeCount(body, 1);
        CompactString.write(body, topic);
        body.put((byte) 0); // per-topic TAG_BUFFER

        // partitions array
        CompactArray.writeCount(body, 1);
        Int32.write(body, partition);
        // record_batch as COMPACT_BYTES
        CompactString.writeUnsignedVarint(body, recordBatch.length + 1);
        body.put(recordBatch);
        body.put((byte) 0); // per-partition TAG_BUFFER

        body.flip();
        return body;
    }

    /**
     * Parses the ProduceResponse to extract the base_offset.
     */
    private long parseProduceResponse(ByteBuffer response) {
        // topics array
        int topicCount = CompactArray.readCount(response);
        long baseOffset = -1;

        for (int t = 0; t < topicCount; t++) {
            CompactString.read(response); // topic_name

            int partitionCount = CompactArray.readCount(response);
            for (int p = 0; p < partitionCount; p++) {
                Int32.read(response); // partition_index
                Int16.read(response); // error_code
                baseOffset = response.getLong(); // base_offset
                skipTagBuffer(response);
            }
            skipTagBuffer(response);
        }
        skipTagBuffer(response);

        return baseOffset;
    }

    private void writeVarint(ByteBuffer buf, int value) {
        CompactString.writeUnsignedVarint(buf, value);
    }

    private int sizeVarint(int value) {
        return CompactString.sizeUnsignedVarint(value);
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
