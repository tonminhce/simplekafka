package com.simplekafka;

import com.simplekafka.shared.primitives.CompactArray;
import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int16;
import com.simplekafka.shared.primitives.Int32;

import java.net.Socket;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Shared wire-protocol helpers for tests.
 * Builds and parses raw Kafka request/response frames so tests do not
 * depend on the production client classes.
 */
final class KafkaWireHelpers {

    private KafkaWireHelpers() {}

    // ------------------------------------------------------------------ send

    /**
     * Opens a blocking TCP connection to the broker using old IO Socket.
     */
    static Socket connectSocket(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        return socket;
    }

    /**
     * Opens a Socket, sends the framed request, reads one response, returns the
     * response as a ByteBuffer (after 4-byte size field, includes response header).
     */
    static ByteBuffer sendAndReceive(String host, int port, ByteBuffer framed) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            socket.setTcpNoDelay(true);
            return sendRawSocket(socket, framed);
        }
    }

    /**
     * Sends a framed request and returns the response using raw Socket IO.
     */
    static ByteBuffer sendRawSocket(Socket socket, ByteBuffer framed) throws IOException {
        var out = socket.getOutputStream();
        byte[] data = new byte[framed.remaining()];
        framed.get(data);
        out.write(data);
        out.flush();

        var in = socket.getInputStream();
        // Read 4-byte size
        byte[] sizeBytes = new byte[4];
        readN(in, sizeBytes);
        ByteBuffer sizeBuf = ByteBuffer.wrap(sizeBytes);
        int messageSize = sizeBuf.getInt();

        byte[] msgBytes = new byte[messageSize];
        readN(in, msgBytes);
        return ByteBuffer.wrap(msgBytes);
    }

    static void readN(java.io.InputStream in, byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            int n = in.read(buf, offset, buf.length - offset);
            if (n == -1) throw new IOException("Connection closed");
            offset += n;
        }
    }

    /**
     * Opens a connection, sends a framed request, reads the response, closes.
     * Returns the response message (after the 4-byte size field).
     */
    static ByteBuffer sendRawSocket(String host, int port, ByteBuffer framed) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            socket.setTcpNoDelay(true);
            return sendRawSocket(socket, framed);
        }
    }

    /**
     * Builds a complete Kafka request frame:
     *   message_size(INT32) + header_v2 + body
     */
    static ByteBuffer frameRequest(short apiKey, short apiVersion,
                                   int correlationId, String clientId,
                                   ByteBuffer body) {
        int headerSize = Int16.size() + Int16.size() + Int32.size()
                + CompactString.size(clientId) + 1; // +1 TAG_BUFFER
        int bodySize = body != null ? body.remaining() : 0;
        int messageSize = headerSize + bodySize;

        ByteBuffer frame = ByteBuffer.allocate(Int32.size() + messageSize);
        Int32.write(frame, messageSize);

        // header v2
        Int16.write(frame, apiKey);
        Int16.write(frame, apiVersion);
        Int32.write(frame, correlationId);
        CompactString.write(frame, clientId);
        frame.put((byte) 0); // TAG_BUFFER

        if (body != null) {
            frame.put(body);
        }
        frame.flip();
        return frame;
    }

    // -------------------------------------------------------------- receive

    /**
     * Reads one Kafka response frame, returns the payload after the 4-byte
     * size field (still includes response header v1).
     */
    static ByteBuffer readResponse(SocketChannel ch) throws IOException {
        ByteBuffer sizeBuf = ByteBuffer.allocate(4);
        readFully(ch, sizeBuf);
        sizeBuf.flip();
        int messageSize = sizeBuf.getInt();

        ByteBuffer msg = ByteBuffer.allocate(messageSize);
        readFully(ch, msg);
        msg.flip();
        return msg;
    }

    static void readFully(SocketChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n == -1) throw new IOException("Connection closed while reading");
        }
    }

    // -------------------------------------------------- response parsing

    /**
     * Skips the response header v1: correlation_id(INT32) + TAG_BUFFER.
     */
    static void skipResponseHeader(ByteBuffer resp) {
        resp.getInt(); // correlation_id
        skipTagBuffer(resp);
    }

    static void skipTagBuffer(ByteBuffer buf) {
        int numTags = CompactString.readUnsignedVarint(buf);
        for (int i = 0; i < numTags; i++) {
            CompactString.readUnsignedVarint(buf);
            int sz = CompactString.readUnsignedVarint(buf);
            buf.position(buf.position() + sz);
        }
    }

    // ----------------------------------------- ApiVersions request / response

    static ByteBuffer buildApiVersionsRequest(int correlationId, String clientId) {
        return frameRequest((short) 18, (short) 4, correlationId, clientId, null);
    }

    /**
     * Parses an ApiVersions response after the response header has been consumed.
     * Returns a list of {apiKey, minVersion, maxVersion} arrays.
     */
    static List<short[]> parseApiVersionsBody(ByteBuffer body) {
        short errorCode = body.getShort();
        int count = CompactArray.readCount(body);
        List<short[]> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            short key = body.getShort();
            short min = body.getShort();
            short max = body.getShort();
            skipTagBuffer(body);
            entries.add(new short[]{key, min, max});
        }
        skipTagBuffer(body);
        return entries;
    }

    // ---------------------------------- DescribeTopicPartitions request / response

    static ByteBuffer buildDescribeTopicPartitionsRequest(int correlationId,
                                                          String clientId,
                                                          String... topicNames) {
        // body: topics array + cursor(null) + TAG_BUFFER
        int size = CompactArray.sizeCount(topicNames.length);
        for (String name : topicNames) {
            size += CompactString.size(name) + 1; // +1 TAG_BUFFER per topic
        }
        size += 1; // cursor (null compact string)
        size += 1; // trailing TAG_BUFFER

        ByteBuffer body = ByteBuffer.allocate(size);
        CompactArray.writeCount(body, topicNames.length);
        for (String name : topicNames) {
            CompactString.write(body, name);
            body.put((byte) 0); // TAG_BUFFER
        }
        CompactString.write(body, null); // cursor = null
        body.put((byte) 0); // trailing TAG_BUFFER
        body.flip();

        return frameRequest((short) 75, (short) 0, correlationId, clientId, body);
    }

    // ----------------------------------------------- Produce request helpers

    /**
     * Builds a minimal RecordBatch with one record.
     * The batch contains a properly formatted header so the broker can
     * parse recordsCount.
     */
    static byte[] buildSingleRecordBatch(String value) {
        return buildRecordBatch(new String[]{value});
    }

    /**
     * Builds a RecordBatch with multiple records.
     */
    static byte[] buildRecordBatch(String[] values) {
        // Calculate total record payload size
        int totalRecordPayload = 0;
        for (int i = 0; i < values.length; i++) {
            byte[] valBytes = values[i].getBytes(StandardCharsets.UTF_8);
            int recSize = 1 // attributes
                    + CompactString.sizeUnsignedVarint(0) // timestamp_delta
                    + CompactString.sizeUnsignedVarint(i) // offset_delta
                    + 1 // null key
                    + CompactString.sizeUnsignedVarint(valBytes.length + 1) + valBytes.length
                    + 1; // headers count
            totalRecordPayload += recSize;
        }

        // RecordBatch header: 49 bytes before records_count, then 4 for count
        int batchSize = 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + totalRecordPayload;
        ByteBuffer batch = ByteBuffer.allocate(batchSize);

        batch.putInt(batchSize - 4); // batch_length
        batch.putInt(0);             // partition_leader_epoch
        batch.put((byte) 2);         // magic
        batch.putInt(0);             // crc placeholder
        batch.putShort((short) 0);   // attributes
        batch.putInt(values.length - 1); // last_offset_delta
        batch.putLong(System.currentTimeMillis()); // first_timestamp
        batch.putLong(System.currentTimeMillis()); // max_timestamp
        batch.putLong(0);            // producer_id
        batch.putShort((short) 0);   // producer_epoch
        batch.putInt(0);             // base_sequence
        batch.putInt(values.length); // records_count

        for (int i = 0; i < values.length; i++) {
            byte[] valBytes = values[i].getBytes(StandardCharsets.UTF_8);
            batch.put((byte) 0); // record attributes
            CompactString.writeUnsignedVarint(batch, 0); // timestamp_delta
            CompactString.writeUnsignedVarint(batch, i); // offset_delta
            CompactString.writeUnsignedVarint(batch, 0); // null key
            CompactString.writeUnsignedVarint(batch, valBytes.length + 1);
            batch.put(valBytes);
            batch.put((byte) 0); // headers count
        }

        batch.flip();
        byte[] result = new byte[batch.remaining()];
        batch.get(result);
        return result;
    }

    /**
     * Builds a Produce request body for one topic / one partition.
     */
    static ByteBuffer buildProduceRequestBody(String topic, int partition,
                                               byte[] recordBatch) {
        int size = CompactString.size(null) // transactional_id
                + Int16.size()  // acks
                + Int32.size()  // timeout_ms
                + CompactArray.sizeCount(1)
                + CompactString.size(topic) + 1 // topic + TAG_BUFFER
                + CompactArray.sizeCount(1)      // partitions count
                + Int32.size()                   // partition_index
                + CompactString.sizeUnsignedVarint(recordBatch.length + 1) + recordBatch.length
                + 1;                             // per-partition TAG_BUFFER

        ByteBuffer body = ByteBuffer.allocate(size);
        CompactString.write(body, null); // transactional_id
        Int16.write(body, (short) -1);  // acks
        Int32.write(body, 30000);        // timeout_ms

        CompactArray.writeCount(body, 1);
        CompactString.write(body, topic);
        body.put((byte) 0); // per-topic TAG_BUFFER

        CompactArray.writeCount(body, 1);
        Int32.write(body, partition);
        CompactString.writeUnsignedVarint(body, recordBatch.length + 1);
        body.put(recordBatch);
        body.put((byte) 0); // per-partition TAG_BUFFER

        body.flip();
        return body;
    }

    /**
     * Builds a Produce request body for multiple topics with multiple partitions.
     */
    static ByteBuffer buildMultiProduceRequestBody(String[] topics, int[][] partitions,
                                                    byte[][][] recordBatches) {
        int size = CompactString.size(null) + Int16.size() + Int32.size();
        size += CompactArray.sizeCount(topics.length);

        for (int t = 0; t < topics.length; t++) {
            size += CompactString.size(topics[t]) + 1; // topic + TAG_BUFFER
            size += CompactArray.sizeCount(partitions[t].length);
            for (int p = 0; p < partitions[t].length; p++) {
                size += Int32.size(); // partition_index
                size += CompactString.sizeUnsignedVarint(recordBatches[t][p].length + 1)
                        + recordBatches[t][p].length;
                size += 1; // TAG_BUFFER
            }
        }

        ByteBuffer body = ByteBuffer.allocate(size);
        CompactString.write(body, null);
        Int16.write(body, (short) -1);
        Int32.write(body, 30000);

        CompactArray.writeCount(body, topics.length);
        for (int t = 0; t < topics.length; t++) {
            CompactString.write(body, topics[t]);
            body.put((byte) 0); // TAG_BUFFER
            CompactArray.writeCount(body, partitions[t].length);
            for (int p = 0; p < partitions[t].length; p++) {
                Int32.write(body, partitions[t][p]);
                CompactString.writeUnsignedVarint(body, recordBatches[t][p].length + 1);
                body.put(recordBatches[t][p]);
                body.put((byte) 0); // TAG_BUFFER
            }
        }

        body.flip();
        return body;
    }

    // ------------------------------------------------ Fetch request helpers

    static ByteBuffer buildFetchRequestBody(UUID topicId, int partitionIndex,
                                             long fetchOffset, int maxBytes) {
        int size = Int32.size()  // cluster_id
                + Int32.size()   // session_id
                + CompactArray.sizeCount(1)
                + 16             // topic_id (UUID)
                + CompactArray.sizeCount(1)
                + Int32.size()   // partition_index
                + Int32.size()   // current_leader_epoch
                + 8              // fetch_offset
                + Int32.size()   // last_fetched_epoch
                + 8              // log_start_offset (INT64)
                + Int32.size()   // partition_max_bytes
                + 1              // per-partition TAG_BUFFER
                + 1              // per-topic TAG_BUFFER
                + 1;             // trailing TAG_BUFFER

        ByteBuffer body = ByteBuffer.allocate(size);
        Int32.write(body, -1);  // cluster_id
        Int32.write(body, 0);   // session_id

        CompactArray.writeCount(body, 1);
        body.putLong(topicId.getMostSignificantBits());
        body.putLong(topicId.getLeastSignificantBits());

        CompactArray.writeCount(body, 1);
        Int32.write(body, partitionIndex);
        Int32.write(body, 0);   // current_leader_epoch
        body.putLong(fetchOffset);
        Int32.write(body, -1);  // last_fetched_epoch
        body.putLong(0L);       // log_start_offset (INT64)
        Int32.write(body, maxBytes);
        body.put((byte) 0);     // per-partition TAG_BUFFER

        body.put((byte) 0);     // per-topic TAG_BUFFER
        body.put((byte) 0);     // trailing TAG_BUFFER

        body.flip();
        return body;
    }

    // ---------------------------------------- produce response parsing

    /**
     * Parses produce response, returns [topicName, partitionIndex, errorCode, baseOffset] per partition.
     */
    static List<Object[]> parseProduceResponse(ByteBuffer resp) {
        skipResponseHeader(resp);
        List<Object[]> results = new ArrayList<>();
        int topicCount = CompactArray.readCount(resp);
        for (int t = 0; t < topicCount; t++) {
            String topicName = CompactString.read(resp);
            int partCount = CompactArray.readCount(resp);
            for (int p = 0; p < partCount; p++) {
                int partIndex = resp.getInt();
                short err = resp.getShort();
                long baseOffset = resp.getLong();
                skipTagBuffer(resp);
                results.add(new Object[]{topicName, partIndex, err, baseOffset});
            }
            skipTagBuffer(resp);
        }
        skipTagBuffer(resp);
        return results;
    }

    /**
     * Parses fetch response, returns [errorCode, highWatermark, recordsBytes] per partition.
     */
    static List<Object[]> parseFetchResponse(ByteBuffer resp) {
        skipResponseHeader(resp);
        List<Object[]> results = new ArrayList<>();
        Int32.read(resp); // throttle_time_ms
        Int16.read(resp); // error_code
        Int32.read(resp); // session_id

        int topicCount = CompactArray.readCount(resp);
        for (int t = 0; t < topicCount; t++) {
            resp.getLong(); resp.getLong(); // topic_id UUID

            int partCount = CompactArray.readCount(resp);
            for (int p = 0; p < partCount; p++) {
                int partIndex = resp.getInt();
                short errCode = resp.getShort();
                long hw = resp.getLong();
                resp.getLong(); // last_stable_offset
                resp.getLong(); // log_start_offset

                int recLen = CompactString.readUnsignedVarint(resp) - 1;
                byte[] records = new byte[recLen];
                if (recLen > 0) resp.get(records);

                CompactArray.readCount(resp); // aborted txns
                Int32.read(resp);             // preferred_read_replica
                skipTagBuffer(resp);

                results.add(new Object[]{partIndex, errCode, hw, records});
            }
            skipTagBuffer(resp);
        }
        skipTagBuffer(resp);
        return results;
    }

    /**
     * Parses DescribeTopicPartitions response, returns topic entries.
     */
    static List<TopicMetadataEntry> parseDescribeResponse(ByteBuffer resp) {
        skipResponseHeader(resp);
        Int32.read(resp); // throttle_time_ms

        int topicCount = CompactArray.readCount(resp);
        List<TopicMetadataEntry> entries = new ArrayList<>();
        for (int t = 0; t < topicCount; t++) {
            short errorCode = resp.getShort();
            long msb = resp.getLong();
            long lsb = resp.getLong();
            UUID topicId = new UUID(msb, lsb);
            String topicName = CompactString.read(resp);

            int partCount = CompactArray.readCount(resp);
            List<Integer> partitionIds = new ArrayList<>();
            for (int p = 0; p < partCount; p++) {
                short pErr = resp.getShort();
                int pIndex = resp.getInt();
                int leaderId = resp.getInt();
                int leaderEpoch = resp.getInt();

                int replicaCount = CompactArray.readCount(resp);
                for (int r = 0; r < replicaCount; r++) resp.getInt();

                int isrCount = CompactArray.readCount(resp);
                for (int i = 0; i < isrCount; i++) resp.getInt();

                skipTagBuffer(resp);
                partitionIds.add(pIndex);
            }
            resp.getInt(); // topic_authorized_operations
            skipTagBuffer(resp);

            entries.add(new TopicMetadataEntry(errorCode, topicId, topicName, partitionIds));
        }

        CompactString.read(resp); // next_cursor
        skipTagBuffer(resp);

        return entries;
    }

    // ----------------------------------------------------- data class

    static class TopicMetadataEntry {
        final short errorCode;
        final UUID topicId;
        final String topicName;
        final List<Integer> partitionIds;

        TopicMetadataEntry(short errorCode, UUID topicId, String topicName,
                           List<Integer> partitionIds) {
            this.errorCode = errorCode;
            this.topicId = topicId;
            this.topicName = topicName;
            this.partitionIds = partitionIds;
        }
    }
}
