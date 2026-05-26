package com.simplekafka;

import com.simplekafka.shared.primitives.CompactArray;
import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int16;
import com.simplekafka.shared.primitives.Int32;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Epic 3: Message Production")
class ProduceTest extends AbstractBrokerTest {

    private ByteBuffer send(ByteBuffer req) throws IOException {
        return KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
    }

    @Test
    @Timeout(15)
    @DisplayName("3.1 ApiVersions includes Produce (key 0)")
    void apiVersionsIncludesProduce() throws IOException {
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "test");
        ByteBuffer resp = send(req);
        KafkaWireHelpers.skipResponseHeader(resp);
        List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
        boolean hasProduce = false;
        for (short[] e : entries) {
            if (e[0] == 0) hasProduce = true;
        }
        assertTrue(hasProduce, "ApiVersions must include Produce (key 0)");
    }

    @Test
    @Timeout(15)
    @DisplayName("3.2 Produce to non-existent topic returns error 3")
    void produceToNonExistentTopicReturnsError3() throws IOException {
        byte[] batch = KafkaWireHelpers.buildSingleRecordBatch("hello");
        ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody("no-such-topic", 0, batch);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 3, results.get(0)[2]);
    }

    @Test
    @Timeout(15)
    @DisplayName("3.3 Produce to non-existent partition returns error 3")
    void produceToNonExistentPartitionReturnsError3() throws IOException {
        createTopic("exists-topic", 1);

        byte[] batch = KafkaWireHelpers.buildSingleRecordBatch("hello");
        ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody("exists-topic", 99, batch);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 3, results.get(0)[2]);
    }

    @Test
    @Timeout(15)
    @DisplayName("3.4 Single record written to disk, base_offset returned")
    void singleRecordWrittenBaseOffsetReturned() throws IOException {
        createTopic("single-rec", 1);

        byte[] batch = KafkaWireHelpers.buildSingleRecordBatch("hello-world");
        ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody("single-rec", 0, batch);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[2]);
        assertEquals(0L, results.get(0)[3]);
    }

    @Test
    @Timeout(15)
    @DisplayName("3.5 Multiple records in one batch with sequential offsets")
    void multipleRecordsInOneBatchSequentialOffsets() throws IOException {
        createTopic("multi-rec", 1);

        // First batch: 3 records -> base_offset = 0
        byte[] batch1 = KafkaWireHelpers.buildRecordBatch(
                new String[]{"msg-A", "msg-B", "msg-C"});
        ByteBuffer body1 = KafkaWireHelpers.buildProduceRequestBody("multi-rec", 0, batch1);
        ByteBuffer req1 = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body1);
        ByteBuffer resp1 = send(req1);
        List<Object[]> r1 = KafkaWireHelpers.parseProduceResponse(resp1);
        assertEquals(0L, r1.get(0)[3], "First batch base offset = 0");

        // Second batch: 2 more records -> base_offset = 3
        byte[] batch2 = KafkaWireHelpers.buildRecordBatch(
                new String[]{"msg-D", "msg-E"});
        ByteBuffer body2 = KafkaWireHelpers.buildProduceRequestBody("multi-rec", 0, batch2);
        ByteBuffer req2 = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body2);
        ByteBuffer resp2 = send(req2);
        List<Object[]> r2 = KafkaWireHelpers.parseProduceResponse(resp2);
        assertEquals(3L, r2.get(0)[3], "Second batch base offset = 3");
    }

    @Test
    @Timeout(15)
    @DisplayName("3.6 Multiple partitions in one request")
    void multiplePartitionsInOneRequest() throws IOException {
        createTopic("multi-part-prod", 2);

        byte[] batch0 = KafkaWireHelpers.buildSingleRecordBatch("to-partition-0");
        byte[] batch1 = KafkaWireHelpers.buildSingleRecordBatch("to-partition-1");

        int size = CompactString.size(null) + Int16.size() + Int32.size()
                + CompactArray.sizeCount(1)
                + CompactString.size("multi-part-prod") + 1
                + CompactArray.sizeCount(2)
                + Int32.size() + CompactString.sizeUnsignedVarint(batch0.length + 1) + batch0.length + 1
                + Int32.size() + CompactString.sizeUnsignedVarint(batch1.length + 1) + batch1.length + 1;

        ByteBuffer body = ByteBuffer.allocate(size);
        CompactString.write(body, null);
        Int16.write(body, (short) -1);
        Int32.write(body, 30000);

        CompactArray.writeCount(body, 1);
        CompactString.write(body, "multi-part-prod");
        body.put((byte) 0);

        CompactArray.writeCount(body, 2);
        Int32.write(body, 0);
        CompactString.writeUnsignedVarint(body, batch0.length + 1);
        body.put(batch0);
        body.put((byte) 0);

        Int32.write(body, 1);
        CompactString.writeUnsignedVarint(body, batch1.length + 1);
        body.put(batch1);
        body.put((byte) 0);
        body.flip();

        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals(2, results.size());
        assertEquals(0, results.get(0)[1]);
        assertEquals((short) 0, results.get(0)[2]);
        assertEquals(1, results.get(1)[1]);
        assertEquals((short) 0, results.get(1)[2]);
    }

    @Test
    @Timeout(15)
    @DisplayName("3.7 Multiple topics in one request")
    void multipleTopicsInOneRequest() throws IOException {
        createTopic("topic-a", 1);
        createTopic("topic-b", 1);

        byte[] batchA = KafkaWireHelpers.buildSingleRecordBatch("msg-a");
        byte[] batchB = KafkaWireHelpers.buildSingleRecordBatch("msg-b");

        ByteBuffer body = KafkaWireHelpers.buildMultiProduceRequestBody(
                new String[]{"topic-a", "topic-b"},
                new int[][]{{0}, {0}},
                new byte[][][]{new byte[][]{batchA}, new byte[][]{batchB}});

        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals(2, results.size());
        boolean foundA = false, foundB = false;
        for (Object[] r : results) {
            String topic = (String) r[0];
            if ("topic-a".equals(topic)) foundA = true;
            if ("topic-b".equals(topic)) foundB = true;
            assertEquals((short) 0, r[2]);
        }
        assertTrue(foundA);
        assertTrue(foundB);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Produce to out-of-range partition")
    void produceToOutOfRangePartition() throws IOException {
        createTopic("two-parts", 2);

        byte[] batch = KafkaWireHelpers.buildSingleRecordBatch("data");
        ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody("two-parts", 5, batch);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 3, results.get(0)[2]);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Very large record payload")
    void veryLargeRecordPayload() throws IOException {
        createTopic("large-payload", 1);

        String largeValue = "x".repeat(100_000);
        byte[] batch = KafkaWireHelpers.buildSingleRecordBatch(largeValue);
        ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody("large-payload", 0, batch);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[2]);
    }

    @Test
    @Timeout(30)
    @DisplayName("Edge: Multiple rapid sequential produce requests")
    void multipleRapidSequentialProduces() throws IOException {
        createTopic("rapid-prod", 1);

        for (int i = 0; i < 10; i++) {
            byte[] batch = KafkaWireHelpers.buildSingleRecordBatch("msg-" + i);
            ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody("rapid-prod", 0, batch);
            ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                    nextCorrelationId(), "test", body);
            ByteBuffer resp = send(req);
            List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
            assertEquals((short) 0, results.get(0)[2], "Produce " + i + " should succeed");
        }
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Topic name with special characters in produce")
    void produceWithSpecialCharTopic() throws IOException {
        String specialName = "my.topic_v2";
        createTopic(specialName, 1);

        byte[] batch = KafkaWireHelpers.buildSingleRecordBatch("data");
        ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody(specialName, 0, batch);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseProduceResponse(resp);
        assertEquals((short) 0, results.get(0)[2]);
    }
}
