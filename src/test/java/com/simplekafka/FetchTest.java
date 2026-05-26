package com.simplekafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Epic 4: Message Consumption")
class FetchTest extends AbstractBrokerTest {

    private ByteBuffer send(ByteBuffer req) throws IOException {
        return KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
    }

    private void produceOneRecord(String topic) throws IOException {
        byte[] batch = KafkaWireHelpers.buildSingleRecordBatch("data");
        ByteBuffer body = KafkaWireHelpers.buildProduceRequestBody(topic, 0, batch);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body);
        send(req);
    }

    @Test
    @Timeout(15)
    @DisplayName("4.1 Fetch empty response for topic with no data")
    void fetchEmptyResponseForTopicWithNoData() throws IOException {
        createTopic("empty-topic", 1);
        UUID topicId = broker.getMetadataStore().getTopic("empty-topic").getTopicId();

        ByteBuffer body = KafkaWireHelpers.buildFetchRequestBody(topicId, 0, 0L, 1024);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[1]);
        assertEquals(0L, results.get(0)[2], "High watermark should be 0");
        assertEquals(0, ((byte[]) results.get(0)[3]).length);
    }

    @Test
    @Timeout(15)
    @DisplayName("4.2 Fetch unknown topic_id returns error 100")
    void fetchUnknownTopicIdReturnsError100() throws IOException {
        UUID unknownId = UUID.randomUUID();

        ByteBuffer body = KafkaWireHelpers.buildFetchRequestBody(unknownId, 0, 0L, 1024);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 100, results.get(0)[1]);
    }

    @Test
    @Timeout(15)
    @DisplayName("4.3 Fetch offset past all data returns empty records")
    void fetchOffsetPastAllDataReturnsEmptyRecords() throws IOException {
        createTopic("offset-test", 1);
        UUID topicId = broker.getMetadataStore().getTopic("offset-test").getTopicId();

        produceOneRecord("offset-test");

        ByteBuffer body = KafkaWireHelpers.buildFetchRequestBody(topicId, 0, 100L, 1024);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[1]);
        assertEquals(0, ((byte[]) results.get(0)[3]).length);
    }

    @Test
    @Timeout(15)
    @DisplayName("4.4 Fetch reads records from disk at specific offset")
    void fetchReadsRecordsFromDiskAtSpecificOffset() throws IOException {
        createTopic("fetch-read", 1);
        UUID topicId = broker.getMetadataStore().getTopic("fetch-read").getTopicId();

        // Produce 2 batches
        byte[] batch1 = KafkaWireHelpers.buildSingleRecordBatch("first");
        ByteBuffer body1 = KafkaWireHelpers.buildProduceRequestBody("fetch-read", 0, batch1);
        ByteBuffer req1 = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body1);
        ByteBuffer resp1 = send(req1);
        List<Object[]> r1 = KafkaWireHelpers.parseProduceResponse(resp1);
        assertEquals(0L, r1.get(0)[3]);

        byte[] batch2 = KafkaWireHelpers.buildSingleRecordBatch("second");
        ByteBuffer body2 = KafkaWireHelpers.buildProduceRequestBody("fetch-read", 0, batch2);
        ByteBuffer req2 = KafkaWireHelpers.frameRequest((short) 0, (short) 11,
                nextCorrelationId(), "test", body2);
        ByteBuffer resp2 = send(req2);
        List<Object[]> r2 = KafkaWireHelpers.parseProduceResponse(resp2);
        assertEquals(1L, r2.get(0)[3]);

        // Fetch from offset 0
        ByteBuffer fetchBody = KafkaWireHelpers.buildFetchRequestBody(topicId, 0, 0L, 65536);
        ByteBuffer fetchReq = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", fetchBody);
        ByteBuffer fetchResp = send(fetchReq);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(fetchResp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[1]);
        assertEquals(2L, results.get(0)[2], "High watermark should be 2");
        assertTrue(((byte[]) results.get(0)[3]).length > 0);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Fetch offset 0 on empty topic")
    void fetchOffset0OnEmptyTopic() throws IOException {
        createTopic("empty-offset", 1);
        UUID topicId = broker.getMetadataStore().getTopic("empty-offset").getTopicId();

        ByteBuffer body = KafkaWireHelpers.buildFetchRequestBody(topicId, 0, 0L, 1024);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[1]);
        assertEquals(0, ((byte[]) results.get(0)[3]).length);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Fetch with very large max_bytes")
    void fetchWithVeryLargeMaxBytes() throws IOException {
        createTopic("large-max", 1);
        UUID topicId = broker.getMetadataStore().getTopic("large-max").getTopicId();
        produceOneRecord("large-max");

        ByteBuffer body = KafkaWireHelpers.buildFetchRequestBody(topicId, 0, 0L, Integer.MAX_VALUE);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[1]);
        assertTrue(((byte[]) results.get(0)[3]).length > 0);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Produce then immediately fetch (latency test)")
    void produceThenImmediatelyFetch() throws IOException {
        createTopic("latency-test", 1);
        UUID topicId = broker.getMetadataStore().getTopic("latency-test").getTopicId();

        // Produce on one connection
        produceOneRecord("latency-test");

        // Fetch on another connection
        ByteBuffer fetchBody = KafkaWireHelpers.buildFetchRequestBody(topicId, 0, 0L, 65536);
        ByteBuffer fetchReq = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", fetchBody);
        ByteBuffer fetchResp = send(fetchReq);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(fetchResp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[1]);
        assertTrue(((byte[]) results.get(0)[3]).length > 0);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Fetch non-existent partition of existing topic")
    void fetchNonExistentPartition() throws IOException {
        createTopic("exists-topic", 1);
        UUID topicId = broker.getMetadataStore().getTopic("exists-topic").getTopicId();

        ByteBuffer body = KafkaWireHelpers.buildFetchRequestBody(topicId, 99, 0L, 1024);
        ByteBuffer req = KafkaWireHelpers.frameRequest((short) 1, (short) 16,
                nextCorrelationId(), "test", body);
        ByteBuffer resp = send(req);

        List<Object[]> results = KafkaWireHelpers.parseFetchResponse(resp);
        assertEquals(1, results.size());
        assertEquals((short) 0, results.get(0)[1]);
        assertEquals(0, ((byte[]) results.get(0)[3]).length);
    }
}
