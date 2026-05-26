package com.simplekafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Epic 2: Topic Metadata Discovery")
class TopicDiscoveryTest extends AbstractBrokerTest {

    private ByteBuffer send(ByteBuffer req) throws IOException {
        return KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
    }

    @Test
    @Timeout(15)
    @DisplayName("2.1 Unknown topic returns error 3 with zero topic_id")
    void unknownTopicReturnsError3() throws IOException {
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client", "nonexistent-topic");
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);

        assertEquals(1, entries.size());
        KafkaWireHelpers.TopicMetadataEntry entry = entries.get(0);
        assertEquals(3, entry.errorCode, "Error code should be UNKNOWN_TOPIC_OR_PARTITION (3)");
        assertEquals(new UUID(0L, 0L), entry.topicId, "Topic ID should be zero UUID");
        assertEquals("nonexistent-topic", entry.topicName);
        assertTrue(entry.partitionIds.isEmpty(), "Unknown topic should have no partitions");
    }

    @Test
    @Timeout(15)
    @DisplayName("2.2 Existing topic returns correct metadata with UUID and partitions")
    void existingTopicReturnsCorrectMetadata() throws IOException {
        createTopic("test-topic", 1);

        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client", "test-topic");
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);

        assertEquals(1, entries.size());
        KafkaWireHelpers.TopicMetadataEntry entry = entries.get(0);
        assertEquals(0, entry.errorCode);
        assertNotEquals(new UUID(0L, 0L), entry.topicId);
        assertEquals("test-topic", entry.topicName);
        assertEquals(1, entry.partitionIds.size());
        assertEquals(0, entry.partitionIds.get(0));
    }

    @Test
    @Timeout(15)
    @DisplayName("2.3 Multi-partition topic returns all partitions")
    void multiPartitionTopicReturnsAllPartitions() throws IOException {
        createTopic("multi-part", 3);

        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client", "multi-part");
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);

        assertEquals(1, entries.size());
        assertEquals(0, entries.get(0).errorCode);
        assertEquals(3, entries.get(0).partitionIds.size());
        assertEquals(List.of(0, 1, 2), entries.get(0).partitionIds);
    }

    @Test
    @Timeout(15)
    @DisplayName("2.4 Topics returned in alphabetical order (zeta, alpha, beta)")
    void topicsReturnedAlphabetically() throws IOException {
        createTopic("zeta", 1);
        createTopic("alpha", 1);
        createTopic("beta", 1);

        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client", "zeta", "alpha", "beta");
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);

        assertEquals(3, entries.size());
        assertEquals("alpha", entries.get(0).topicName);
        assertEquals("beta", entries.get(1).topicName);
        assertEquals("zeta", entries.get(2).topicName);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Topic name with special characters")
    void topicNameWithSpecialCharacters() throws IOException {
        String specialName = "my.topic-with_special~chars!";
        createTopic(specialName, 1);

        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client", specialName);
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);

        assertEquals(1, entries.size());
        assertEquals(0, entries.get(0).errorCode);
        assertEquals(specialName, entries.get(0).topicName);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Mix of existing and non-existing topics")
    void mixedExistingAndUnknownTopics() throws IOException {
        createTopic("exists", 1);

        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client", "exists", "nope");
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);

        assertEquals(2, entries.size());
        assertEquals("exists", entries.get(0).topicName);
        assertEquals(0, entries.get(0).errorCode);
        assertEquals("nope", entries.get(1).topicName);
        assertEquals(3, entries.get(1).errorCode);
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Empty topic list")
    void emptyTopicList() throws IOException {
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client");
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);
        assertTrue(entries.isEmpty());
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Single partition with correct leader")
    void singlePartitionLeaderInfo() throws IOException {
        createTopic("leader-test", 1);

        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildDescribeTopicPartitionsRequest(
                corrId, "test-client", "leader-test");
        ByteBuffer resp = send(req);

        List<KafkaWireHelpers.TopicMetadataEntry> entries =
                KafkaWireHelpers.parseDescribeResponse(resp);
        assertEquals(1, entries.size());
        assertEquals(0, entries.get(0).errorCode);
        assertEquals(List.of(0), entries.get(0).partitionIds);
    }
}
