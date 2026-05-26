package com.simplekafka;

import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.client.SimpleKafkaProducer;
import com.simplekafka.metadata.ClusterMetadataStore;
import com.simplekafka.metadata.TopicRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Epic 5: Cluster Management & Integration")
class EndToEndTest {

    private static final String LOG_DIR = "/tmp/kraft-combined-logs";

    private SimpleKafkaBroker broker;
    private int port;
    private Thread brokerThread;

    @BeforeEach
    void startBroker() throws Exception {
        cleanLogDir();
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }
        broker = new SimpleKafkaBroker(port, 1);

        brokerThread = new Thread(() -> broker.start(), "e2e-broker");
        brokerThread.setDaemon(true);
        brokerThread.start();

        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 100);
                break;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
    }

    @AfterEach
    void stopBroker() throws Exception {
        if (broker != null) broker.stop();
        if (brokerThread != null) brokerThread.join(5000);
        cleanLogDir();
    }

    private ByteBuffer send(ByteBuffer req) throws IOException {
        return KafkaWireHelpers.sendAndReceive("localhost", port, req);
    }

    @Test
    @Timeout(15)
    @DisplayName("5.1 Topic/partition creation records persisted to __cluster_metadata")
    void metadataPersistedToClusterMetadataLog() throws IOException {
        broker.createTopic("persisted-topic", 2, (short) 1);

        ClusterMetadataStore store = broker.getMetadataStore();
        TopicRecord topic = store.getTopic("persisted-topic");
        assertNotNull(topic);
        assertNotNull(topic.getTopicId());

        var partitions = store.getPartitions(topic.getTopicId());
        assertEquals(2, partitions.size());

        Path metadataFile = Path.of(LOG_DIR, "__cluster_metadata-0", "00000000000000000000.log");
        assertTrue(Files.exists(metadataFile));
        assertTrue(Files.size(metadataFile) > 0);
    }

    @Test
    @Timeout(15)
    @DisplayName("5.2 Controller election works")
    void controllerElectionWorks() {
        assertTrue(broker.isController());
    }

    @Test
    @Timeout(15)
    @DisplayName("5.3 End-to-end: producer sends hello, consumer receives hello")
    void endToEndProducerConsumerHello() throws Exception {
        broker.createTopic("e2e-topic", 1, (short) 1);
        UUID topicId = broker.getMetadataStore().getTopic("e2e-topic").getTopicId();

        // Produce using SimpleKafkaProducer
        SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", port, "e2e-producer");
        producer.initialize();
        try {
            long baseOffset = producer.send("e2e-topic", 0, null, "hello");
            assertEquals(0L, baseOffset, "First message should have offset 0");
        } finally {
            producer.close();
        }

        // Fetch using SimpleKafkaConsumer
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", port, "e2e-consumer");
        consumer.initialize();
        try {
            consumer.seek(0);
            List<byte[]> batches = consumer.poll(topicId, 0, 65536);
            assertFalse(batches.isEmpty(), "Should receive at least one batch");

            String recordsStr = new String(batches.get(0), StandardCharsets.UTF_8);
            assertTrue(recordsStr.contains("hello"), "Records should contain 'hello'");
        } finally {
            consumer.close();
        }
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Broker restart - metadata survives")
    void brokerRestartMetadataSurvives() throws Exception {
        broker.createTopic("survivor", 1, (short) 1);
        UUID originalId = broker.getMetadataStore().getTopic("survivor").getTopicId();
        broker.stop();
        if (brokerThread != null) brokerThread.join(5000);

        SimpleKafkaBroker broker2 = new SimpleKafkaBroker(port, 1);
        Thread t = new Thread(() -> broker2.start());
        t.setDaemon(true);
        t.start();

        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 100);
                break;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }

        TopicRecord topic = broker2.getMetadataStore().getTopic("survivor");
        assertNotNull(topic, "Topic should survive broker restart");
        assertEquals(originalId, topic.getTopicId());
        assertEquals("survivor", topic.getTopicName());

        broker2.stop();
        t.join(5000);
    }

    @Test
    @Timeout(30)
    @DisplayName("Edge: Multiple topics end-to-end produce and fetch")
    void multipleTopicsEndToEnd() throws Exception {
        broker.createTopic("multi-a", 1, (short) 1);
        broker.createTopic("multi-b", 1, (short) 1);
        UUID idA = broker.getMetadataStore().getTopic("multi-a").getTopicId();
        UUID idB = broker.getMetadataStore().getTopic("multi-b").getTopicId();

        // Produce to both using SimpleKafkaProducer
        SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", port, "multi-producer");
        producer.initialize();
        try {
            long offsetA = producer.send("multi-a", 0, null, "msg-a");
            assertEquals(0L, offsetA);
            long offsetB = producer.send("multi-b", 0, null, "msg-b");
            assertEquals(0L, offsetB);
        } finally {
            producer.close();
        }

        // Fetch from both using SimpleKafkaConsumer
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", port, "multi-consumer");
        consumer.initialize();
        try {
            consumer.seek(0);
            List<byte[]> batchesA = consumer.poll(idA, 0, 65536);
            assertFalse(batchesA.isEmpty(), "Topic A should have data");
            assertTrue(new String(batchesA.get(0), StandardCharsets.UTF_8).contains("msg-a"));

            consumer.seek(0);
            List<byte[]> batchesB = consumer.poll(idB, 0, 65536);
            assertFalse(batchesB.isEmpty(), "Topic B should have data");
            assertTrue(new String(batchesB.get(0), StandardCharsets.UTF_8).contains("msg-b"));
        } finally {
            consumer.close();
        }
    }

    private void cleanLogDir() throws IOException {
        Path dir = Path.of(LOG_DIR);
        if (Files.exists(dir)) {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(f -> {
                            try { Files.deleteIfExists(f.toPath()); } catch (IOException ignored) {}
                        });
            } catch (Exception ignored) {}
        }
    }
}
