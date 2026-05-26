package com.simplekafka;

import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.client.SimpleKafkaProducer;
import com.simplekafka.metadata.TopicRecord;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Quick demo: starts broker, produces messages, consumes them back.
 */
public class Demo {

    private static final String TOPIC = "demo-topic";
    private static final String LOG_DIR = "D:\\tmp\\kraft-combined-logs";

    public static void main(String[] args) throws Exception {
        // Clean up old data
        cleanLogDir();

        int port = 9092;

        // 1. Start broker
        System.out.println("=== Starting SimpleKafka Broker on port " + port + " ===");
        SimpleKafkaBroker broker = new SimpleKafkaBroker(port, 1);
        Thread brokerThread = new Thread(() -> broker.start(), "broker");
        brokerThread.setDaemon(true);
        brokerThread.start();

        // Wait for broker ready
        Thread.sleep(500);

        // 2. Create topic
        System.out.println("=== Creating topic: " + TOPIC + " ===");
        broker.createTopic(TOPIC, 1, (short) 1);
        TopicRecord topicRecord = broker.getMetadataStore().getTopic(TOPIC);
        UUID topicId = topicRecord.getTopicId();
        System.out.println("Topic created: " + TOPIC + " (id=" + topicId + ")");

        // 3. Produce messages
        System.out.println("\n=== Producing messages ===");
        SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", port, "demo-producer");
        producer.initialize();

        String[] messages = {"hello", "world", "from", "simplekafka"};
        for (String msg : messages) {
            long offset = producer.send(TOPIC, 0, null, msg);
            System.out.println("  Sent: \"" + msg + "\" -> offset=" + offset);
        }
        producer.close();
        System.out.println("Producer closed.");

        // 4. Consume messages
        System.out.println("\n=== Consuming messages ===");
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", port, "demo-consumer");
        consumer.initialize();
        consumer.seek(0);

        List<String> records = consumer.pollValues(topicId, 0, 65536);
        System.out.println("  Received " + records.size() + " message(s):");

        for (int i = 0; i < records.size(); i++) {
            System.out.println("  [" + i + "] " + records.get(i));
        }

        consumer.close();
        System.out.println("Consumer closed.");

        // 5. Done
        System.out.println("\n=== Demo complete! ===");
        broker.stop();
        brokerThread.join(3000);
    }

    private static void cleanLogDir() throws IOException {
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
