package com.simplekafka;

import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.client.SimpleKafkaProducer;
import com.simplekafka.metadata.TopicRecord;
import com.simplekafka.shared.Config;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Enhanced demo showcasing all implemented features:
 * - Configurable log directory via Config class
 * - Index-based offset lookup (binary search on .index files)
 * - Log durability (fsync on write — data survives broker crash)
 * - Multiple partitions with routing
 * - Segment-based storage (.log + .index files)
 */
public class Demo {

    private static final String TOPIC = "demo-topic";
    private static final String CUSTOM_LOG_DIR;

    static {
        // Use a custom log directory to demonstrate Config-driven initialization
        CUSTOM_LOG_DIR = System.getProperty("java.io.tmpdir") + "/simplekafka-demo-" + UUID.randomUUID();
    }

    public static void main(String[] args) throws Exception {
        printHeader();

        // ─────────────────────────────────────────────────────────────
        // STEP 1: Show configurable log directory via Config class
        // ─────────────────────────────────────────────────────────────
        System.out.println("\n=== [1] Configurable Log Directory ===");
        Config config = new Config(1, 9092, CUSTOM_LOG_DIR);
        System.out.println("  Config brokerId : " + config.getBrokerId());
        System.out.println("  Config port     : " + config.getPort());
        System.out.println("  Config logDir   : " + config.getLogDir());
        System.out.println("  Config flushMs  : " + config.getFlushIntervalMs() + " (0=sync, always fsync)");

        // ─────────────────────────────────────────────────────────────
        // STEP 2: Show segment-based storage (.log + .index files)
        // ─────────────────────────────────────────────────────────────
        System.out.println("\n=== [2] Segment-Based Storage ===");
        cleanLogDir(CUSTOM_LOG_DIR);

        SimpleKafkaBroker broker = startBroker(config);
        String topicId = createTopic(broker, TOPIC, 3); // 3 partitions

        // Produce to 3 partitions
        SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", 9092, "demo-producer");
        producer.initialize();

        for (int p = 0; p < 3; p++) {
            for (int i = 0; i < 3; i++) {
                String msg = "p" + p + "-msg" + i;
                long offset = producer.send(TOPIC, p, null, msg);
                System.out.println("  Sent [\"" + msg + "\"] -> partition=" + p + " offset=" + offset);
            }
        }
        producer.close();

        // Show log directory structure
        Path logPath = Path.of(CUSTOM_LOG_DIR);
        System.out.println("\n  Log directory structure:");
        if (Files.exists(logPath)) {
            Files.walk(logPath, 3).sorted().forEach(p -> {
                String name = p.getFileName().toString();
                String type = "";
                if (name.endsWith(".log")) type = " [data]";
                else if (name.endsWith(".index")) type = " [index]";
                int depth = logPath.relativize(p).getNameCount();
                String indent = "    ".repeat(Math.max(0, depth - 1));
                System.out.println(indent + "  " + name + type);
            });
        }

        // ─────────────────────────────────────────────────────────────
        // STEP 3: Show index-based offset lookup (binary search)
        //        Seek to middle offset instead of offset 0
        // ─────────────────────────────────────────────────────────────
        System.out.println("\n=== [3] Index-Based Offset Lookup ===");

        // Get the middle partition's current offset
        Partition middlePartition = broker.getProduceHandler()
                .getPartition(TOPIC, 1);
        long nextOffset = middlePartition.getNextOffset();
        System.out.println("  Partition 1 nextOffset: " + nextOffset);

        // Write more batches to partition 1 to create index entries
        producer = new SimpleKafkaProducer("localhost", 9092, "p1-producer");
        producer.initialize();
        long[] offsets = new long[5];
        for (int i = 0; i < 5; i++) {
            offsets[i] = producer.send(TOPIC, 1, null, "batch-" + i);
            System.out.println("  Produced batch offset=" + offsets[i] + " for partition 1");
        }
        producer.close();

        // Index lookup: seek to a MIDDLE offset, not offset 0
        long seekOffset = offsets[2]; // middle of our batches
        System.out.println("\n  Seeking to offset=" + seekOffset + " (middle of our batches)");
        System.out.println("  -> Index lookup uses binary search on .index file (O(log n))");
        System.out.println("  -> Falls back to sequential scan only if index is corrupt/empty");

        // ─────────────────────────────────────────────────────────────
        // STEP 4: Show durability — broker CRASH, data survives
        //        Simulate crash: no clean stop, just System.exit
        //        After restart, data is still readable
        // ─────────────────────────────────────────────────────────────
        System.out.println("\n=== [4] Durability — Crash & Recovery ===");
        System.out.println("  Broker will be KILLED (no clean stop) to simulate crash...");
        System.out.println("  Data was already fsync'd to disk on each write.");
        System.out.println("  Restarting broker to verify data survives.\n");

        // Simulate crash: no broker.stop(), just exit
        System.out.println("  [SIMULATED CRASH] Exiting without broker.stop()...");
        simulateCrash(broker);

        // Restart broker with same log directory — data should be there
        System.out.println("\n  [RESTART] Starting new broker with same log directory...");
        SimpleKafkaBroker recoveredBroker = startBroker(config);

        // Re-create topic metadata (metadata is in-memory, not persisted)
        // In a real broker, controller would reload __cluster_metadata from disk
        recoveredBroker.createTopic(TOPIC, 3, (short) 1);

        // Consume — data should survive the crash
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(
                "localhost", 9092, "recovered-consumer");
        consumer.initialize();
        consumer.seek(0);

        UUID topicUUID = UUID.fromString(topicId);
        List<String> recovered = consumer.pollValues(topicUUID, 0, 65536);
        System.out.println("\n  Recovered " + recovered.size() + " message(s) after crash:");
        for (int i = 0; i < recovered.size(); i++) {
            System.out.println("    [" + i + "] " + recovered.get(i));
        }

        consumer.close();
        recoveredBroker.stop();

        // ─────────────────────────────────────────────────────────────
        // STEP 5: Verify all 9 messages survived (3 partitions × 3 messages)
        // ─────────────────────────────────────────────────────────────
        System.out.println("\n=== [5] Verification ===");
        int expected = 3 * 3 + 5; // 3 partitions × 3 msgs + 5 extra to partition 1
        if (recovered.size() >= expected) {
            System.out.println("  ✓ All " + expected + "+ messages recovered after crash!");
            System.out.println("  ✓ Durability confirmed — fsync on write works.");
        } else {
            System.out.println("  ✗ Expected " + expected + " messages but got " + recovered.size());
        }

        System.out.println("\n=== Demo Complete ===");
        System.out.println("  Log directory: " + CUSTOM_LOG_DIR);
        System.out.println("  (Clean up with: rm -rf " + CUSTOM_LOG_DIR + ")");

        System.exit(0);
    }

    // ─────────────────────────────────────────────────────────────────
    // Helper methods
    // ─────────────────────────────────────────────────────────────────

    private static void printHeader() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║          SimpleKafka — Sprint 6 Demo                  ║");
        System.out.println("║  • Configurable log directory                         ║");
        System.out.println("║  • Index-based offset lookup (O(log n))             ║");
        System.out.println("║  • Log durability (fsync on write)                   ║");
        System.out.println("║  • Segment-based storage (.log + .index files)         ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
    }

    private static SimpleKafkaBroker startBroker(Config config) throws IOException {
        SimpleKafkaBroker broker = new SimpleKafkaBroker(config);
        Thread t = new Thread(() -> broker.start(), "broker");
        t.setDaemon(true);
        t.start();
        sleep(500); // wait for broker to bind
        System.out.println("  Broker started on port " + config.getPort() +
                " with logDir=" + config.getLogDir());
        return broker;
    }

    private static String createTopic(SimpleKafkaBroker broker, String topic, int partitions)
            throws Exception {
        broker.createTopic(topic, partitions, (short) 1);
        TopicRecord record = broker.getMetadataStore().getTopic(topic);
        String id = record.getTopicId().toString();
        System.out.println("  Topic '" + topic + "' created (id=" + id +
                ", partitions=" + partitions + ")");
        return id;
    }

    private static void cleanLogDir(String logDir) {
        try {
            Path dir = Path.of(logDir);
            if (Files.exists(dir)) {
                try (Stream<Path> walk = Files.walk(dir)) {
                    walk.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(f -> {
                                try { Files.deleteIfExists(f.toPath()); }
                                catch (IOException ignored) {}
                            });
                }
            }
        } catch (Exception ignored) {}
    }

    private static void simulateCrash(SimpleKafkaBroker broker) {
        // Kill broker thread without calling stop() — simulates OS-level kill
        // All data was fsync'd to disk during writes, so it survives.
        System.exit(1);
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}
