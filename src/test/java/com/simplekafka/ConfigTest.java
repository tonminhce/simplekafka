package com.simplekafka;

import com.simplekafka.shared.Config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Config loading and defaults.
 */
class ConfigTest {

    @TempDir
    Path tempDir;

    @Test
    void testDefaultConfig() {
        Config config = new Config();

        assertEquals(1, config.getBrokerId());
        assertEquals(9092, config.getPort());
        assertEquals("/tmp/kraft-combined-logs", config.getLogDir());
        assertEquals(0, config.getFlushIntervalMs());
    }

    @Test
    void testParameterizedConstructor() {
        Config config = new Config(2, 9093, "/var/lib/kafka/logs");

        assertEquals(2, config.getBrokerId());
        assertEquals(9093, config.getPort());
        assertEquals("/var/lib/kafka/logs", config.getLogDir());
    }

    @Test
    void testSetters() {
        Config config = new Config();
        config.setBrokerId(5);
        config.setPort(9192);
        config.setLogDir("/data/kafka");
        config.setFlushIntervalMs(1000);

        assertEquals(5, config.getBrokerId());
        assertEquals(9192, config.getPort());
        assertEquals("/data/kafka", config.getLogDir());
        assertEquals(1000, config.getFlushIntervalMs());
    }

    @Test
    void testFromPropertiesFile() throws IOException {
        File propsFile = tempDir.resolve("server.properties").toFile();
        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("broker.id=3\n");
            writer.write("listeners=PLAINTEXT://0.0.0.0:9094\n");
            writer.write("log.dirs=/var/lib/kafka/data\n");
            writer.write("log.flush.interval.ms=5000\n");
        }

        Config config = Config.fromProperties(propsFile);

        assertEquals(3, config.getBrokerId());
        assertEquals(9094, config.getPort());
        assertEquals("/var/lib/kafka/data", config.getLogDir());
        assertEquals(5000, config.getFlushIntervalMs());
    }

    @Test
    void testFromPropertiesWithDefaults() throws IOException {
        File propsFile = tempDir.resolve("minimal.properties").toFile();
        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("# empty file, all defaults\n");
        }

        Config config = Config.fromProperties(propsFile);

        assertEquals(1, config.getBrokerId());
        assertEquals(9092, config.getPort());
        assertEquals("/tmp/kraft-combined-logs", config.getLogDir());
        assertEquals(0, config.getFlushIntervalMs());
    }

    @Test
    void testFromPropertiesWithMissingLogDirs() throws IOException {
        File propsFile = tempDir.resolve("partial.properties").toFile();
        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("broker.id=7\n");
            // log.dirs not set — should use default
        }

        Config config = Config.fromProperties(propsFile);

        assertEquals(7, config.getBrokerId());
        assertEquals("/tmp/kraft-combined-logs", config.getLogDir());
    }

    @Test
    void testBrokerWithConfigUsesLogDir() throws IOException {
        String customDir = tempDir.resolve("custom-logs").toString();
        Config config = new Config(1, 19092, customDir);

        assertEquals(customDir, config.getLogDir());
    }
}
