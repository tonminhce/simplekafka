package com.simplekafka.shared;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Central configuration for the SimpleKafka broker.
 * <p>
 * Provides a single source of truth for all configurable values.
 * Can be loaded from a properties file or constructed with defaults.
 */
public class Config {

    private int brokerId = 1;
    private int port = 9092;
    private String logDir = "/tmp/kraft-combined-logs";
    private long flushIntervalMs = 0;

    public Config() {}

    public Config(int brokerId, int port, String logDir) {
        this.brokerId = brokerId;
        this.port = port;
        this.logDir = logDir;
    }

    /**
     * Loads configuration from a properties file.
     * <p>
     * Supported keys:
     * - broker.id (default: 1)
     * - listeners (format: PLAINTEXT://host:port, default port: 9092)
     * - log.dirs (default: /tmp/kraft-combined-logs)
     * - log.flush.interval.ms (default: 0 = synchronous)
     */
    public static Config fromProperties(File file) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            props.load(fis);
        }

        Config config = new Config();
        config.brokerId = Integer.parseInt(props.getProperty("broker.id", "1"));

        String listeners = props.getProperty("listeners", "PLAINTEXT://:9092");
        int colonIdx = listeners.lastIndexOf(':');
        config.port = colonIdx >= 0
                ? Integer.parseInt(listeners.substring(colonIdx + 1))
                : 9092;

        config.logDir = props.getProperty("log.dirs", "/tmp/kraft-combined-logs");
        long flushMs = Long.parseLong(
                props.getProperty("log.flush.interval.ms", "0"));
        if (flushMs < 0) {
            throw new IllegalArgumentException(
                    "log.flush.interval.ms must be non-negative, got: " + flushMs);
        }
        config.flushIntervalMs = flushMs;

        return config;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getPort() {
        return port;
    }

    public String getLogDir() {
        return logDir;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public void setFlushIntervalMs(long flushIntervalMs) {
        if (flushIntervalMs < 0) {
            throw new IllegalArgumentException(
                    "flushIntervalMs must be non-negative, got: " + flushIntervalMs);
        }
        this.flushIntervalMs = flushIntervalMs;
    }
}
