package com.simplekafka.metadata;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads and writes the __cluster_metadata log file.
 * <p>
 * Stores TOPIC_RECORD and PARTITION_RECORD entries.
 * Path: /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
 * <p>
 * Each record in the log:
 * - record_type (INT8): 2 = TOPIC_RECORD, 3 = PARTITION_RECORD
 * - record_length (INT32): length of the remaining payload
 * - payload: type-specific data
 */
public class ClusterMetadataLog {

    private static final Logger LOGGER = Logger.getLogger(ClusterMetadataLog.class.getName());
    private static final String METADATA_DIR = "/tmp/kraft-combined-logs/__cluster_metadata-0";
    private static final String METADATA_FILE = "00000000000000000000.log";
    private static final byte TOPIC_RECORD_TYPE = 2;
    private static final byte PARTITION_RECORD_TYPE = 3;

    private final File logFile;
    private FileChannel writeChannel;

    public ClusterMetadataLog() {
        this(METADATA_DIR);
    }

    public ClusterMetadataLog(String metadataDir) {
        File dir = new File(metadataDir);
        if (!dir.exists() && !dir.mkdirs()) {
            LOGGER.warning("Failed to create metadata directory: " + dir.getAbsolutePath());
        }
        this.logFile = new File(dir, METADATA_FILE);
    }

    /**
     * Opens the metadata log for writing. Creates the file if it doesn't exist.
     */
    public synchronized void open() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
        this.writeChannel = raf.getChannel();
        LOGGER.info("Cluster metadata log opened: " + logFile.getAbsolutePath());
    }

    /**
     * Writes a TOPIC_RECORD to the metadata log.
     */
    public synchronized void writeTopicRecord(TopicRecord topic) throws IOException {
        byte[] nameBytes = topic.getTopicName().getBytes(StandardCharsets.UTF_8);
        int payloadSize = 1 + nameBytes.length + 16; // name_len(1) + name + uuid(16)

        ByteBuffer record = ByteBuffer.allocate(1 + 4 + payloadSize);
        record.put(TOPIC_RECORD_TYPE);
        record.putInt(payloadSize);
        record.put((byte) nameBytes.length);
        record.put(nameBytes);
        record.putLong(topic.getTopicId().getMostSignificantBits());
        record.putLong(topic.getTopicId().getLeastSignificantBits());
        record.flip();

        writeChannel.write(record, writeChannel.size());
        writeChannel.force(true);

        LOGGER.fine("Wrote TOPIC_RECORD: " + topic.getTopicName());
    }

    /**
     * Writes a PARTITION_RECORD to the metadata log.
     */
    public synchronized void writePartitionRecord(PartitionRecord partition) throws IOException {
        // Calculate payload size
        int replicasSize = 4 * partition.getReplicas().size();
        int isrSize = 4 * partition.getIsr().size();
        byte[] topicNameBytes = new byte[0]; // We don't store topic_name in partition record
        int payloadSize = 4 + 16 + 4 + 4 + 4 + replicasSize + 4 + isrSize;

        ByteBuffer record = ByteBuffer.allocate(1 + 4 + payloadSize);
        record.put(PARTITION_RECORD_TYPE);
        record.putInt(payloadSize);
        record.putInt(partition.getPartitionId());
        record.putLong(partition.getTopicId().getMostSignificantBits());
        record.putLong(partition.getTopicId().getLeastSignificantBits());
        record.putInt(partition.getLeaderId());
        record.putInt(partition.getLeaderEpoch());
        record.putInt(partition.getReplicas().size());
        for (int r : partition.getReplicas()) {
            record.putInt(r);
        }
        record.putInt(partition.getIsr().size());
        for (int i : partition.getIsr()) {
            record.putInt(i);
        }
        record.flip();

        writeChannel.write(record, writeChannel.size());
        writeChannel.force(true);

        LOGGER.fine("Wrote PARTITION_RECORD: partition " + partition.getPartitionId());
    }

    /**
     * Reads all records from the metadata log and returns them as a list of parsed objects.
     * The list can contain both TopicRecord and PartitionRecord instances.
     */
    public List<Object> readRecords() throws IOException {
        List<Object> records = new ArrayList<>();

        if (!logFile.exists() || logFile.length() == 0) {
            return records;
        }

        try (RandomAccessFile raf = new RandomAccessFile(logFile, "r");
             FileChannel channel = raf.getChannel()) {

            ByteBuffer buf = ByteBuffer.allocate((int) channel.size());
            channel.read(buf, 0);
            buf.flip();

            while (buf.remaining() > 5) { // at least type(1) + length(4)
                byte type = buf.get();
                int length = buf.getInt();

                if (buf.remaining() < length) {
                    break; // Incomplete record
                }

                byte[] payload = new byte[length];
                buf.get(payload);
                ByteBuffer payloadBuf = ByteBuffer.wrap(payload);

                switch (type) {
                    case TOPIC_RECORD_TYPE -> {
                        int nameLen = payloadBuf.get() & 0xFF;
                        byte[] nameBytes = new byte[nameLen];
                        payloadBuf.get(nameBytes);
                        String name = new String(nameBytes, StandardCharsets.UTF_8);
                        long msb = payloadBuf.getLong();
                        long lsb = payloadBuf.getLong();
                        records.add(new TopicRecord(name, new UUID(msb, lsb)));
                    }
                    case PARTITION_RECORD_TYPE -> {
                        int partitionId = payloadBuf.getInt();
                        long msb = payloadBuf.getLong();
                        long lsb = payloadBuf.getLong();
                        UUID topicId = new UUID(msb, lsb);
                        int leaderId = payloadBuf.getInt();
                        int leaderEpoch = payloadBuf.getInt();
                        int replicaCount = payloadBuf.getInt();
                        List<Integer> replicas = new ArrayList<>();
                        for (int i = 0; i < replicaCount; i++) {
                            replicas.add(payloadBuf.getInt());
                        }
                        int isrCount = payloadBuf.getInt();
                        List<Integer> isr = new ArrayList<>();
                        for (int i = 0; i < isrCount; i++) {
                            isr.add(payloadBuf.getInt());
                        }
                        records.add(new PartitionRecord(partitionId, topicId, leaderId, leaderEpoch, replicas, isr));
                    }
                    default -> LOGGER.warning("Unknown record type: " + type);
                }
            }
        }

        return records;
    }

    /**
     * Loads metadata from the log into the ClusterMetadataStore.
     */
    public void loadInto(ClusterMetadataStore store) throws IOException {
        List<Object> records = readRecords();
        for (Object record : records) {
            if (record instanceof TopicRecord topic) {
                store.addTopic(topic);
            } else if (record instanceof PartitionRecord partition) {
                store.addPartition(partition);
            }
        }
        LOGGER.info("Loaded " + records.size() + " records from metadata log");
    }

    /**
     * Closes the metadata log.
     */
    public synchronized void close() {
        if (writeChannel != null) {
            try {
                writeChannel.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing metadata log", e);
            }
        }
    }
}
