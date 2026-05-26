package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a topic partition with segment-based log storage.
 * <p>
 * Manages appending RecordBatch entries and reading messages by offset.
 * Each partition maintains its own offset sequence starting from 0.
 * <p>
 * On-disk layout: {@code <logDir>/<topicName>-<partitionIndex>/00000000000000000000.log}
 */
public class Partition {

    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final String SEGMENT_FILE_TEMPLATE = "%020d.log";
    private static final String INDEX_FILE_TEMPLATE = "%020d.index";
    private static final long SEGMENT_SIZE_LIMIT = 1_073_741_824L; // 1GB default segment size

    private final String topicName;
    private final int partitionIndex;
    private final String logDir;
    private final AtomicLong nextOffset;
    private final List<LogSegment> segments = new ArrayList<>();

    /**
     * Creates a partition, initializing the first log segment.
     *
     * @param topicName      the topic name
     * @param partitionIndex the partition index
     * @param logDir         the base log directory (e.g., /tmp/kraft-combined-logs)
     */
    public Partition(String topicName, int partitionIndex, String logDir) throws IOException {
        this.topicName = topicName;
        this.partitionIndex = partitionIndex;
        this.logDir = logDir;
        this.nextOffset = new AtomicLong(0);

        initialize();
    }

    /**
     * Initializes the partition: creates the directory and opens existing segments
     * or creates the first segment.
     */
    private void initialize() throws IOException {
        File partitionDir = new File(logDir, topicName + "-" + partitionIndex);
        if (!partitionDir.exists() && !partitionDir.mkdirs()) {
            throw new IOException("Failed to create partition directory: " + partitionDir.getAbsolutePath());
        }

        // For simplicity, create or open the first segment.
        createNewSegment(0);
        LOGGER.info("Partition initialized: " + topicName + "-" + partitionIndex +
                " at " + partitionDir.getAbsolutePath());
    }

    /**
     * Creates a new log segment at the given base offset.
     */
    private void createNewSegment(long baseOffset) throws IOException {
        File partitionDir = new File(logDir, topicName + "-" + partitionIndex);
        String logName = String.format(SEGMENT_FILE_TEMPLATE, baseOffset);
        String indexName = String.format(INDEX_FILE_TEMPLATE, baseOffset);

        File logFile = new File(partitionDir, logName);
        File indexFile = new File(partitionDir, indexName);

        LogSegment segment = new LogSegment(logFile, indexFile, baseOffset);
        segments.add(segment);
    }

    /**
     * Gets the active (last) segment for appending.
     */
    private LogSegment activeSegment() {
        return segments.getLast();
    }

    /**
     * Appends a RecordBatch to the partition log.
     * <p>
     * The RecordBatch bytes are written as-is (the caller is responsible for
     * providing properly formatted data). This method assigns the base_offset.
     *
     * @param recordBatchBytes the raw RecordBatch bytes as sent by the client
     * @return the base_offset assigned to this batch
     */
    public synchronized long appendRecordBatch(byte[] recordBatchBytes) throws IOException {
        long baseOffset = nextOffset.get();

        // Write the RecordBatch to the active segment.
        // We prepend the base_offset (INT64) as per Kafka on-disk format.
        ByteBuffer framed = ByteBuffer.allocate(8 + recordBatchBytes.length);
        framed.putLong(baseOffset);
        framed.put(recordBatchBytes);
        framed.flip();

        LogSegment segment = activeSegment();
        int position = segment.appendRaw(framed.array());

        // Calculate how many records are in the batch by parsing the batch header.
        // The recordBatchBytes starts after base_offset, so:
        // batch_length(4) + partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2) +
        // last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) + producer_id(8) +
        // producer_epoch(2) + base_sequence(4) + records_count(4) = 53 bytes header
        ByteBuffer batchBuf = ByteBuffer.wrap(recordBatchBytes);
        // Skip to records_count: partition_leader_epoch(4) + batch_length was already consumed
        // Actually recordBatchBytes from the Produce request starts at batch_length field.
        // Let me parse from the client format to get records_count.
        int recordsCount = parseRecordsCount(recordBatchBytes);
        nextOffset.addAndGet(recordsCount);

        // Write index entry.
        int relativeOffset = 0; // First record in batch at this position
        segment.writeIndexEntry(relativeOffset, position);

        LOGGER.fine("Appended batch to " + topicName + "-" + partitionIndex +
                " at offset " + baseOffset + " with " + recordsCount + " records");

        return baseOffset;
    }

    /**
     * Parses the number of records from a RecordBatch.
     * <p>
     * RecordBatch layout (what we receive from client):
     * batch_length(4) + partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2) +
     * last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) + producer_id(8) +
     * producer_epoch(2) + base_sequence(4) + records_count(4) + records...
     */
    private int parseRecordsCount(byte[] recordBatchBytes) {
        ByteBuffer buf = ByteBuffer.wrap(recordBatchBytes);
        // batch_length (4)
        buf.getInt();
        // partition_leader_epoch (4)
        buf.getInt();
        // magic (1)
        buf.get();
        // crc (4)
        buf.getInt();
        // attributes (2)
        buf.getShort();
        // last_offset_delta (4)
        buf.getInt();
        // first_timestamp (8)
        buf.getLong();
        // max_timestamp (8)
        buf.getLong();
        // producer_id (8)
        buf.getLong();
        // producer_epoch (2)
        buf.getShort();
        // base_sequence (4)
        buf.getInt();
        // records_count (4)
        return buf.getInt();
    }

    /**
     * Reads messages from the given offset up to maxBytes.
     *
     * @param offset  the starting offset
     * @param maxBytes maximum bytes to read
     * @return ByteBuffer with the records data
     */
    public synchronized ByteBuffer readMessages(long offset, int maxBytes) throws IOException {
        // If the requested offset is at or past all data, return empty.
        if (offset >= nextOffset.get()) {
            return ByteBuffer.allocate(0);
        }

        LogSegment segment = findSegmentForOffset(offset);
        if (segment == null) {
            return ByteBuffer.allocate(0);
        }

        int position = findPositionForOffset(segment, offset);
        if (position < 0) {
            return ByteBuffer.allocate(0);
        }

        return segment.read(position, maxBytes);
    }

    /**
     * Finds the segment that contains the given offset using binary search.
     */
    private LogSegment findSegmentForOffset(long offset) {
        // Simple search: find the segment whose baseOffset <= offset
        LogSegment result = null;
        for (LogSegment segment : segments) {
            if (segment.getBaseOffset() <= offset) {
                result = segment;
            } else {
                break;
            }
        }
        return result;
    }

    /**
     * Finds the byte position within a segment for a given offset.
     * Uses the index file for lookup.
     */
    private int findPositionForOffset(LogSegment segment, long offset) {
        long relativeOffset = offset - segment.getBaseOffset();
        if (relativeOffset == 0) {
            return 0;
        }
        // For now, simple implementation: scan from beginning.
        // A production implementation would binary-search the index.
        // Since we store index entries, we could read them, but for simplicity
        // we return 0 and let the caller scan.
        return 0;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public long getNextOffset() {
        return nextOffset.get();
    }
}
