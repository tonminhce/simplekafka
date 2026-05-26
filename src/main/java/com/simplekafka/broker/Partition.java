package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
    private final long flushIntervalMs;
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
        this(topicName, partitionIndex, logDir, 0);
    }

    /**
     * Creates a partition with a configurable flush interval.
     *
     * @param topicName       the topic name
     * @param partitionIndex  the partition index
     * @param logDir          the base log directory
     * @param flushIntervalMs fsync interval in ms (0 = synchronous, always fsync)
     */
    public Partition(String topicName, int partitionIndex, String logDir, long flushIntervalMs) throws IOException {
        this.topicName = topicName;
        this.partitionIndex = partitionIndex;
        this.logDir = logDir;
        this.flushIntervalMs = flushIntervalMs;
        this.nextOffset = new AtomicLong(0);

        initialize();
    }

    /**
     * Initializes the partition: creates the directory and opens existing segments
     * or creates the first segment.
     */
    public void initialize() throws IOException {
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

        LogSegment segment = new LogSegment(logFile, indexFile, baseOffset, flushIntervalMs);
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
        // Validate minimum RecordBatch header size before writing.
        if (recordBatchBytes.length < 49) {
            throw new IOException("RecordBatch too short: " + recordBatchBytes.length + " bytes (minimum 49)");
        }

        // Parse records count first to reject malformed data before writing to disk.
        int recordsCount = parseRecordsCount(recordBatchBytes);
        if (recordsCount < 0) {
            throw new IOException("Invalid records_count: " + recordsCount);
        }

        long baseOffset = nextOffset.get();

        // Check if the active segment would exceed the size limit with this batch.
        // We check BEFORE writing so we don't write to a full segment.
        // The on-disk entry is 8 (base_offset) + recordBatchBytes.length bytes.
        long entrySize = 8L + recordBatchBytes.length;

        // Roll segment if needed, then write — in a loop to handle concurrent rolls.
        // Using a do-while loop handles the case where two threads both pass the
        // size check: the second thread will find the new (smaller) segment and retry.
        LogSegment segment;
        do {
            segment = activeSegment();
            if (segment.size() + entrySize <= SEGMENT_SIZE_LIMIT) {
                break;
            }
            // Roll to a new segment.
            long newBaseOffset = baseOffset;
            LOGGER.info("Rolling segment for " + topicName + "-" + partitionIndex +
                    ": old segment size=" + segment.size() + " bytes, new segment at offset " + newBaseOffset);
            segment.close();
            createNewSegment(newBaseOffset);
            // After rolling, loop back to re-check with the new active segment.
        } while (true);

        // Write the RecordBatch to the selected segment.
        // We prepend the base_offset (INT64) as per Kafka on-disk format.
        ByteBuffer framed = ByteBuffer.allocate(8 + recordBatchBytes.length);
        framed.putLong(baseOffset);
        framed.put(recordBatchBytes);
        framed.flip();

        long position = segment.appendRaw(framed.array());

        nextOffset.addAndGet(recordsCount);

        // Write index entry with the actual relative offset for this batch.
        // relativeOffset = batch's baseOffset - segment's baseOffset
        int relativeOffset = (int) (baseOffset - segment.getBaseOffset());
        segment.writeIndexEntry(relativeOffset, (int) position);

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
     * Returns the segment with the largest baseOffset that is <= the requested offset.
     */
    private LogSegment findSegmentForOffset(long offset) {
        if (segments.isEmpty()) {
            return null;
        }

        // Binary search: find the rightmost segment with baseOffset <= offset.
        int lo = 0;
        int hi = segments.size() - 1;
        int result = -1;

        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2; // prevent overflow vs (lo + hi) / 2
            long midBase = segments.get(mid).getBaseOffset();
            if (midBase <= offset) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        return result >= 0 ? segments.get(result) : null;
    }

    /**
     * Finds the byte position within a segment for a given offset.
     * First attempts index-based lookup via the .index file.
     * Falls back to sequential scan if the index is unavailable.
     * <p>
     * On-disk layout per batch: base_offset(INT64) + batch_length(INT32) + batch_data(batch_length bytes)
     */
    private int findPositionForOffset(LogSegment segment, long offset) throws IOException {
        if (offset == segment.getBaseOffset()) {
            return 0;
        }

        // Try index-based lookup first.
        long offsetDelta = offset - segment.getBaseOffset();
        // Guard against offset < baseOffset (shouldn't happen, but defensive)
        if (offsetDelta < 0) {
            return 0;
        }
        // Guard against overflow of int: if delta exceeds Integer.MAX_VALUE,
        // the index cannot store it (INT32). Fall back to sequential scan.
        if (offsetDelta > Integer.MAX_VALUE) {
            return -1;  // signals fallback to sequential scan
        }
        int relativeOffset = (int) offsetDelta;

        int indexPosition = segment.lookupPosition(relativeOffset);
        if (indexPosition >= 0) {
            return indexPosition;
        }

        // Fallback: sequential scan (original behavior)
        ByteBuffer data = segment.read(0, (int) Math.min(segment.size(), (long) Integer.MAX_VALUE));
        int limit = data.limit();
        int pos = 0;

        while (pos + 12 <= limit) { // at least base_offset(8) + batch_length(4)
            long batchBaseOffset = data.getLong(pos);
            int batchLength = data.getInt(pos + 8);

            if (batchLength <= 0) {
                break; // corrupt entry, stop scanning
            }

            int entrySize = 8 + 4 + batchLength; // base_offset + batch_length + batch_data

            if (batchBaseOffset == offset) {
                return pos;
            }

            int nextPos = pos + entrySize;

            // If this is the last batch, and our offset falls within it, return current pos.
            if (nextPos > limit) {
                if (offset >= batchBaseOffset) {
                    return pos;
                }
                break;
            }

            // Peek at the next batch's base_offset.
            if (nextPos + 8 <= limit) {
                long nextBaseOffset = data.getLong(nextPos);
                if (nextBaseOffset > offset && offset >= batchBaseOffset) {
                    return pos;
                }
            }

            pos = nextPos;
        }

        return pos; // best-effort: return last known position
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

    /**
     * Closes all log segments and releases resources.
     */
    public synchronized void close() {
        for (LogSegment segment : segments) {
            segment.close();
        }
    }
}
