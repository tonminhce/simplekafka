package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a single log segment: a .log file and its .index file.
 * <p>
 * Each segment stores RecordBatch entries sequentially.
 * The index file maps offsets to byte positions in the log file.
 */
public class LogSegment {

    private static final Logger LOGGER = Logger.getLogger(LogSegment.class.getName());

    private final File logFile;
    private final File indexFile;
    private final long baseOffset;
    private final long flushIntervalMs;
    private FileChannel logChannel;
    private FileChannel indexChannel;
    private RandomAccessFile logRaf;
    private RandomAccessFile indexRaf;
    private long nextPosition;
    private volatile long lastFlushTimeNanos;

    /**
     * Creates or opens an existing log segment.
     *
     * @param logFile   the .log file
     * @param indexFile the .index file
     * @param baseOffset the base offset of this segment
     */
    public LogSegment(File logFile, File indexFile, long baseOffset) throws IOException {
        this(logFile, indexFile, baseOffset, 0);
    }

    public LogSegment(File logFile, File indexFile, long baseOffset, long flushIntervalMs) throws IOException {
        this.logFile = logFile;
        this.indexFile = indexFile;
        this.baseOffset = baseOffset;
        this.flushIntervalMs = flushIntervalMs;
        this.lastFlushTimeNanos = System.nanoTime();

        boolean exists = logFile.exists();
        this.logRaf = new RandomAccessFile(logFile, "rw");
        this.logChannel = logRaf.getChannel();

        if (exists) {
            this.nextPosition = (int) logChannel.size();
        } else {
            this.nextPosition = 0;
        }

        // Open index file
        this.indexRaf = new RandomAccessFile(indexFile, "rw");
        this.indexChannel = indexRaf.getChannel();

        LOGGER.fine("LogSegment opened: " + logFile.getAbsolutePath() + " at position " + nextPosition);
    }

    /**
     * Appends a RecordBatch to the log file and updates the index.
     *
     * @param recordBatch the raw RecordBatch bytes (excluding base_offset and partition_leader_epoch)
     * @return the file position where the batch was written
     */
    public synchronized int append(ByteBuffer recordBatch) throws IOException {
        long position = nextPosition;

        // Write base_offset (8 bytes) + batch data
        ByteBuffer writeBuf = ByteBuffer.allocate(8 + recordBatch.remaining());
        writeBuf.putLong(baseOffset); // placeholder base_offset, overwritten by Partition
        writeBuf.put(recordBatch);
        writeBuf.flip();

        logChannel.write(writeBuf, position);
        nextPosition = position + writeBuf.limit();

        return (int) position;
    }

    /**
     * Appends a pre-framed RecordBatch (already including base_offset) directly.
     *
     * @param recordBatch the complete record batch bytes including base_offset
     * @return the file position where the batch was written
     */
    public synchronized long appendRaw(byte[] recordBatch) throws IOException {
        long position = nextPosition;

        ByteBuffer writeBuf = ByteBuffer.wrap(recordBatch);
        logChannel.write(writeBuf, position);
        nextPosition = position + recordBatch.length;

        maybeForceLog();
        return position;
    }

    /**
     * Writes an index entry mapping an offset to a file position.
     * Each entry: offset (INT32, relative) + position (INT32).
     */
    public synchronized void writeIndexEntry(int relativeOffset, int position) throws IOException {
        ByteBuffer entry = ByteBuffer.allocate(8);
        entry.putInt(relativeOffset);
        entry.putInt(position);
        entry.flip();

        long indexPos = indexChannel.size();
        indexChannel.write(entry, indexPos);
        maybeForceIndex();
    }

    /**
     * Reads data from the log starting at the given position.
     *
     * @param position byte position in the log file
     * @param maxBytes maximum bytes to read
     * @return ByteBuffer with the data read
     */
    public synchronized ByteBuffer read(long position, int maxBytes) throws IOException {
        long available = nextPosition - position;
        int toRead = (int) Math.min(available, (long) maxBytes);
        if (toRead <= 0) {
            return ByteBuffer.allocate(0);
        }

        ByteBuffer buf = ByteBuffer.allocate(toRead);
        // Loop until the buffer is fully read — FileChannel.read may return short.
        long filePos = position;
        while (buf.hasRemaining()) {
            int read = logChannel.read(buf, filePos + buf.position());
            if (read == -1) {
                break;
            }
        }
        buf.flip();
        return buf;
    }

    /**
     * Looks up the byte position for a given relative offset using the index file.
     * Uses binary search on the index entries.
     * <p>
     * Each index entry is 8 bytes: relative_offset (INT32) + byte_position (INT32).
     *
     * @param relativeOffset the relative offset to look up
     * @return the byte position, or -1 if the index is empty or corrupt
     */
    public synchronized int lookupPosition(int relativeOffset) throws IOException {
        long indexSize = indexChannel.size();
        if (indexSize < 8) {
            return -1; // Empty or no entries
        }

        int entryCount = (int) (indexSize / 8);
        if (entryCount <= 0) {
            return -1;
        }

        // Read all index entries into memory for binary search.
        // Cast to long before multiplication to prevent int overflow (e.g., entryCount ~250M → overflow)
        ByteBuffer indexBuf = ByteBuffer.allocate((int) ((long) entryCount * 8));
        indexChannel.read(indexBuf, 0);
        indexBuf.flip();

        // Binary search for the largest relativeOffset <= target.
        int lo = 0;
        int hi = entryCount - 1;
        int bestPosition = -1;

        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2; // overflow-safe
            int entryOffset = indexBuf.getInt(mid * 8);
            int entryPosition = indexBuf.getInt(mid * 8 + 4);

            if (entryOffset <= relativeOffset) {
                bestPosition = entryPosition;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        return bestPosition;
    }

    /**
     * Returns the byte size of this segment.
     */
    public long size() {
        return nextPosition;
    }

    /**
     * Returns the configured flush interval in milliseconds.
     */
    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    /**
     * Returns the timestamp of the last successful flush in nanoseconds.
     * Note: Uses nanoTime internally (monotonic), not wall-clock time.
     */
    public long getLastFlushTime() {
        return lastFlushTimeNanos;
    }

    /**
     * Forces an immediate fsync of both log and index channels.
     */
    public synchronized void forceFlush() throws IOException {
        if (logChannel != null && logChannel.isOpen()) {
            logChannel.force(false);
        }
        if (indexChannel != null && indexChannel.isOpen()) {
            indexChannel.force(false);
        }
        lastFlushTimeNanos = System.nanoTime();
    }

    /**
     * Conditionally fsyncs the log channel based on flush interval.
     * If flushIntervalMs == 0, always fsync (synchronous mode).
     * Otherwise, only fsync when the interval has elapsed.
     */
    private void maybeForceLog() throws IOException {
        if (flushIntervalMs == 0) {
            logChannel.force(false);
            indexChannel.force(false);  // also sync index in synchronous mode
            lastFlushTimeNanos = System.nanoTime();
        } else {
            long now = System.nanoTime();
            long elapsedNanos = now - lastFlushTimeNanos;
            // Guard against nanoTime wraparound (extremely rare) or negative (shouldn't happen)
            if (elapsedNanos >= flushIntervalMs * 1_000_000L) {
                logChannel.force(false);
                indexChannel.force(false);  // also sync index in batched mode
                lastFlushTimeNanos = now;
            }
        }
    }

    /**
     * Conditionally fsyncs the index channel based on flush interval.
     * Always fsyncs in synchronous mode (flushIntervalMs == 0).
     */
    private void maybeForceIndex() throws IOException {
        if (flushIntervalMs == 0) {
            indexChannel.force(false);
            lastFlushTimeNanos = System.nanoTime();
        }
        // In batched mode, index fsync happens together with log fsync in maybeForceLog
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    /**
     * Closes the segment files.
     */
    public synchronized void close() {
        IOException forceLogError = null;
        IOException forceIndexError = null;

        // Attempt force — errors are collected but never prevent close.
        if (logChannel != null && logChannel.isOpen()) {
            try {
                logChannel.force(false);
            } catch (IOException e) {
                forceLogError = e;
            }
        }
        if (indexChannel != null && indexChannel.isOpen()) {
            try {
                indexChannel.force(false);
            } catch (IOException e) {
                forceIndexError = e;
            }
        }

        // Close channels regardless of force() outcome — this is the critical step.
        if (logChannel != null) {
            try { logChannel.close(); } catch (IOException ignored) {}
            logChannel = null;
        }
        if (indexChannel != null) {
            try { indexChannel.close(); } catch (IOException ignored) {}
            indexChannel = null;
        }
        if (logRaf != null) {
            try { logRaf.close(); } catch (IOException ignored) {}
            logRaf = null;
        }
        if (indexRaf != null) {
            try { indexRaf.close(); } catch (IOException ignored) {}
            indexRaf = null;
        }

        // Log force errors after close is done — close always succeeds.
        if (forceLogError != null || forceIndexError != null) {
            LOGGER.log(Level.WARNING, "Error flushing segment during close", forceLogError);
            LOGGER.log(Level.WARNING, "Error flushing index during close", forceIndexError);
        }
    }
}
