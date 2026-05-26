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
    private FileChannel logChannel;
    private FileChannel indexChannel;
    private RandomAccessFile logRaf;
    private RandomAccessFile indexRaf;
    private int nextPosition;

    /**
     * Creates or opens an existing log segment.
     *
     * @param logFile   the .log file
     * @param indexFile the .index file
     * @param baseOffset the base offset of this segment
     */
    public LogSegment(File logFile, File indexFile, long baseOffset) throws IOException {
        this.logFile = logFile;
        this.indexFile = indexFile;
        this.baseOffset = baseOffset;

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
        int position = nextPosition;

        // Write base_offset (8 bytes) + batch data
        ByteBuffer writeBuf = ByteBuffer.allocate(8 + recordBatch.remaining());
        writeBuf.putLong(baseOffset); // placeholder base_offset, overwritten by Partition
        writeBuf.put(recordBatch);
        writeBuf.flip();

        logChannel.write(writeBuf, position);
        nextPosition = position + writeBuf.limit();

        return position;
    }

    /**
     * Appends a pre-framed RecordBatch (already including base_offset) directly.
     *
     * @param recordBatch the complete record batch bytes including base_offset
     * @return the file position where the batch was written
     */
    public synchronized int appendRaw(byte[] recordBatch) throws IOException {
        int position = nextPosition;

        ByteBuffer writeBuf = ByteBuffer.wrap(recordBatch);
        logChannel.write(writeBuf, position);
        nextPosition = position + recordBatch.length;

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
    }

    /**
     * Reads data from the log starting at the given position.
     *
     * @param position byte position in the log file
     * @param maxBytes maximum bytes to read
     * @return ByteBuffer with the data read
     */
    public synchronized ByteBuffer read(int position, int maxBytes) throws IOException {
        int available = nextPosition - position;
        int toRead = Math.min(available, maxBytes);
        if (toRead <= 0) {
            return ByteBuffer.allocate(0);
        }

        ByteBuffer buf = ByteBuffer.allocate(toRead);
        // Loop until the buffer is fully read — FileChannel.read may return short.
        while (buf.hasRemaining()) {
            int read = logChannel.read(buf, position + buf.position());
            if (read == -1) {
                break;
            }
        }
        buf.flip();
        return buf;
    }

    /**
     * Returns the byte size of this segment.
     */
    public int size() {
        return nextPosition;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    /**
     * Closes the segment files.
     */
    public synchronized void close() {
        try {
            if (logChannel != null) logChannel.close();
            if (indexChannel != null) indexChannel.close();
            if (logRaf != null) logRaf.close();
            if (indexRaf != null) indexRaf.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing segment", e);
        }
    }
}
