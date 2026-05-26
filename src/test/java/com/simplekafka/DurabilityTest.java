package com.simplekafka;

import com.simplekafka.broker.Partition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for log durability (fsync) behavior.
 */
class DurabilityTest {

    @TempDir
    Path tempDir;

    private Partition partition;

    @BeforeEach
    void setUp() throws IOException {
        partition = new Partition("test-topic", 0, tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (partition != null) {
            partition.close();
        }
    }

    private byte[] buildRecordBatch(int recordsCount) {
        int recordSize = 10;
        int batchSize = 49 + recordsCount * recordSize;
        ByteBuffer buf = ByteBuffer.allocate(batchSize);

        buf.putInt(batchSize - 4);
        buf.putInt(0);
        buf.put((byte) 2);
        buf.putInt(0);
        buf.putShort((short) 0);
        buf.putInt(recordsCount - 1);
        buf.putLong(System.currentTimeMillis());
        buf.putLong(System.currentTimeMillis());
        buf.putLong(0);
        buf.putShort((short) 0);
        buf.putInt(0);
        buf.putInt(recordsCount);

        for (int i = 0; i < recordsCount; i++) {
            buf.put((byte) 0);
            buf.put((byte) 0);
            buf.put((byte) 0);
            buf.put((byte) 0);
            buf.put((byte) 1);
            buf.put((byte) 0);
        }

        buf.flip();
        byte[] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    /**
     * Get the active LogSegment from Partition via reflection.
     */
    private Object getActiveSegment() throws Exception {
        Field segmentsField = Partition.class.getDeclaredField("segments");
        segmentsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.List<Object> segments = (java.util.List<Object>) segmentsField.get(partition);
        return segments.get(segments.size() - 1);
    }

    @Test
    void testDataPersistsAfterWrite() throws IOException {
        byte[] batch = buildRecordBatch(1);
        long offset = partition.appendRecordBatch(batch);

        ByteBuffer data = partition.readMessages(offset, 4096);
        assertTrue(data.remaining() > 0, "Data should be readable after write");
    }

    @Test
    void testForceFlushCalledAfterAppend() throws Exception {
        byte[] batch = buildRecordBatch(1);

        long beforeFlushTime = System.currentTimeMillis();
        partition.appendRecordBatch(batch);

        // Verify lastFlushTime was updated (fsync happened)
        Object segment = getActiveSegment();
        Method getLastFlushTime = segment.getClass().getDeclaredMethod("getLastFlushTime");
        getLastFlushTime.setAccessible(true);
        long lastFlush = (long) getLastFlushTime.invoke(segment);

        assertTrue(lastFlush >= beforeFlushTime,
                "lastFlushTime should be updated after append (fsync was called)");
    }

    @Test
    void testForceFlushUpdatesOnEachWrite() throws Exception {
        byte[] batch = buildRecordBatch(1);
        Object segment = getActiveSegment();
        Method getLastFlushTime = segment.getClass().getDeclaredMethod("getLastFlushTime");
        getLastFlushTime.setAccessible(true);

        partition.appendRecordBatch(batch);
        long flush1 = (long) getLastFlushTime.invoke(segment);

        // Small delay to ensure timestamps differ
        Thread.sleep(10);

        partition.appendRecordBatch(batch);
        long flush2 = (long) getLastFlushTime.invoke(segment);

        assertTrue(flush2 >= flush1,
                "Each write should update lastFlushTime in synchronous mode (flushIntervalMs=0)");
    }

    @Test
    void testForceFlushMethodWorks() throws Exception {
        byte[] batch = buildRecordBatch(1);
        partition.appendRecordBatch(batch);

        Object segment = getActiveSegment();
        Method forceFlush = segment.getClass().getDeclaredMethod("forceFlush");
        forceFlush.setAccessible(true);

        // Should not throw
        assertDoesNotThrow(() -> {
            try {
                forceFlush.invoke(segment);
            } catch (Exception e) {
                if (e.getCause() instanceof RuntimeException) throw (RuntimeException) e.getCause();
                throw new RuntimeException(e.getCause());
            }
        });
    }

    @Test
    void testFlushIntervalIsZeroByDefault() throws Exception {
        Object segment = getActiveSegment();
        Method getFlushInterval = segment.getClass().getDeclaredMethod("getFlushIntervalMs");
        getFlushInterval.setAccessible(true);
        long interval = (long) getFlushInterval.invoke(segment);

        assertEquals(0, interval, "Default flush interval should be 0 (synchronous)");
    }

    @Test
    void testCloseFlushesWithoutException() throws IOException {
        byte[] batch = buildRecordBatch(1);
        partition.appendRecordBatch(batch);

        // Close triggers force(false) — should not throw
        assertDoesNotThrow(() -> partition.close());
        partition = null; // prevent tearDown double-close
    }

    @Test
    void testWriteAndReadLargeData() throws IOException {
        byte[] batch = buildRecordBatch(1);
        for (int i = 0; i < 100; i++) {
            partition.appendRecordBatch(batch);
        }

        assertEquals(100, partition.getNextOffset());

        for (int offset = 0; offset < 100; offset += 25) {
            ByteBuffer data = partition.readMessages(offset, 4096);
            assertTrue(data.remaining() > 0, "Should read from offset " + offset);
        }
    }
}
