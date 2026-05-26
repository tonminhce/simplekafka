package com.simplekafka;

import com.simplekafka.broker.Partition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for index-based offset lookup and fallback sequential scan.
 */
class IndexLookupTest {

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

    /**
     * Builds a minimal valid RecordBatch with the given number of records.
     * Uses the same proven approach as SegmentRollingTest.
     */
    private byte[] buildRecordBatch(int recordsCount) {
        int recordSize = 10; // matches SegmentRollingTest with 0 padding
        int batchSize = 49 + recordsCount * recordSize;
        ByteBuffer buf = ByteBuffer.allocate(batchSize);

        buf.putInt(batchSize - 4);     // batch_length
        buf.putInt(0);                  // partition_leader_epoch
        buf.put((byte) 2);             // magic
        buf.putInt(0);                  // crc
        buf.putShort((short) 0);       // attributes
        buf.putInt(recordsCount - 1);  // last_offset_delta
        buf.putLong(System.currentTimeMillis()); // first_timestamp
        buf.putLong(System.currentTimeMillis()); // max_timestamp
        buf.putLong(0);                // producer_id
        buf.putShort((short) 0);       // producer_epoch
        buf.putInt(0);                  // base_sequence
        buf.putInt(recordsCount);      // records_count

        for (int i = 0; i < recordsCount; i++) {
            buf.put((byte) 0); // attributes
            buf.put((byte) 0); // timestamp_delta
            buf.put((byte) 0); // offset_delta
            buf.put((byte) 0); // key length = null
            buf.put((byte) 1); // value length (varint: 1 = 0 bytes)
            buf.put((byte) 0); // headers count
        }

        buf.flip();
        byte[] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    @Test
    void testIndexLookupAfterSingleWrite() throws IOException {
        byte[] batch = buildRecordBatch(1);
        long offset = partition.appendRecordBatch(batch);

        ByteBuffer data = partition.readMessages(offset, 4096);
        assertTrue(data.remaining() > 0, "Should read data back via index lookup");
    }

    @Test
    void testIndexLookupAfterMultipleWrites() throws IOException {
        byte[] batch = buildRecordBatch(1);

        long off0 = partition.appendRecordBatch(batch);
        long off1 = partition.appendRecordBatch(batch);
        long off2 = partition.appendRecordBatch(batch);

        assertEquals(0, off0);
        assertEquals(1, off1);
        assertEquals(2, off2);

        ByteBuffer data0 = partition.readMessages(0, 4096);
        assertTrue(data0.remaining() > 0, "Should read from offset 0");

        ByteBuffer data1 = partition.readMessages(1, 4096);
        assertTrue(data1.remaining() > 0, "Should read from offset 1");

        ByteBuffer data2 = partition.readMessages(2, 4096);
        assertTrue(data2.remaining() > 0, "Should read from offset 2");
    }

    @Test
    void testIndexLookupReadSequentially() throws IOException {
        byte[] batch = buildRecordBatch(1);

        for (int i = 0; i < 5; i++) {
            partition.appendRecordBatch(batch);
        }

        ByteBuffer allData = partition.readMessages(0, 65536);
        assertTrue(allData.remaining() > 0, "Should read all batches from offset 0");

        ByteBuffer partialData = partition.readMessages(3, 65536);
        assertTrue(partialData.remaining() > 0, "Should read batches from offset 3");
        assertTrue(partialData.remaining() < allData.remaining(),
                "Partial read should be smaller than full read");
    }

    @Test
    void testIndexLookupAtExactOffset() throws IOException {
        byte[] batch = buildRecordBatch(1);

        partition.appendRecordBatch(batch);
        partition.appendRecordBatch(batch);

        ByteBuffer data0 = partition.readMessages(0, 4096);
        ByteBuffer data1 = partition.readMessages(1, 4096);

        assertTrue(data0.remaining() > 0);
        assertTrue(data1.remaining() > 0);
    }

    @Test
    void testIndexLookupPastAllData() throws IOException {
        byte[] batch = buildRecordBatch(1);
        partition.appendRecordBatch(batch);

        ByteBuffer data = partition.readMessages(999, 1024);
        assertEquals(0, data.remaining(), "Should return empty for offset past data");
    }

    @Test
    void testIndexLookupWithZeroMaxBytes() throws IOException {
        byte[] batch = buildRecordBatch(1);
        partition.appendRecordBatch(batch);

        ByteBuffer data = partition.readMessages(0, 0);
        assertEquals(0, data.remaining(), "Should return empty for 0 maxBytes");
    }

    @Test
    void testIndexLookupWithSmallMaxBytes() throws IOException {
        byte[] batch = buildRecordBatch(1);
        partition.appendRecordBatch(batch);

        ByteBuffer data = partition.readMessages(0, 10);
        assertTrue(data.remaining() <= 10, "Should not return more than maxBytes");
    }

    @Test
    void testMultipleRecordsPerBatchAdvancesOffset() throws IOException {
        byte[] batch = buildRecordBatch(3);

        long offset = partition.appendRecordBatch(batch);
        assertEquals(0, offset);
        assertEquals(3, partition.getNextOffset());

        ByteBuffer data = partition.readMessages(0, 4096);
        assertTrue(data.remaining() > 0);
    }

    @Test
    void testIndexLookupWithManyBatches() throws IOException {
        byte[] batch = buildRecordBatch(1);

        for (int i = 0; i < 50; i++) {
            partition.appendRecordBatch(batch);
        }

        assertEquals(50, partition.getNextOffset());

        for (int offset = 0; offset < 50; offset += 10) {
            ByteBuffer data = partition.readMessages(offset, 4096);
            assertTrue(data.remaining() > 0,
                    "Should read from offset " + offset);
        }
    }
}
