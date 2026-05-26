package com.simplekafka.shared.primitives;

import java.nio.ByteBuffer;

/**
 * Reads and writes COMPACT_ARRAY values in Kafka wire format.
 * <p>
 * Compact encoding: unsigned varint count + elements.
 * The count is stored as (actualCount + 1) where 0 means null.
 */
public final class CompactArray {

    private CompactArray() {
    }

    /**
     * Reads the element count from a compact array header.
     * Returns -1 if the array is null.
     */
    public static int readCount(ByteBuffer buffer) {
        int raw = CompactString.readUnsignedVarint(buffer) - 1;
        return raw;
    }

    /**
     * Writes the element count as a compact array header.
     */
    public static void writeCount(ByteBuffer buffer, int count) {
        CompactString.writeUnsignedVarint(buffer, count + 1);
    }

    /**
     * Returns the byte size of a compact array count header.
     */
    public static int sizeCount(int count) {
        return CompactString.sizeUnsignedVarint(count + 1);
    }
}
