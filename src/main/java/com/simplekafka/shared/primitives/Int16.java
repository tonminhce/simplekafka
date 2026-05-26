package com.simplekafka.shared.primitives;

import java.nio.ByteBuffer;

/**
 * Reads and writes INT16 (2-byte signed) values in Kafka wire format.
 */
public final class Int16 {

    private Int16() {
    }

    /**
     * Reads a 2-byte signed integer from the buffer.
     */
    public static short read(ByteBuffer buffer) {
        return buffer.getShort();
    }

    /**
     * Writes a 2-byte signed integer to the buffer.
     */
    public static void write(ByteBuffer buffer, short value) {
        buffer.putShort(value);
    }

    /**
     * Returns the number of bytes used by INT16.
     */
    public static int size() {
        return 2;
    }
}
