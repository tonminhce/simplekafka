package com.simplekafka.shared.primitives;

import java.nio.ByteBuffer;

/**
 * Reads and writes INT32 (4-byte signed) values in Kafka wire format.
 */
public final class Int32 {

    private Int32() {
    }

    /**
     * Reads a 4-byte signed integer from the buffer.
     */
    public static int read(ByteBuffer buffer) {
        return buffer.getInt();
    }

    /**
     * Writes a 4-byte signed integer to the buffer.
     */
    public static void write(ByteBuffer buffer, int value) {
        buffer.putInt(value);
    }

    /**
     * Returns the number of bytes used by INT32.
     */
    public static int size() {
        return 4;
    }
}
