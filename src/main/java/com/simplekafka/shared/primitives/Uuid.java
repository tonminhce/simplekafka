package com.simplekafka.shared.primitives;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Reads and writes UUID (16-byte) values in Kafka wire format.
 */
public final class Uuid {

    private Uuid() {
    }

    /**
     * Reads a 16-byte UUID from the buffer.
     */
    public static UUID read(ByteBuffer buffer) {
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * Writes a 16-byte UUID to the buffer.
     */
    public static void write(ByteBuffer buffer, UUID value) {
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
    }

    /**
     * Returns the number of bytes used by UUID.
     */
    public static int size() {
        return 16;
    }

    /**
     * Writes a zero-filled UUID (all zeros) for unknown topics.
     */
    public static void writeZero(ByteBuffer buffer) {
        buffer.putLong(0L);
        buffer.putLong(0L);
    }
}
