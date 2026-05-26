package com.simplekafka.shared;

import com.simplekafka.shared.primitives.Int32;

import java.nio.ByteBuffer;

/**
 * Represents a Kafka response header v1.
 * <p>
 * Layout: correlation_id(INT32) + TAG_BUFFER
 */
public class ResponseHeader {

    private static final int TAG_BUFFER_ZERO = 0;

    private final int correlationId;

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Writes the response header v1 to the buffer.
     */
    public void write(ByteBuffer buffer) {
        Int32.write(buffer, correlationId);
        buffer.put((byte) TAG_BUFFER_ZERO); // empty TAG_BUFFER
    }

    /**
     * Returns the byte size of a response header v1.
     */
    public static int size() {
        return Int32.size() + 1; // correlation_id + TAG_BUFFER(1 byte for 0 tags)
    }
}
