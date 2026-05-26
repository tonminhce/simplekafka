package com.simplekafka.shared;

import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int32;

import java.nio.ByteBuffer;

/**
 * Utility methods for Kafka wire-format protocol encoding.
 */
public final class Protocol {

    private Protocol() {
    }

    /**
     * Wraps a response body with the Kafka message framing:
     * message_size(INT32) + response_header_v1 + body.
     */
    public static ByteBuffer frameResponse(ResponseHeader header, ByteBuffer body) {
        int bodySize = body.remaining();
        int headerSize = ResponseHeader.size();
        int messageSize = headerSize + bodySize;

        ByteBuffer frame = ByteBuffer.allocate(Int32.size() + messageSize);
        Int32.write(frame, messageSize);
        header.write(frame);
        frame.put(body);
        frame.flip();
        return frame;
    }

    /**
     * Writes an empty TAG_BUFFER (0 tags).
     */
    public static void writeEmptyTagBuffer(ByteBuffer buffer) {
        buffer.put((byte) 0);
    }

    /**
     * Writes a nullable COMPACT_STRING that is null.
     */
    public static void writeNullCompactString(ByteBuffer buffer) {
        CompactString.write(buffer, null);
    }
}
