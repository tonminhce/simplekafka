package com.simplekafka.shared.primitives;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Reads and writes COMPACT_STRING values in Kafka wire format.
 * <p>
 * Compact encoding: unsigned varint length + UTF-8 bytes.
 * The length is stored as (actualLength + 1) to distinguish null from empty.
 */
public final class CompactString {

    private CompactString() {
    }

    /**
     * Reads a compact string from the buffer.
     *
     * @return the decoded string, or null if the length byte is 0 (null sentinel)
     */
    public static String read(ByteBuffer buffer) {
        int length = readUnsignedVarint(buffer) - 1;
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Writes a compact string to the buffer.
     */
    public static void write(ByteBuffer buffer, String value) {
        if (value == null) {
            writeUnsignedVarint(buffer, 0);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarint(buffer, bytes.length + 1);
        buffer.put(bytes);
    }

    /**
     * Returns the encoded size of a compact string.
     */
    public static int size(String value) {
        if (value == null) {
            return sizeUnsignedVarint(0);
        }
        int byteLen = value.getBytes(StandardCharsets.UTF_8).length;
        return sizeUnsignedVarint(byteLen + 1) + byteLen;
    }

    /**
     * Reads an unsigned varint from the buffer.
     */
    public static int readUnsignedVarint(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        int b;
        do {
            if (!buffer.hasRemaining() || shift > 28) {
                throw new IllegalArgumentException("Malformed varint");
            }
            b = buffer.get() & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    /**
     * Writes an unsigned varint to the buffer.
     */
    public static void writeUnsignedVarint(ByteBuffer buffer, int value) {
        while ((value & ~0x7F) != 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    /**
     * Returns the number of bytes needed to encode a varint.
     */
    public static int sizeUnsignedVarint(int value) {
        int size = 1;
        while ((value & ~0x7F) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }
}
