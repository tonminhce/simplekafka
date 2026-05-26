package com.simplekafka.shared;

import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int16;
import com.simplekafka.shared.primitives.Int32;

import java.nio.ByteBuffer;

/**
 * Represents a parsed Kafka request header v2.
 * <p>
 * Layout: api_key(INT16) + api_version(INT16) + correlation_id(INT32) + client_id(COMPACT_STRING) + TAG_BUFFER
 */
public class RequestHeader {

    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;

    private RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    /**
     * Parses a request header v2 from the given buffer.
     * The buffer must be positioned at the start of the header (after the 4-byte message_size).
     */
    public static RequestHeader parse(ByteBuffer buffer) {
        short apiKey = Int16.read(buffer);
        short apiVersion = Int16.read(buffer);
        int correlationId = Int32.read(buffer);
        String clientId = CompactString.read(buffer);
        skipTaggedFields(buffer);
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }

    /**
     * Returns the fixed size of the header fields (without variable-length client_id and tags).
     */
    public static int fixedSize() {
        return Int16.size() + Int16.size() + Int32.size();
    }

    public short getApiKey() {
        return apiKey;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public String getClientId() {
        return clientId;
    }

    /**
     * Skips a TAG_BUFFER (tagged fields) section.
     * Reads the number of tags, then skips each tag's ID, size, and data.
     */
    static void skipTaggedFields(ByteBuffer buffer) {
        int numTags = CompactString.readUnsignedVarint(buffer);
        for (int i = 0; i < numTags; i++) {
            CompactString.readUnsignedVarint(buffer); // tagId
            int size = CompactString.readUnsignedVarint(buffer); // size
            buffer.position(buffer.position() + size); // skip tag data
        }
    }
}
