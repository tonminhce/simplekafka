package com.simplekafka.broker.handlers;

import com.simplekafka.shared.ErrorCodes;
import com.simplekafka.shared.Protocol;
import com.simplekafka.shared.RequestHeader;
import com.simplekafka.shared.ResponseHeader;
import com.simplekafka.shared.primitives.CompactArray;
import com.simplekafka.shared.primitives.Int16;

import java.nio.ByteBuffer;

/**
 * Handles ApiVersion requests (API key 18).
 * <p>
 * Returns supported API keys and their version ranges.
 */
public class ApiVersionsHandler {

    /**
     * Processes an ApiVersions request and returns the framed response.
     *
     * @param header the parsed request header
     * @return ByteBuffer ready to be written to the channel
     */
    public ByteBuffer handle(RequestHeader header) {
        ByteBuffer body = buildResponseBody(header.getApiVersion());
        ResponseHeader responseHeader = new ResponseHeader(header.getCorrelationId());
        return Protocol.frameResponse(responseHeader, body);
    }

    /**
     * Builds the ApiVersions response body.
     * <p>
     * Layout:
     * error_code(INT16)
     * api_versions_count(COMPACT_ARRAY length)
     * api_versions[]:
     *   api_key(INT16) + min_version(INT16) + max_version(INT16) + TAG_BUFFER
     * TAG_BUFFER
     */
    private ByteBuffer buildResponseBody(short requestedVersion) {
        // Validate requested version is within supported range.
        short errorCode = ErrorCodes.NONE;
        if (requestedVersion < 0 || requestedVersion > 4) {
            errorCode = ErrorCodes.UNSUPPORTED_VERSION;
        }

        ApiVersionEntry[] entries = getSupportedApis();
        int size = computeBodySize(entries);
        ByteBuffer body = ByteBuffer.allocate(size);

        // error_code
        Int16.write(body, errorCode);

        // api_versions array (compact)
        CompactArray.writeCount(body, entries.length);
        for (ApiVersionEntry entry : entries) {
            Int16.write(body, entry.apiKey);
            Int16.write(body, entry.minVersion);
            Int16.write(body, entry.maxVersion);
            Protocol.writeEmptyTagBuffer(body); // TAG_BUFFER per entry
        }

        // trailing TAG_BUFFER
        Protocol.writeEmptyTagBuffer(body);

        body.flip();
        return body;
    }

    private int computeBodySize(ApiVersionEntry[] entries) {
        int size = Int16.size(); // error_code
        size += CompactArray.sizeCount(entries.length);
        for (ApiVersionEntry ignored : entries) {
            size += Int16.size() + Int16.size() + Int16.size() + 1; // key + min + max + TAG_BUFFER
        }
        size += 1; // trailing TAG_BUFFER
        return size;
    }

    /**
     * Returns the supported API versions.
     * Ordered by API key.
     */
    private ApiVersionEntry[] getSupportedApis() {
        return new ApiVersionEntry[]{
                new ApiVersionEntry((short) 0, (short) 0, (short) 11),   // Produce
                new ApiVersionEntry((short) 1, (short) 0, (short) 16),   // Fetch
                new ApiVersionEntry((short) 18, (short) 0, (short) 4),   // ApiVersions
                new ApiVersionEntry((short) 75, (short) 0, (short) 0),   // DescribeTopicPartitions
        };
    }

    /**
     * Immutable record for an API version range.
     */
    private record ApiVersionEntry(short apiKey, short minVersion, short maxVersion) {
    }
}
