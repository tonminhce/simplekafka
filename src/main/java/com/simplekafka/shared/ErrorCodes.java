package com.simplekafka.shared;

/**
 * Standard Kafka error codes used across all API responses.
 */
public final class ErrorCodes {

    public static final short NONE = 0;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short UNSUPPORTED_VERSION = 35;
    public static final short UNKNOWN_TOPIC_ID = 100;

    private ErrorCodes() {
    }
}
