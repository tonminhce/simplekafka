package com.simplekafka.shared;

/**
 * Standard Kafka error codes used across all API responses.
 */
public final class ErrorCodes {

    public static final short NONE = 0;
    public static final short UNKNOWN_SERVER_ERROR = -1;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short UNSUPPORTED_VERSION = 35;
    public static final short UNKNOWN_TOPIC_ID = 100;

    // Replication errors
    public static final short NOT_LEADER_OR_FOLLOWER = 6;
    public static final short REPLICA_NOT_AVAILABLE = 9;
    public static final short NOT_ENOUGH_REPLICAS = 19;
    public static final short STALE_CONTROLLER_EPOCH = 11;
    public static final short STALE_LEADER_EPOCH_CODE = 83;

    // Transaction errors
    public static final short PRODUCER_FENCED = 40;
    public static final short INVALID_PRODUCER_EPOCH = 47;
    public static final short TRANSACTIONAL_ID_NOT_FOUND = 48;
    public static final short INVALID_TXN_STATE = 49;

    // Security errors
    public static final short TOPIC_AUTHORIZATION_FAILED = 29;
    public static final short CLUSTER_AUTHORIZATION_FAILED = 31;
    public static final short SASL_AUTHENTICATION_FAILED = 58;

    private ErrorCodes() {
    }
}
