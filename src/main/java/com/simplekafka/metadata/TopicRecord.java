package com.simplekafka.metadata;

import java.util.UUID;

/**
 * Represents a TOPIC_RECORD from the __cluster_metadata log.
 * Immutable value object holding topic name and its UUID.
 */
public class TopicRecord {

    private final String topicName;
    private final UUID topicId;

    public TopicRecord(String topicName, UUID topicId) {
        this.topicName = topicName;
        this.topicId = topicId;
    }

    public String getTopicName() {
        return topicName;
    }

    public UUID getTopicId() {
        return topicId;
    }
}
