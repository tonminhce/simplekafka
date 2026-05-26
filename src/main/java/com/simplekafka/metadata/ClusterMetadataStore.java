package com.simplekafka.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * In-memory cache of cluster metadata loaded from the __cluster_metadata log.
 * <p>
 * Provides lookup by topic name, topic UUID, and partition.
 * Thread-safe via synchronized access for future multi-threaded use.
 */
public class ClusterMetadataStore {

    /** topicName -> TopicRecord */
    private final Map<String, TopicRecord> topicsByName = new LinkedHashMap<>();

    /** topicId -> TopicRecord */
    private final Map<UUID, TopicRecord> topicsById = new LinkedHashMap<>();

    /** topicId -> list of partitions sorted by partitionIndex */
    private final Map<UUID, List<PartitionRecord>> partitionsByTopicId = new LinkedHashMap<>();

    /**
     * Adds a topic record to the store.
     */
    public synchronized void addTopic(TopicRecord topic) {
        topicsByName.put(topic.getTopicName(), topic);
        topicsById.put(topic.getTopicId(), topic);
        partitionsByTopicId.putIfAbsent(topic.getTopicId(), new ArrayList<>());
    }

    /**
     * Adds a partition record to the store.
     */
    public synchronized void addPartition(PartitionRecord partition) {
        List<PartitionRecord> partitions = partitionsByTopicId.computeIfAbsent(
                partition.getTopicId(), k -> new ArrayList<>());

        // Insert in sorted order by partitionId.
        int idx = Collections.binarySearch(
                partitions,
                partition,
                (a, b) -> Integer.compare(a.getPartitionId(), b.getPartitionId()));
        if (idx < 0) {
            idx = -idx - 1;
            partitions.add(idx, partition);
        } else {
            partitions.set(idx, partition); // replace existing
        }
    }

    /**
     * Looks up a topic by name.
     *
     * @return the TopicRecord, or null if not found
     */
    public synchronized TopicRecord getTopic(String topicName) {
        return topicsByName.get(topicName);
    }

    /**
     * Looks up a topic by UUID.
     *
     * @return the TopicRecord, or null if not found
     */
    public synchronized TopicRecord getTopicById(UUID topicId) {
        return topicsById.get(topicId);
    }

    /**
     * Returns all partitions for a given topic UUID, sorted by partition index.
     *
     * @return unmodifiable list of partition records, or empty list if topic not found
     */
    public synchronized List<PartitionRecord> getPartitions(UUID topicId) {
        List<PartitionRecord> partitions = partitionsByTopicId.get(topicId);
        return partitions != null ? Collections.unmodifiableList(partitions) : List.of();
    }

    /**
     * Returns a specific partition by topic ID and partition index.
     *
     * @return the PartitionRecord, or null if not found
     */
    public synchronized PartitionRecord getPartition(UUID topicId, int partitionIndex) {
        List<PartitionRecord> partitions = partitionsByTopicId.get(topicId);
        if (partitions == null) {
            return null;
        }
        for (PartitionRecord p : partitions) {
            if (p.getPartitionId() == partitionIndex) {
                return p;
            }
        }
        return null;
    }

    /**
     * Returns all known topic names.
     */
    public synchronized List<String> getAllTopicNames() {
        return new ArrayList<>(topicsByName.keySet());
    }

    /**
     * Clears all metadata.
     */
    public synchronized void clear() {
        topicsByName.clear();
        topicsById.clear();
        partitionsByTopicId.clear();
    }
}
