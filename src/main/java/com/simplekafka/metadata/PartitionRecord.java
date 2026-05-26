package com.simplekafka.metadata;

import java.util.List;
import java.util.UUID;

/**
 * Represents a PARTITION_RECORD from the __cluster_metadata log.
 * Immutable value object holding partition assignment information.
 */
public class PartitionRecord {

    private final int partitionId;
    private final UUID topicId;
    private final int leaderId;
    private final int leaderEpoch;
    private final List<Integer> replicas;
    private final List<Integer> isr;

    public PartitionRecord(int partitionId, UUID topicId, int leaderId, int leaderEpoch,
                           List<Integer> replicas, List<Integer> isr) {
        this.partitionId = partitionId;
        this.topicId = topicId;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.replicas = List.copyOf(replicas);
        this.isr = List.copyOf(isr);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public UUID getTopicId() {
        return topicId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public List<Integer> getIsr() {
        return isr;
    }
}
