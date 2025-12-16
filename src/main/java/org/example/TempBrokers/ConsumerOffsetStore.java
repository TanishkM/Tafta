package org.example.TempBrokers;

import org.example.TopicPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerOffsetStore {
    private final Map<String,Map<TopicPartition,Long>> offsets = new ConcurrentHashMap<>();

    public long getOffset(String consumerGroup, TopicPartition topicPartition) {
        return offsets
                .getOrDefault(consumerGroup, Map.of())
                .getOrDefault(topicPartition, 0L);
    }
    public void commitOffset(String consumerId, TopicPartition topicPartition, long offset) {
        offsets
                .computeIfAbsent(consumerId, k -> new ConcurrentHashMap<>())
                .put(topicPartition, offset);
    }
}
