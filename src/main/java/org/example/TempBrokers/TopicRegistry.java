package org.example.TempBrokers;

import org.example.Topic;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicRegistry {
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    public void createTopic(String name, int partitionsCount) throws IOException {
        if (topics.containsKey(name)) {
            throw new IllegalArgumentException("Topic already exists: " + name);
        }
        topics.putIfAbsent(name, new Topic(name, partitionsCount));
    }

    public Topic getTopic(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + name);
        }
        return topic;
    }
}
