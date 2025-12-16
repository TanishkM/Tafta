package org.example;

import java.util.Objects;

public class TopicPartition {
    private final Topic topic;
    private final int partition;

    public TopicPartition(Topic topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public Topic getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "TopicPartition{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                '}';
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!(o instanceof TopicPartition)) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic,partition);
    }
}
