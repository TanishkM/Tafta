package org.example.Producer;

import org.example.Topic;
import org.example.TopicRegistry;

public class SimpleProducer implements Producer {

    private final TopicRegistry topicRegistry;

    public SimpleProducer(TopicRegistry topicRegistry) {
        this.topicRegistry = topicRegistry;
    }

    @Override
    public void send(String topicName, byte[] key, byte[] value) {
        Topic topic = topicRegistry.getTopic(topicName);
        topic.publish(key,value);
    }
}
