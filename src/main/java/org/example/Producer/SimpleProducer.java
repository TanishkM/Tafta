package org.example.Producer;

import org.example.Topic;
import org.example.TempBrokers.TopicRegistry;

import java.io.IOException;

public class SimpleProducer implements Producer {

    private final TopicRegistry topicRegistry;

    public SimpleProducer(TopicRegistry topicRegistry) {
        this.topicRegistry = topicRegistry;
    }

    @Override
    public void send(String topicName, byte[] key, byte[] value) throws IOException {
        Topic topic = topicRegistry.getTopic(topicName);
        topic.publish(key,value);
    }
}
