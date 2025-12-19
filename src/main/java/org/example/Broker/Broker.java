package org.example.Broker;

import org.example.Message;

import java.util.List;

public interface Broker {

    void createTopic(String name, int partitions);

    void send(String topic, byte[] key, byte[] value);

    List<Message> poll(
            String consumerId,
            String topic,
            int partition,
            long offset
    );

    void runRetention();
}
