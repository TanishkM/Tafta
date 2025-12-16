package org.example.Producer;

import org.example.Message;
import org.example.Topic;
import org.example.TopicRegistry;

import java.util.List;

public class ProducerTest {

    public static void main(String[] args) {
        TopicRegistry registry = new TopicRegistry();
        registry.createTopic("payments", 2);

        Producer producer = new SimpleProducer(registry);

        producer.send("payments", "user1".getBytes(), "p1".getBytes());
        producer.send("payments", "user1".getBytes(), "p2".getBytes());
        producer.send("payments", null, "p3".getBytes());

        Topic topic = registry.getTopic("payments");

        for (int i = 0; i < topic.getPartitionCount(); i++) {
            List<Message> messages = topic.read(i, 0);
            for (Message msg : messages) {
                System.out.printf(
                        "Partition %d Offset %d Value %s%n",
                        i,
                        msg.getOffset(),
                        new String(msg.getValue())
                );
            }
        }

        System.out.println("Producer test passed");
    }
}
