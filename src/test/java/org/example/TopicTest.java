package org.example;

import java.util.List;

public class TopicTest {
    public static void main(String[] args) {
        Topic topic = new Topic("orders", 3);

        // produce messages
        topic.publish("user1".getBytes(), "order1".getBytes());
        topic.publish("user1".getBytes(), "order2".getBytes());
        topic.publish(null, "order3".getBytes());
        topic.publish(null, "order4".getBytes());

        // read all partitions
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

        System.out.println("Topic test passed");
    }
}

