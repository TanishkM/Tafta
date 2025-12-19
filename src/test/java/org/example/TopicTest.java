package org.example;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TopicTest {

    @Test
    public void testTopicPublishAndRead() throws IOException {
        Topic topic = new Topic("orders", 3);

        // produce messages
        topic.publish("user1".getBytes(), "order1".getBytes());
        topic.publish("user1".getBytes(), "order2".getBytes());
        topic.publish(null, "order3".getBytes());
        topic.publish(null, "order4".getBytes());

        assertEquals("Topic should have 3 partitions", 3, topic.getPartitionCount());

        int totalMessages = 0;
        boolean foundOrder1 = false, foundOrder2 = false, foundOrder3 = false, foundOrder4 = false;

        // read all partitions
        for (int i = 0; i < topic.getPartitionCount(); i++) {
            List<Message> messages = topic.read(i, 0);
            totalMessages += messages.size();

            for (Message msg : messages) {
                String value = new String(msg.getValue());
                if (value.equals("order1"))
                    foundOrder1 = true;
                if (value.equals("order2"))
                    foundOrder2 = true;
                if (value.equals("order3"))
                    foundOrder3 = true;
                if (value.equals("order4"))
                    foundOrder4 = true;

                // Verify offset is valid
                assertTrue("Offset should be non-negative", msg.getOffset() >= 0);
            }
        }

        assertEquals("Should have 4 total messages", 4, totalMessages);
        assertTrue("Should have message 'order1'", foundOrder1);
        assertTrue("Should have message 'order2'", foundOrder2);
        assertTrue("Should have message 'order3'", foundOrder3);
        assertTrue("Should have message 'order4'", foundOrder4);
    }
}
