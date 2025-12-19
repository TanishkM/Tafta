package org.example.Producer;

import org.example.Message;
import org.example.Topic;
import org.example.TempBrokers.TopicRegistry;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class ProducerTest {

    private TopicRegistry registry;
    private Producer producer;

    @Before
    public void setUp() throws IOException {
        registry = new TopicRegistry();
        registry.createTopic("payments", 2);
        producer = new SimpleProducer(registry);
    }

    @Test
    public void testProducerSendsMessages() throws IOException {
        producer.send("payments", "user1".getBytes(), "p1".getBytes());
        producer.send("payments", "user1".getBytes(), "p2".getBytes());
        producer.send("payments", null, "p3".getBytes());

        Topic topic = registry.getTopic("payments");
        assertNotNull("Topic should exist", topic);

        int totalMessages = 0;
        boolean foundP1 = false, foundP2 = false, foundP3 = false;

        for (int i = 0; i < topic.getPartitionCount(); i++) {
            List<Message> messages = topic.read(i, 0);
            totalMessages += messages.size();

            for (Message msg : messages) {
                String value = new String(msg.getValue());
                if (value.equals("p1"))
                    foundP1 = true;
                if (value.equals("p2"))
                    foundP2 = true;
                if (value.equals("p3"))
                    foundP3 = true;

                // Verify offset is valid
                assertTrue("Offset should be non-negative", msg.getOffset() >= 0);
            }
        }

        assertEquals("Should have 3 total messages", 3, totalMessages);
        assertTrue("Should have message 'p1'", foundP1);
        assertTrue("Should have message 'p2'", foundP2);
        assertTrue("Should have message 'p3'", foundP3);
    }
}
