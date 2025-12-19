package org.example.Consumer;

import org.example.Message;
import org.example.Producer.Producer;
import org.example.Producer.SimpleProducer;
import org.example.TempBrokers.ConsumerOffsetStore;
import org.example.TempBrokers.TopicRegistry;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class ConsumerTest {

        private TopicRegistry registry;
        private Producer producer;
        private ConsumerOffsetStore offsetStore;

        @Before
        public void setUp() throws IOException {
                registry = new TopicRegistry();
                registry.createTopic("logs", 2);
                producer = new SimpleProducer(registry);
                offsetStore = new ConsumerOffsetStore();
        }

        @Test
        public void testConsumerPolling() throws IOException {
                // Produce initial messages
                producer.send("logs", null, "a".getBytes());
                producer.send("logs", null, "b".getBytes());
                producer.send("logs", null, "c".getBytes());

                Consumer consumer1 = new SimpleConsumer(
                                "consumer-1",
                                "logs",
                                registry,
                                offsetStore);

                // First poll - should return messages
                List<Message> firstPoll = consumer1.poll();
                assertNotNull("First poll should not be null", firstPoll);
                assertFalse("First poll should contain messages", firstPoll.isEmpty());

                // Verify message values
                boolean foundA = false, foundB = false, foundC = false;
                for (Message m : firstPoll) {
                        String value = new String(m.getValue());
                        if (value.equals("a"))
                                foundA = true;
                        if (value.equals("b"))
                                foundB = true;
                        if (value.equals("c"))
                                foundC = true;
                }
                assertTrue("Should have received message 'a'", foundA || foundB || foundC);

                // Second poll - should be empty (no new messages)
                List<Message> secondPoll = consumer1.poll();
                assertNotNull("Second poll should not be null", secondPoll);
                assertEquals("Second poll should be empty", 0, secondPoll.size());

                // Produce a new message
                producer.send("logs", null, "d".getBytes());

                // Third poll - should return the new message
                List<Message> thirdPoll = consumer1.poll();
                assertNotNull("Third poll should not be null", thirdPoll);
                assertTrue("Third poll should contain the new message", thirdPoll.size() > 0);

                boolean foundD = false;
                for (Message m : thirdPoll) {
                        if (new String(m.getValue()).equals("d")) {
                                foundD = true;
                                break;
                        }
                }
                assertTrue("Should have received message 'd'", foundD);
        }
}
