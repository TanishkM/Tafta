package org.example.Consumer;

import org.example.Producer.Producer;
import org.example.Producer.SimpleProducer;
import org.example.TempBrokers.ConsumerOffsetStore;
import org.example.TempBrokers.TopicRegistry;

public class ConsumerTest {

    public static void main(String[] args) {
        TopicRegistry registry = new TopicRegistry();
        registry.createTopic("logs", 2);

        Producer producer = new SimpleProducer(registry);
        ConsumerOffsetStore offsetStore = new ConsumerOffsetStore();

        producer.send("logs", null, "a".getBytes());
        producer.send("logs", null, "b".getBytes());
        producer.send("logs", null, "c".getBytes());

        Consumer consumer1 = new SimpleConsumer(
                "consumer-1",
                "logs",
                registry,
                offsetStore
        );

        System.out.println("First poll:");
        consumer1.poll().forEach(m ->
                System.out.println(new String(m.getValue()))
        );

        System.out.println("Second poll (should be empty):");
        consumer1.poll().forEach(m ->
                System.out.println(new String(m.getValue()))
        );

        producer.send("logs", null, "d".getBytes());

        System.out.println("Third poll:");
        consumer1.poll().forEach(m ->
                System.out.println(new String(m.getValue()))
        );

        System.out.println("Consumer test passed");
    }
}
