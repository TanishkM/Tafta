package org.example;
import org.example.Broker.SimpleBroker;
import org.example.Retention.RetentionPolicy;
import org.example.Retention.TimeBasedRetentionPolicy;

import java.io.File;
import java.time.Duration;

public class BrokerTest {

    public static void main(String[] args) {
        RetentionPolicy retention = new TimeBasedRetentionPolicy(
                Duration.ofDays(7)
        );

        SimpleBroker broker = new SimpleBroker(
                new File("data"),
                1024,
                retention
        );

        broker.createTopic("events", 2);

        broker.send("events", null, "e1".getBytes());
        broker.send("events", null, "e2".getBytes());

        var messages = broker.poll(
                "consumer-1",
                "events",
                0,
                0
        );

        messages.forEach(m ->
                System.out.println(new String(m.getValue()))
        );

        broker.runRetention();

        System.out.println("Broker test passed");
    }
}
