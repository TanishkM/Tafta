package org.example;

import org.example.Broker.BrokerHttpServer;
import org.example.Broker.SimpleBroker;
import org.example.Retention.RetentionPolicy;
import org.example.Retention.TimeBasedRetentionPolicy;

import java.io.File;
import java.time.Duration;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws Exception {

        RetentionPolicy retentionPolicy =
                new TimeBasedRetentionPolicy(Duration.ofDays(7));

        SimpleBroker broker = new SimpleBroker(
                new File("data"),
                1024 * 1024,   // 1MB segments
                retentionPolicy
        );

        broker.createTopic("test-topic", 1);

        BrokerHttpServer server =
                new BrokerHttpServer(broker, 8080);

        server.start();

        System.out.println("Broker started on http://localhost:8080");
        System.out.println("Press Ctrl+C to stop");
    }
}
