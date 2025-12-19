package org.example.Broker;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class BrokerHttpServer {
    private final SimpleBroker broker;
    private final HttpServer server;

    public BrokerHttpServer (SimpleBroker broker, int port) throws IOException {
        this.broker = broker;
        this.server = HttpServer.create(new InetSocketAddress(port),0);
        server.createContext("/topics", new TopicHandler(broker));
        server.setExecutor(Executors.newCachedThreadPool());
    }
    public void start(){
        server.start();
        broker.start();
        System.out.println("Broker HTTP server started");

    }
    public void stop() {
        broker.shutdown();
        server.stop(0);
    }
}
