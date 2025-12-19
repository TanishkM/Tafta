package org.example.Handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.example.Broker.SimpleBroker;

import java.io.IOException;
import java.util.Objects;

public class OffsetHandler implements HttpHandler {

    private final SimpleBroker broker;

    public OffsetHandler(SimpleBroker broker) {
        this.broker = broker;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String[] parts = exchange.getRequestURI().getPath().split("/");
        String method = exchange.getRequestMethod();

        if (parts.length != 5) {
            send(exchange, 400, "Invalid offset path");
            return;
        }

        String consumerId = parts[2];
        String topic = parts[3];
        int partition = Integer.parseInt(parts[4]);

        if ("GET".equals(method)) {
            long offset = broker.fetchOffset(consumerId, topic, partition);
            send(exchange, 200, "{\"offset\":" + offset + "}");
            return;
        }

        if ("POST".equals(method)) {
            String body = new String(exchange.getRequestBody().readAllBytes());
            long offset = Long.parseLong(
                    Objects.requireNonNull(extractJsonField(body, "offset"))
            );

            broker.commitOffset(consumerId, topic, partition, offset);
            send(exchange, 200, "{\"status\":\"OK\"}");
            return;
        }

        send(exchange, 405, "Method not allowed");
    }
    private void send(HttpExchange exchange, int status, String response)
            throws IOException {

        byte[] bytes = response.getBytes();

        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);

        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String extractJsonField(String json, String field) {
        String pattern = "\"" + field + "\":";
        int start = json.indexOf(pattern);

        if (start == -1) return null;

        start += pattern.length();
        int end = json.indexOf("}", start);

        return json.substring(start, end).trim();
    }
}
