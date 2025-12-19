package org.example.Broker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.example.Message;

import java.io.IOException;
import java.util.List;

public class TopicHandler implements HttpHandler {
    private final SimpleBroker broker;

    public TopicHandler(SimpleBroker broker) {
        this.broker = broker;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();


        // /topics/{topic}/produce
        // /topics/{topic}/partitions/{partition}/poll

        String[] parts = path.split("/");
        if(parts.length<4){
            send(exchange, 400, "Invalid Path");
            return;
        }
        String topic = parts[2];
        if("POST".equals(method) && "produce".equals(parts[3])){
            handleProduce(exchange, topic);
            return;
        }
        if("GET".equals(method) && "partitions".equals(parts[3])){
            int partition = Integer.parseInt(parts[4]);
            handlePoll(exchange, topic, partition);
            return;
        }
        send(exchange, 404, "Not Found");
    }
    private void handleProduce(HttpExchange exchange, String topic)
            throws IOException {

        String body = new String(exchange.getRequestBody().readAllBytes());

        String key = extractJsonField(body, "key");
        String value = extractJsonField(body, "value");

        broker.send(
                topic,
                key != null ? key.getBytes() : null,
                value.getBytes()
        );

        send(exchange, 200, "{\"status\":\"OK\"}");
    }

    private void handlePoll(HttpExchange exchange, String topic, int partition) throws IOException {
        var params = exchange.getRequestURI().getQuery();
        long offset = parseOffset(params);
        long timeoutMs = parseTimeout(params);

        long start = System.currentTimeMillis();

        while (true) {
            var messages = broker.poll("client", topic, partition, offset);
            if (!messages.isEmpty()) {
                send(exchange, 200, serialize(messages));
                return;
            }

            if (System.currentTimeMillis() - start > timeoutMs) {
                send(exchange, 200, "[]");
                return;
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {}
        }
    }
    private void send(HttpExchange exchange, int status, String response)
            throws IOException {

        byte[] bytes = response.getBytes();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);

        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
    private long parseOffset(String query) {
        if (query == null) return 0;

        for (String param : query.split("&")) {
            String[] kv = param.split("=");
            if (kv.length == 2 && kv[0].equals("offset")) {
                return Long.parseLong(kv[1]);
            }
        }
        return 0;
    }
    private long parseTimeout(String query) {
        if (query == null) return 5000;

        for (String param : query.split("&")) {
            String[] kv = param.split("=");
            if (kv.length == 2 && kv[0].equals("timeoutMs")) {
                return Long.parseLong(kv[1]);
            }
        }
        return 5000;
    }
    private String extractJsonField(String json, String field) {
        String pattern = "\"" + field + "\":\"";
        int start = json.indexOf(pattern);

        if (start == -1) return null;

        start += pattern.length();
        int end = json.indexOf("\"", start);

        return end == -1 ? null : json.substring(start, end);
    }

    private String serialize(List<Message> messages) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"messages\":[");

        for (int i = 0; i < messages.size(); i++) {
            Message m = messages.get(i);
            sb.append("{")
                    .append("\"offset\":").append(m.getOffset()).append(",")
                    .append("\"value\":\"")
                    .append(new String(m.getValue()))
                    .append("\"")
                    .append("}");

            if (i < messages.size() - 1) {
                sb.append(",");
            }
        }

        sb.append("]}");
        return sb.toString();
    }

}
