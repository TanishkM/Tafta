package org.example.BasicClients;

import java.net.HttpURLConnection;
import java.net.URL;

public class BasicConsumerClient {

    private final String brokerUrl;
    private long offset = 0;

    public BasicConsumerClient(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public void poll(String topic, int partition) throws Exception {
        URL url = new URL(
                brokerUrl +
                        "/topics/" + topic +
                        "/partitions/" + partition +
                        "/poll?offset=" + offset + "&timeoutMs=5000"
        );

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        var response = new String(conn.getInputStream().readAllBytes());
        // parse messages, update offset manually
        System.out.println(response);
    }
}
