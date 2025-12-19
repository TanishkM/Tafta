package org.example.BasicClients;

import java.net.HttpURLConnection;
import java.net.URL;

public class BasicProducerClient {

    private final String brokerUrl;

    public BasicProducerClient(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public void send(String topic, String key, String value) throws Exception {
        URL url = new URL(brokerUrl + "/topics/" + topic + "/produce");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setDoOutput(true);

        String body = String.format(
                "{\"key\":\"%s\",\"value\":\"%s\"}",
                key,
                value
        );

        conn.getOutputStream().write(body.getBytes());
        conn.getInputStream().close();
    }
}
