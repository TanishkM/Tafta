package org.example.Producer;

public interface Producer {
    void send(String topic, byte[] key, byte[] value);
}
