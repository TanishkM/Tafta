package org.example.Producer;

import java.io.IOException;

public interface Producer {
    void send(String topic, byte[] key, byte[] value) throws IOException;
}
