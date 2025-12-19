package org.example.OffsetPersistence;

public interface OffsetStore {

    long readOffset(String consumerId, String topic, int partition);

    void commitOffset(
            String consumerId,
            String topic,
            int partition,
            long offset
    );
}

