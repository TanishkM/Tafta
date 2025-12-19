package org.example.Partition;

import org.example.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {
    int partitionId;
    List<Message> log;
    AtomicLong nextOffset;

    public Partition(int partitionId) {
        this.partitionId = partitionId;
        this.log = new ArrayList<>();
        this.nextOffset = new AtomicLong(0);
    }
    public synchronized Message append(byte[] key, byte[] value) {
        long offset = nextOffset.getAndIncrement();
        Message message = new Message(offset, key, value, System.currentTimeMillis());
        log.add(message);
        return message;
    }
    public synchronized List<Message> readFromOffset(long offset) {
        List<Message> result = new ArrayList<>();
        for(Message message : log) {
            if(message.getOffset() >= offset) {
                result.add(message);
            }
        }
        return result;
    }

    public int getPartitionId() {
        return partitionId;
    }
    public long getNextOffset() {
        return nextOffset.get();
    }
}
