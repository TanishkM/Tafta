package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {
    private final String name;
    private final List<Partition> partitions;
    private final AtomicInteger roundRobinCounter;

    public Topic(String name, int partitionsCount) {
        this.name = name;
        this.partitions = new ArrayList<>();
        this.roundRobinCounter = new AtomicInteger(0);
        for(int i = 0; i < partitionsCount; i++) {
            partitions.add(new Partition(i));
        }
    }
    public Message publish(byte[] key, byte[] value) {
        Partition partition = selectPartition(key);
        return partition.append(key, value);
    }
    public List<Message> read(int partitionId, long offset) {
        if(partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition id: " + partitionId);
        }
        Partition partition = partitions.get(partitionId);
        return partition.readFromOffset(offset);
    }
    private Partition selectPartition(byte[] key) {
        if(key == null) {
            int partitionIndex = roundRobinCounter.getAndIncrement() % partitions.size();
            return partitions.get(partitionIndex);
        } else {
            int hash = Math.abs(java.util.Arrays.hashCode(key));
            int partitionIndex = hash % partitions.size();
            return partitions.get(partitionIndex);
        }
    }
    public String getName() {
        return name;
    }
    public int getPartitionCount() {
        return partitions.size();
    }
}
