package org.example.Broker;

import org.example.Message;
import org.example.Partition.SegmentedPartition;
import org.example.Retention.RetentionPolicy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BrokerTopic {
    private final String name;
    private final List<SegmentedPartition> partitions;

    public BrokerTopic(String name, int partitionCount, File baseDir, long maxSegmentBytes) throws IOException {
        this.name = name;
        this.partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new SegmentedPartition(baseDir, name, i, maxSegmentBytes));
        }
    }
    public Message append(byte[] key, byte[] value) throws IOException {
        SegmentedPartition partition = selectPartition(key);
        return partition.append(key, value);
    }
    public List<Message> read(int partitionId, long offset) throws IOException {
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition ID");
        }
        return partitions.get(partitionId).readFromOffset(offset);
    }
    public void cleanup(RetentionPolicy policy){
        partitions.forEach(partition -> partition.cleanup(policy));
    }
    private SegmentedPartition selectPartition(byte[] key) {
        if (key == null) {
            int idx = (int) (System.nanoTime() % partitions.size());
            return partitions.get(Math.abs(idx));
        }
        int hash = Math.abs(Arrays.hashCode(key));
        return partitions.get(hash % partitions.size());
    }
}
