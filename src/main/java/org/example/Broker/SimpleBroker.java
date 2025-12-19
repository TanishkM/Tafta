package org.example.Broker;

import org.example.Message;
import org.example.OffsetPersistence.FileOffsetStore;
import org.example.OffsetPersistence.OffsetStore;
import org.example.Retention.RetentionPolicy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimpleBroker implements Broker {
    private final Map<String,BrokerTopic> topics = new ConcurrentHashMap<>();
    private final File dataDir;
    private final long maxSegmentBytes;
    private final RetentionPolicy retentionPolicy;
    private final OffsetStore offsetStore;


    public SimpleBroker(File dataDir, long maxSegmentBytes, RetentionPolicy retentionPolicy) {
        this.dataDir = dataDir;
        this.maxSegmentBytes = maxSegmentBytes;
        this.retentionPolicy = retentionPolicy;
        dataDir.mkdirs();
        this.offsetStore = new FileOffsetStore(dataDir);
    }


    @Override
    public void createTopic(String name,int partition){
        try {
            if(topics.containsKey(name)){
                throw new IllegalArgumentException("Topic already exists");
            }
            topics.put(name,new BrokerTopic(name,partition,dataDir,maxSegmentBytes));
        }
        catch (IOException e){
            throw new RuntimeException("Failed to create topic",e);
        }
    }
    @Override
    public void send(String topic, byte[] key, byte[] value) {
        BrokerTopic t = topics.get(topic);
        if (t == null) {
            throw new IllegalArgumentException("Topic not found: " + topic);
        }
        try {
            t.append(key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public List<Message> poll(String consumerId, String topic, int partition, long offset) {
        BrokerTopic brokerTopic = topics.get(topic);
        if(brokerTopic == null){
            throw new IllegalArgumentException("Topic does not exist");
        }
        try {
            return brokerTopic.read(partition, offset);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read messages", e);
        }
    }

    @Override
    public void runRetention() {
        topics.values().forEach(topic -> topic.cleanup(retentionPolicy));
    }


    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public void start() {
        scheduler.scheduleAtFixedRate(
                this::runRetention,
                1,
                1,
                TimeUnit.MINUTES
        );
    }

    public void shutdown() {
        scheduler.shutdown();
    }
    public long fetchOffset(
            String consumerId,
            String topic,
            int partition
    ) {
        return offsetStore.readOffset(consumerId, topic, partition);
    }

    public void commitOffset(
            String consumerId,
            String topic,
            int partition,
            long offset
    ) {
        offsetStore.commitOffset(consumerId, topic, partition, offset);
    }

}
