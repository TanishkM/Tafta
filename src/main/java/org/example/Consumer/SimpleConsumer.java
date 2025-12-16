package org.example.Consumer;

import org.example.Message;
import org.example.TempBrokers.ConsumerOffsetStore;
import org.example.TempBrokers.TopicRegistry;
import org.example.Topic;
import org.example.TopicPartition;

import java.util.ArrayList;
import java.util.List;

public class SimpleConsumer implements Consumer{
    private final String consumerId;
    private final String topicName;
    private final TopicRegistry topicRegistry;
    private final ConsumerOffsetStore offsetStore;

    public SimpleConsumer(String consumerId, String topicName, TopicRegistry topicRegistry, ConsumerOffsetStore offsetStore) {
        this.consumerId = consumerId;
        this.topicName = topicName;
        this.topicRegistry = topicRegistry;
        this.offsetStore = offsetStore;
    }

    @Override
    public List<Message> poll(){
        List<Message> result = new ArrayList<>();
        Topic topic = topicRegistry.getTopic(topicName);
        for(int i = 0;i<topic.getPartitionCount();i++){
            TopicPartition tp = new TopicPartition(topic,i);
            var offset = offsetStore.getOffset(consumerId,tp);
            List<Message> messages = topic.read(i,offset);
            if(!messages.isEmpty()){
                Message last = messages.get(messages.size()-1);
                offsetStore.commitOffset(consumerId,tp,last.getOffset()+1);
                result.addAll(messages);
            }
        }
        return result;
    }
}
