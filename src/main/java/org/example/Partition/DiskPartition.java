package org.example.Partition;

import org.example.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DiskPartition {
    private final int partitionId;
    private final File logFile;
    private final List<Message> inMemoryLog;
    private final AtomicLong nextOffset;
    private final FileChannel channel;

    public DiskPartition(String topic,int partitionId, File baseDir) throws IOException {
        this.partitionId = partitionId;
        this.inMemoryLog = new ArrayList<>();
        this.nextOffset = new AtomicLong(0);


        // creating log folder
        File topicDir = new File(baseDir, "logs");
        topicDir.mkdirs();

        // creating log file
        this.logFile = new File(topicDir, topic + "-" + partitionId + ".log");
        this.channel =new RandomAccessFile(logFile, "rw").getChannel();
        loadFromDisk();
    }


    private void loadFromDisk() throws IOException {
        channel.position(0);
        ByteBuffer header = ByteBuffer.allocate(24);
        while (channel.position() < channel.size()){
            header.clear();
            channel.read(header);
            header.flip();
            long offset = header.getLong();
            long timestamp = header.getLong();
            int keySize = header.getInt();
            int valueSize = header.getInt();


            byte[] key = null;
            if(keySize > 0){
                key = new byte[keySize];
                channel.read(ByteBuffer.wrap(key));
            }
            byte[] value = new byte[valueSize];
            channel.read(ByteBuffer.wrap(value));

            Message message = new Message(offset, key, value, timestamp);
            inMemoryLog.add(message);
            nextOffset.set(offset + 1);
        }
    }
    public synchronized Message append(byte[] key, byte[] value) throws IOException {
        long offset = nextOffset.getAndIncrement();
        long timestamp = System.currentTimeMillis();
        Message message = new Message(offset, key, value, timestamp);
        writeToDisk(message);
        inMemoryLog.add(message);
        return message;
    }
    private void writeToDisk(Message message)throws IOException {
        byte[] key = message.getKey();
        byte[] value = message.getValue();
        int keySize = key == null ? 0 : key.length;
        int valueSize = value.length;

        ByteBuffer buffer = ByteBuffer.allocate(24 + keySize + valueSize);
        buffer.putLong(message.getOffset());
        buffer.putLong(message.getTimestamp());
        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        if(keySize > 0){
            buffer.put(key);
        }
        buffer.put(value);
        buffer.flip();
        channel.write(buffer);
        channel.force(true);
    }
    public synchronized List<Message> readFromOffset(long offset) {
        List<Message> result = new ArrayList<>();
        for(Message message : inMemoryLog) {
            if(message.getOffset() >= offset) {
                result.add(message);
            }
        }
        return result;
    }
    public long getNextOffset() {
        return nextOffset.get();
    }
}
