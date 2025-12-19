package org.example.Consumer;

import org.example.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class LogSegment {
    private final long baseOffset;
    private final File file;
    private final FileChannel channel;
    private long size;

    public LogSegment(File dir,long baseOffset) throws IOException{
        this.baseOffset = baseOffset;
        this.file = new File(dir, String.format("%020d.log", baseOffset));
        this.channel = new RandomAccessFile(file,"rw").getChannel();
        this.size = channel.size();
        channel.position(size);
    }
    public boolean canAppend(int recordSize,long maxSegmentBytes){
        return size + recordSize <= maxSegmentBytes;
    }
    public synchronized void append(ByteBuffer record) throws IOException{
        channel.write(record);
        channel.force(true);
        size += record.remaining();
    }
    public List<Message> readFromOffset(long offset) throws IOException {
        List<Message> result = new ArrayList<>();
        channel.position(0);

        ByteBuffer header = ByteBuffer.allocate(24);

        while (true) {
            header.clear();
            int bytesRead = channel.read(header);

            // EOF or partial header → stop
            if (bytesRead < 24) {
                break;
            }

            header.flip();

            long msgOffset = header.getLong();
            long timestamp = header.getLong();
            int keySize = header.getInt();
            int valueSize = header.getInt();

            byte[] key = null;
            if (keySize > 0) {
                ByteBuffer keyBuf = ByteBuffer.allocate(keySize);
                channel.read(keyBuf);
                key = keyBuf.array();
            }

            ByteBuffer valueBuf = ByteBuffer.allocate(valueSize);
            channel.read(valueBuf);
            byte[] value = valueBuf.array();

            if (msgOffset >= offset) {
                result.add(new Message(msgOffset, key, value,timestamp));
            }
        }
        return result;
    }

    public long getBaseOffset(){
        return baseOffset;
    }
    public long getSize(){
        return size;
    }
}
