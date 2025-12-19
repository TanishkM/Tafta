package org.example.Partition;
import org.example.Consumer.LogSegment;
import org.example.Message;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentedPartition {

    private final File dir;
    private final List<LogSegment> segments;
    private final AtomicLong nextOffset;
    private final long maxSegmentBytes;

    public SegmentedPartition(
            File baseDir,
            String topic,
            int partitionId,
            long maxSegmentBytes
    ) throws IOException {

        this.dir = new File(baseDir, topic + "-" + partitionId);
        this.dir.mkdirs();
        this.segments = new ArrayList<>();
        this.nextOffset = new AtomicLong(0);
        this.maxSegmentBytes = maxSegmentBytes;

        loadSegments();
    }

    private void loadSegments() throws IOException {
        File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));

        if (files != null) {
            Arrays.sort(files);
            for (File file : files) {
                long baseOffset = Long.parseLong(file.getName().replace(".log", ""));
                segments.add(new LogSegment(dir, baseOffset));
                nextOffset.set(baseOffset);
            }
        }

        if (segments.isEmpty()) {
            segments.add(new LogSegment(dir, 0));
        }
    }
    public synchronized Message append(byte[] key, byte[] value) throws IOException {
        long offset = nextOffset.getAndIncrement();
        long timestamp = System.currentTimeMillis();

        Message msg = new Message(offset, key, value,timestamp);

        ByteBuffer record = serialize(msg);

        LogSegment active = segments.get(segments.size() - 1);

        if (!active.canAppend(record.limit(), maxSegmentBytes)) {
            active = new LogSegment(dir, offset);
            segments.add(active);
        }

        active.append(record);
        return msg;
    }
    public synchronized List<Message> readFromOffset(long offset) throws IOException {
        List<Message> result = new ArrayList<>();
        for (LogSegment segment : segments) {
            if (segment.getBaseOffset() >= offset) {
                result.addAll(segment.readFromOffset(offset));
            }
        }
        return result;
    }
    private ByteBuffer serialize(Message msg) {
        byte[] key = msg.getKey();
        byte[] value = msg.getValue();

        int keySize = key == null ? 0 : key.length;
        int valueSize = value.length;

        ByteBuffer buffer = ByteBuffer.allocate(
                8 + 8 + 4 + 4 + keySize + valueSize
        );

        buffer.putLong(msg.getOffset());
        buffer.putLong(msg.getTimestamp());
        buffer.putInt(keySize);
        buffer.putInt(valueSize);

        if (keySize > 0) buffer.put(key);
        buffer.put(value);

        buffer.flip();
        return buffer;
    }
}
