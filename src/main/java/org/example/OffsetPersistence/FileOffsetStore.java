package org.example.OffsetPersistence;
import java.io.*;
import java.nio.file.Files;

public class FileOffsetStore implements OffsetStore {

    private final File baseDir;

    public FileOffsetStore(File baseDir) {
        this.baseDir = new File(baseDir, "offsets");
        this.baseDir.mkdirs();
    }

    @Override
    public synchronized long readOffset(
            String consumerId,
            String topic,
            int partition
    ) {
        File file = offsetFile(consumerId, topic, partition);

        if (!file.exists()) {
            return 0L;
        }

        try {
            String content = Files.readString(file.toPath()).trim();
            return Long.parseLong(content);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read offset", e);
        }
    }

    @Override
    public synchronized void commitOffset(
            String consumerId,
            String topic,
            int partition,
            long offset
    ) {
        try {
            File file = offsetFile(consumerId, topic, partition);
            file.getParentFile().mkdirs();

            try (FileWriter writer = new FileWriter(file, false)) {
                writer.write(Long.toString(offset));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write offset", e);
        }
    }

    private File offsetFile(String consumerId, String topic, int partition) {
        return new File(
                new File(baseDir, consumerId),
                topic + "-" + partition + ".offset"
        );
    }
}
