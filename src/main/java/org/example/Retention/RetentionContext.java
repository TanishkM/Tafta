package org.example.Retention;
public class RetentionContext {

    private final long totalLogSizeBytes;
    private final long now;

    public RetentionContext(long totalLogSizeBytes) {
        this.totalLogSizeBytes = totalLogSizeBytes;
        this.now = System.currentTimeMillis();
    }

    public long getTotalLogSizeBytes() {
        return totalLogSizeBytes;
    }

    public long getNow() {
        return now;
    }
}
