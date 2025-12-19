package org.example.Retention;

import org.example.LogSegment;

public class SizeBasedRetentionPolicy implements RetentionPolicy {

    private final long maxBytes;

    public SizeBasedRetentionPolicy(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    @Override
    public boolean shouldDelete(LogSegment segment, RetentionContext context) {
        return context.getTotalLogSizeBytes() > maxBytes;
    }
}
