package org.example.Retention;

import org.example.LogSegment;

import java.time.Duration;

public class TimeBasedRetentionPolicy implements RetentionPolicy {

    private final long retentionMs;

    public TimeBasedRetentionPolicy(Duration retention) {
        this.retentionMs = retention.toMillis();
    }

    @Override
    public boolean shouldDelete(LogSegment segment, RetentionContext context) {
        long age = context.getNow() - segment.lastModifiedTime();
        return age > retentionMs;
    }
}
