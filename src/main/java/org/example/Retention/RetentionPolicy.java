package org.example.Retention;

import org.example.LogSegment;

public interface RetentionPolicy {

    /**
     * @param segment metadata about the segment
     * @param context  partition-level context (optional, extensible)
     * @return true if the segment should be deleted
     */
    boolean shouldDelete(LogSegment segment, RetentionContext context);
}
