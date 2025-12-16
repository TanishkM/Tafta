package org.example;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class PartitionTest {

    @Test
    public void testPartitionAppendAndRead() {
        Partition partition = new Partition(0);

        partition.append(null, "msg1".getBytes());
        partition.append(null, "msg2".getBytes());
        partition.append(null, "msg3".getBytes());

        assertEquals("Next offset should be 3", 3, partition.getNextOffset());

        List<Message> messages = partition.readFromOffset(1);

        assertEquals("Should have 2 messages from offset 1", 2, messages.size());
        assertEquals("First message should be 'msg2'", "msg2", new String(messages.get(0).getValue()));
        assertEquals("First message offset should be 1", 1, messages.get(0).getOffset());
    }

}
