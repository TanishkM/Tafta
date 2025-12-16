package org.example;


import java.util.List;

public class PartitionTest {
    public static void main(String[] args) {
        Partition partition = new Partition(0);

        partition.append(null, "msg1".getBytes());
        partition.append(null, "msg2".getBytes());
        partition.append(null, "msg3".getBytes());

        assert partition.getNextOffset() == 3;

        List<Message> messages = partition.readFromOffset(1);

        assert messages.size() == 2;
        assert new String(messages.get(0).getValue()).equals("msg2");
        assert messages.get(0).getOffset() == 1;

        System.out.println("Partition test passed");
    }

}
