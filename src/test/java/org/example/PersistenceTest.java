package org.example;

import org.example.Partition.DiskPartition;

import java.io.File;

public class PersistenceTest {

    public static void main(String[] args) throws Exception {
        File dataDir = new File("data");
        dataDir.mkdirs();

        DiskPartition p = new DiskPartition("events", 0, dataDir);

        p.append(null, "e1".getBytes());
        p.append(null, "e2".getBytes());

        // Simulate restart
        DiskPartition restarted = new DiskPartition("events", 0, dataDir);

        var messages = restarted.readFromOffset(0);

        assert messages.size() == 2;
        assert new String(messages.get(1).getValue()).equals("e2");

        System.out.println("Persistence test passed");
    }
}
