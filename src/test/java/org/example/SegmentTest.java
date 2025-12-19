package org.example;

import org.example.Partition.SegmentedPartition;

import java.io.File;

public class SegmentTest {

    public static void main(String[] args) throws Exception {
        File dataDir = new File("data");
        dataDir.mkdirs();

        SegmentedPartition p = new SegmentedPartition(
                dataDir,
                "metrics",
                1,
                100 // tiny segment to force rolling
        );

        for (int i = 0; i < 10; i++) {
            p.append(null, ("m" + i).getBytes());
        }

        var messages = p.readFromOffset(0);
        assert messages.size() == 10;

        System.out.println("Log segment test passed");
    }
}
