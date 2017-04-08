package com.github.ilms49898723.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Initializer {
    private static final long N = 5;
    public static void start() {
        Path path = new Path("R", "R");
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.create(path, true))
            );
            for (long i = 1; i <= N; ++i) {
                writer.write("R," + i + ",1," + ((float) 1 / N) + "\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
