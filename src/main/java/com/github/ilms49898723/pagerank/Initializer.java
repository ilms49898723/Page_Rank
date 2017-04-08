package com.github.ilms49898723.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Initializer {
    public static void start(String output) {
        Path path = new Path(output, output);
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.create(path, true))
            );
            for (long i = 0; i < PageRank.N; ++i) {
                writer.write("R," + i + ",0," + ((double) 1 / PageRank.N) + "\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
