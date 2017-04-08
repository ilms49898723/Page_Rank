package com.github.ilms49898723.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;

public class InputPreProcessor {
    public static void start(String input, String output, String mapOut) {
        HashMap<Integer, Integer> mapping = new HashMap<>();
        Path in = new Path(input);
        Path out = new Path(output, output);
        Path map = new Path(mapOut, mapOut);
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(in))
            );
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] tokens = line.split("\\s+");
                int a = Integer.parseInt(tokens[0]);
                int b = Integer.parseInt(tokens[1]);
                if (a >= 0 && a < PageRank.N) {
                    mapping.put(a, a);
                }
                if (b >= 0 && b < PageRank.N) {
                    mapping.put(b, b);
                }
            }
            reader.close();
            reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(in))
            );
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.create(out, true))
            );
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] tokens = line.split("\\s+");
                int a = Integer.parseInt(tokens[0]);
                int b = Integer.parseInt(tokens[1]);
                int newA = remap(mapping, a);
                int newB = remap(mapping, b);
                if (newA == -1 || newB == -1) {
                    throw new IOException("Remap error! PageRank.N is too small!");
                }
                writer.write(newA + " " + newB + "\n");
            }
            reader.close();
            writer.close();
            writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.create(map, true))
            );
            for (int key : mapping.keySet()) {
                if (!(key >= 0 && key < PageRank.N)) {
                    writer.write(mapping.get(key) + " " + key + "\n");
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int remap(HashMap<Integer, Integer> mapping, int src) {
        if (src >= 0 && src < PageRank.N) {
            return src;
        } else if (mapping.containsKey(src)) {
            return mapping.get(src);
        } else {
            for (int i = 0; i < PageRank.N; ++i) {
                if (!mapping.containsKey(i)) {
                    mapping.put(src, i);
                    mapping.put(i, -1);
                    return i;
                }
            }
        }
        return -1;
    }
}
