package com.github.ilms49898723.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;

public class OutputPostProcessor {
    public static void start(String input, String output, String mapIn) {
        Path in = new Path(input);
        Path out = new Path(output, output);
        Path map = new Path(mapIn);
        try {
            HashMap<Integer, Integer> mapping = new HashMap<>();
            FileSystem fileSystem = FileSystem.get(new Configuration());
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(map))
            );
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\s+");
                mapping.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
            }
            reader.close();
            reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(in))
            );
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.create(out, true))
            );
            while ((line = reader.readLine()) != null) {
                // R,0,0,1.0
                String[] tokens = line.split(",");
                int index = Integer.parseInt(tokens[1]);
                int newIndex = remap(mapping, index);
                writer.write(newIndex + " " + tokens[3] + "\n");
            }
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int remap(HashMap<Integer, Integer> mapping, int src) {
        if (mapping.containsKey(src)) {
            return mapping.get(src);
        } else {
            return src;
        }
    }
}
