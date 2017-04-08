package com.github.ilms49898723.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class MatrixMultiplication {
    public static void start(String input, String rInput, String output) {
        try {
            double[] oldR = new double[PageRank.N];
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path rIn = new Path(rInput, "R");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(rIn))
            );
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                int index = Integer.parseInt(tokens[1]);
                double value = Double.parseDouble(tokens[3]);
                oldR[index] = value;
            }
            reader.close();
            Path out = new Path(output, output);
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileSystem.create(out))
            );
            int fileIndex = 0;
            while (true) {
                Path in = new Path(input, generateFullName(fileIndex));
                if (!fileSystem.exists(in)) {
                    break;
                }
                reader = new BufferedReader(
                        new InputStreamReader(fileSystem.open(in))
                );
                while ((line = reader.readLine()) != null) {
                    double sum = 0.0;
                    String[] tokens = line.split(",");
                    int column = Integer.parseInt(tokens[1]);
                    int size = Integer.parseInt(tokens[2]);
                    int searchIndex = 3;
                    for (int i = 0; i < size; ++i, searchIndex += 2) {
                        int index = Integer.parseInt(tokens[i]);
                        double value = Double.parseDouble(tokens[i + 1]);
                        sum += oldR[index] * value;
                    }
                    sum = sum * PageRank.BETA + (1.0 - PageRank.BETA) / PageRank.N;
                    writer.write("R," + column + ",0," + sum + "\n");
                }
                ++fileIndex;
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateFullName(int index) {
        String indexPart = "";
        for (int i = 0; i < 5 - Integer.valueOf(index).toString().length(); ++i) {
            indexPart += "0";
        }
        indexPart += index;
        return "part-" + indexPart;
    }
}
