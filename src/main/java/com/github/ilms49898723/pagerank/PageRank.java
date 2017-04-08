package com.github.ilms49898723.pagerank;

import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class PageRank {

    public void start(String[] args) {
        try {
            String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: pagerank <in> <out>");
                System.exit(1);
            }
            Cleaner.start(args[1]);
            Initializer.start();
            MatrixParser.start(args[0], "matrixparse");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
