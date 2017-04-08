package com.github.ilms49898723.pagerank;

import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class PageRank {
    public static final long N = 5;

    public void start(String[] args) {
        try {
            String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: pagerank <in> <out>");
                System.exit(1);
            }
            Cleaner.start(args[1]);
            Initializer.start();
            // in: args[0]; out: matrixparse
            MatrixParser.start(args[0], "matrixparse");
            // in: matrixparse, R; out: matrixmul
            MatrixMultiplication.start();
            // remove R
            Cleaner.remove("R");
            // in: matrixmul; out: R
            RankUpdater.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
