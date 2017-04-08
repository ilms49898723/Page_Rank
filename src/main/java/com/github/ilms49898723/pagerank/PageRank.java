package com.github.ilms49898723.pagerank;

import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class PageRank {
    public static final long N = 5;
    public static final int ROUND = 1;

    public static void start(String[] args) {
        try {
            String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: pagerank <in> <out>");
                System.exit(1);
            }
            Cleaner.start(args[1]);
            Initializer.start();
            InputPreProcessor.start(args[0], "prematrix", "mapping");
            // in: args[0]; out: matrixparse
            MatrixParser.start("prematrix", "matrixparse");
            for (int i = 0; i < ROUND; ++i) {
                // in: matrixparse, R; out: matrixmul
                MatrixMultiplication.start();
                // remove R
                Cleaner.remove("R");
                // in: matrixmul; out: R
                RankUpdater.start();
                Cleaner.remove("matrixmul");
            }
            OutputPostProcessor.start("R", "final", "mapping");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
