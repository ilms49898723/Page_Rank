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
            Initializer.start("R");
            InputPreProcessor.start(args[0], "prematrix", "mapping");
            MatrixParser.start("prematrix", "matrixparse");
            for (int i = 0; i < ROUND; ++i) {
                MatrixMultiplication.start("matrixparse", "R", "matrixmul");
                Cleaner.remove("R");
                RankUpdater.start("matrixmul", "R");
                Cleaner.remove("matrixmul");
            }
            OutputPostProcessor.start("R", args[1], "mapping");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
