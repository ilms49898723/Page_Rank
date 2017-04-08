package com.github.ilms49898723.pagerank;

import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class PageRank {
    public static int N = 5;
    public static int ROUND = 10;
    public static double BETA = 0.8;

    public static void start(String[] args) {
        try {
            String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("Usage: pagerank <in> <out> [round | round #nodes]");
                System.exit(1);
            }
            if (otherArgs.length == 3) {
                PageRank.ROUND = Integer.parseInt(otherArgs[2]);
            }
            if (otherArgs.length == 4) {
                PageRank.ROUND = Integer.parseInt(otherArgs[2]);
                PageRank.N = Integer.parseInt(otherArgs[3]);
            }
            Cleaner.start(args[1]);
            Initializer.start("R");
            InputPreProcessor.start(args[0], "prematrix", "mapping");
            MatrixParser.start("prematrix", "matrixparse");
            for (int i = 0; i < ROUND; ++i) {
                MatrixMultiplication.start("matrixparse", "R", "matrixmul");
                Cleaner.remove("R");
                RankUpdater.start("matrixmul", "R");
//                Cleaner.remove("matrixmul");
            }
            OutputPostProcessor.start("R", args[1], "mapping");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
