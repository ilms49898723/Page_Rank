package com.github.ilms49898723.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MatrixMultiplication {
    private static class MatrixKey implements WritableComparable<MatrixKey> {
        private int mI;
        private int mJ;

        public MatrixKey() {
            mI = -1;
            mJ = -1;
        }

        public MatrixKey(int i, int j) {
            mI = i;
            mJ = j;
        }

        public int getI() {
            return mI;
        }

        public int getJ() {
            return mJ;
        }

        @Override
        public int compareTo(MatrixKey o) {
            if (Integer.valueOf(mI).compareTo(o.mI) != 0) {
                return Integer.valueOf(mI).compareTo(o.mI);
            } else {
                return Integer.valueOf(mJ).compareTo(o.mJ);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(mI);
            out.writeInt(mJ);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mI = in.readInt();
            mJ = in.readInt();
        }
    }
    private static class MatrixValue implements Writable {
        private int mMatrix;
        private int mIndex;
        private double mValue;

        public MatrixValue() {
            mMatrix = -1;
            mIndex = -1;
            mValue = -1;
        }

        public MatrixValue(int matrix, int index, double value) {
            mMatrix = matrix;
            mIndex = index;
            mValue = value;
        }

        public int getMatrix() {
            return mMatrix;
        }

        public int getIndex() {
            return mIndex;
        }

        public double getValue() {
            return mValue;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(mMatrix);
            out.writeInt(mIndex);
            out.writeDouble(mValue);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mMatrix = in.readInt();
            mIndex = in.readInt();
            mValue = in.readDouble();
        }

        @Override
        public String toString() {
            return "MatrixValue{" +
                    "mMatrix=" + mMatrix +
                    ", mIndex=" + mIndex +
                    ", mValue=" + mValue +
                    '}';
        }
    }

    private static class MatrixMapper
            extends MapReduceBase
            implements Mapper<Object, Text, MatrixKey, MatrixValue> {
        @Override
        public void map(Object o, Text text, OutputCollector<MatrixKey, MatrixValue> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = text.toString().split(",");
            if (tokens.length != 4) {
                throw new IOException("Length of tokens should be 2. Got " + tokens.length);
            }
            int matrix = (tokens[0].equalsIgnoreCase("m")) ? 0 : 1;
            int i = Integer.parseInt(tokens[1]);
            int j = Integer.parseInt(tokens[2]);
            double v = Double.parseDouble(tokens[3]);
            if (matrix == 0) {
                MatrixKey key = new MatrixKey(i, 1);
                MatrixValue value = new MatrixValue(matrix, j, v);
                outputCollector.collect(key, value);
            } else {
                for (int k = 1; k <= PageRank.N; ++k) {
                    MatrixKey key = new MatrixKey(k, j);
                    MatrixValue value = new MatrixValue(matrix, i, v);
                    outputCollector.collect(key, value);
                }
            }
        }
    }

    private static class MatrixReducer
            extends MapReduceBase
            implements Reducer<MatrixKey, MatrixValue, ObjectWritable, Text> {
        @Override
        public void reduce(MatrixKey matrixKey, Iterator<MatrixValue> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            double sum = 0.0;
            List<MatrixValue> values1 = new ArrayList<>();
            List<MatrixValue> values2 = new ArrayList<>();
            while (iterator.hasNext()) {
                MatrixValue next = iterator.next();
                MatrixValue value = new MatrixValue(next.getMatrix(), next.getIndex(), next.getValue());
                if (value.getMatrix() == 0) {
                    values1.add(value);
                } else {
                    values2.add(value);
                }
            }
            for (MatrixValue value1 : values1) {
                for (MatrixValue value2 : values2) {
                    if (value1.getIndex() == value2.getIndex()) {
                        sum += value1.getValue() * value2.getValue();
                    }
                }
            }
            sum = sum * 0.8 + 0.2 / PageRank.N;
            String output = "R," + matrixKey.getI() + "," + matrixKey.getJ() + "," + sum;
            outputCollector.collect(null, new Text(output));
        }
    }

    public static void start() {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("Matrix Multiplication");
        jobConf.setJarByClass(PageRank.class);
        jobConf.setMapOutputKeyClass(MatrixKey.class);
        jobConf.setMapOutputValueClass(MatrixValue.class);
        jobConf.setOutputKeyClass(ObjectWritable.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setMapperClass(MatrixMapper.class);
        jobConf.setReducerClass(MatrixReducer.class);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobConf, new Path("matrixparse"), new Path("R"));
        FileOutputFormat.setOutputPath(jobConf, new Path("matrixmul"));
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
