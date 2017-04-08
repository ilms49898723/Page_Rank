package com.github.ilms49898723.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
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
        private String mValue;

        public MatrixValue() {
            mMatrix = -1;
            mIndex = -1;
            mValue = "-1";
        }

        public MatrixValue(int matrix, int index, String value) {
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

        public String getValue() {
            return mValue;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(mMatrix);
            out.writeInt(mIndex);
            out.writeUTF(mValue);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mMatrix = in.readInt();
            mIndex = in.readInt();
            mValue = in.readUTF();
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
            int matrix = (tokens[0].equalsIgnoreCase("m")) ? 0 : 1;
            int i = Integer.parseInt(tokens[1]);
            int j = Integer.parseInt(tokens[2]);
            String v = tokens[3];
            if (matrix == 0) {
                MatrixKey key = new MatrixKey(i, 0);
                MatrixValue value = new MatrixValue(matrix, j, v);
                outputCollector.collect(key, value);
            } else {
                for (int k = 0; k < PageRank.N; ++k) {
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
            BigDecimal sum = new BigDecimal(0.0);
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
                        BigDecimal left = new BigDecimal(value1.getValue());
                        BigDecimal right = new BigDecimal(value2.getValue());
                        sum = sum.add(left.multiply(right));
                    }
                }
            }
            sum = sum.multiply(new BigDecimal("0.8")).add(new BigDecimal((1 - PageRank.BETA) / PageRank.N));
            if (sum.compareTo(new BigDecimal("-1.0")) < 0) {
                throw new IOException("sum goes negative");
            }
            String output = "R," + matrixKey.getI() + "," + matrixKey.getJ() + "," + sum;
            outputCollector.collect(null, new Text(output));
        }
    }

    public static void start(String input, String rInput, String output) {
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
        FileInputFormat.setInputPaths(jobConf, new Path(input), new Path(rInput));
        FileOutputFormat.setOutputPath(jobConf, new Path(output));
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
