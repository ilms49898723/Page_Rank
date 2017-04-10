package com.github.ilms49898723.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class MatrixMultiplication {
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
    }

    private static class MatrixMapper
            extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, MatrixValue> {
        @Override
        public void map(Object o, Text text, OutputCollector<IntWritable, MatrixValue> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = text.toString().split(",");
            if (tokens[0].equalsIgnoreCase("r")) {
                int index = Integer.parseInt(tokens[1]);
                double value = Double.parseDouble(tokens[3]);
                for (int i = 4; i < tokens.length; ++i) {
                    int to = Integer.parseInt(tokens[i]);
                    IntWritable matrixIndex = new IntWritable(to);
                    MatrixValue matrixValue = new MatrixValue(0, index, value);
                    outputCollector.collect(matrixIndex, matrixValue);
                }
            } else {
                int to = Integer.parseInt(tokens[1]);
                for (int i = 3; i < tokens.length; i += 2) {
                    int index = Integer.parseInt(tokens[i]);
                    double value = Double.parseDouble(tokens[i + 1]);
                    IntWritable matrixIndex = new IntWritable(to);
                    MatrixValue matrixValue = new MatrixValue(1, index, value);
                    outputCollector.collect(matrixIndex, matrixValue);
                }
            }
        }
    }

    private static class MatrixReducer
            extends MapReduceBase
            implements Reducer<IntWritable, MatrixValue, ObjectWritable, Text> {
        @Override
        public void reduce(IntWritable intWritable, Iterator<MatrixValue> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            ArrayList<MatrixValue> values1 = new ArrayList<>();
            HashMap<Integer, Double> values2 = new HashMap<>();
            while (iterator.hasNext()) {
                MatrixValue next = iterator.next();
                if (next.getMatrix() == 0) {
                    values1.add(new MatrixValue(next.getMatrix(), next.getIndex(), next.getValue()));
                } else {
                    values2.put(next.getIndex(), next.getValue());
                }
            }
            double sum = 0.0;
            for (MatrixValue val1 : values1) {
                sum += val1.getValue() * values2.get(val1.getIndex());
            }
            String output = "R," + intWritable.toString() + "," + sum;
            outputCollector.collect(null, new Text(output));
        }
    }

    public static void start(String input, String rInput, String output) {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("Matrix Multiplication");
        jobConf.setJarByClass(PageRank.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
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
