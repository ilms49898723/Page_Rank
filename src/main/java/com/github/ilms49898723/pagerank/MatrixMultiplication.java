package com.github.ilms49898723.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;

import java.io.*;
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
            int matrix = (tokens[0].equalsIgnoreCase("m")) ? 0 : 1;
            int i = Integer.parseInt(tokens[1]);
            int j = Integer.parseInt(tokens[2]);
            double v = Double.parseDouble(tokens[3]);
            MatrixKey key = new MatrixKey(i, 0);
            MatrixValue value = new MatrixValue(matrix, j, v);
            outputCollector.collect(key, value);
        }
    }

    private static class MatrixReducer
            extends MapReduceBase
            implements Reducer<MatrixKey, MatrixValue, ObjectWritable, Text> {
        private ArrayList<MatrixValue> mRankValues;

        public MatrixReducer() {
            super();
//            mRankValues = readRFromDisk();
        }

        @Override
        public void reduce(MatrixKey matrixKey, Iterator<MatrixValue> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            double sum = 0.0;
            List<MatrixValue> values1 = new ArrayList<>();
            List<MatrixValue> values2 = mRankValues != null ? mRankValues : readRFromDisk();
            while (iterator.hasNext()) {
                MatrixValue next = iterator.next();
                MatrixValue value = new MatrixValue(next.getMatrix(), next.getIndex(), next.getValue());
                values1.add(value);
            }
            values1.sort((o1, o2) -> Integer.compare(o1.getIndex(), o2.getIndex()));
            values2.sort((o1, o2) -> Integer.compare(o1.getIndex(), o2.getIndex()));
            int val2Index = 0;
            for (MatrixValue value1 : values1) {
                while (values2.get(val2Index).getIndex() < value1.getIndex()) {
                    ++val2Index;
                }
                if (value1.getIndex() == values2.get(val2Index).getIndex()) {
                    sum += value1.getValue() * values2.get(val2Index).getValue();
                }
            }
            sum = sum * PageRank.BETA + (1.0 - PageRank.BETA) / PageRank.N;
            String output = "R," + matrixKey.getI() + "," + matrixKey.getJ() + "," + sum;
            outputCollector.collect(null, new Text(output));
        }

        private ArrayList<MatrixValue> readRFromDisk() throws IOException {
            ArrayList<MatrixValue> result = new ArrayList<>();
            FileSystem fileSystem = FileSystem.get(new Configuration());
            int fileIndex = 0;
            while (fileIndex < 1) {
                Path in = new Path("R", "part-00000");
//                    if (!fileSystem.exists(in)) {
//                        break;
//                    }
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fileSystem.open(in))
                );
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split(",");
                    int index = Integer.parseInt(tokens[1]);
                    double value = Double.parseDouble(tokens[3]);
                    result.add(new MatrixValue(1, index, value));
                }
                ++fileIndex;
            }
            return result;
        }

        private String generateFullName(int index) {
            String indexPart = "";
            for (int i = 0; i < 5 - Integer.valueOf(index).toString().length(); ++i) {
                indexPart += "0";
            }
            indexPart += index;
            return "part-" + indexPart;
        }
    }

    public static void start(String input, String output) {
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
        FileInputFormat.setInputPaths(jobConf, new Path(input));
        FileOutputFormat.setOutputPath(jobConf, new Path(output));
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
