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
import java.util.Iterator;
import java.util.List;

public class MatrixTrans {
    private static class MatrixValue implements Writable {
        private int mIndex;
        private double mValue;

        public MatrixValue() {
            mIndex = -1;
            mValue = -1;
        }

        public MatrixValue(int index, double value) {
            mIndex = index;
            mValue = value;
        }

        public int getIndex() {
            return mIndex;
        }

        public double getValue() {
            return mValue;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(mIndex);
            out.writeDouble(mValue);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mIndex = in.readInt();
            mValue = in.readDouble();
        }

        @Override
        public String toString() {
            return "MatrixValue{" +
                    ", mIndex=" + mIndex +
                    ", mValue=" + mValue +
                    '}';
        }
    }

    private static class MatrixMapper
            extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, MatrixValue> {
        @Override
        public void map(Object o, Text text, OutputCollector<IntWritable, MatrixValue> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = text.toString().split(",");
            int i = Integer.parseInt(tokens[1]);
            int j = Integer.parseInt(tokens[2]);
            double v = Double.parseDouble(tokens[3]);
            IntWritable key = new IntWritable(j);
            MatrixValue value = new MatrixValue(i, v);
            outputCollector.collect(key, value);
        }
    }

    private static class MatrixReducer
            extends MapReduceBase
            implements Reducer<IntWritable, MatrixValue, ObjectWritable, Text> {
        @Override
        public void reduce(IntWritable intWritable, Iterator<MatrixValue> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            List<MatrixValue> values = new ArrayList<>();
            while (iterator.hasNext()) {
                MatrixValue value = iterator.next();
                values.add(new MatrixValue(value.getIndex(), value.getValue()));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("M,").append(intWritable.toString()).append(",").append(values.size());
            for (MatrixValue value : values) {
                builder.append(",").append(value.getIndex()).append(",").append(value.getValue());
            }
            outputCollector.collect(null, new Text(builder.toString()));
        }
    }

    public static void start(String input, String output) {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("Matrix Trans");
        jobConf.setJarByClass(PageRank.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(IntWritable.class);
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
