package com.github.ilms49898723.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RankUpdater {
    private static class MatrixValue {
        private int mI;
        private int mJ;
        private String mValue;

        public MatrixValue() {
            mI = -1;
            mJ = -1;
            mValue = "-1";
        }

        public MatrixValue(int i, int j, String v) {
            mI = i;
            mJ = j;
            mValue = v;
        }

        public int getI() {
            return mI;
        }

        public int getJ() {
            return mJ;
        }

        public String getValue() {
            return mValue;
        }

        public void setValue(String value) {
            mValue = value;
        }
    }

    private static class RankMapper
            extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, Text> {
        @Override
        public void map(Object o, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(new IntWritable(0), new Text(text.toString()));
        }
    }

    private static class RankReducer
            extends MapReduceBase
            implements Reducer<IntWritable, Text, ObjectWritable, Text> {
        @Override
        public void reduce(IntWritable intWritable, Iterator<Text> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            List<MatrixValue> values = new ArrayList<>();
            BigDecimal sum = new BigDecimal("0.0");
            while (iterator.hasNext()) {
                String data = new Text(iterator.next()).toString();
                String[] tokens = data.split(",");
                int i = Integer.parseInt(tokens[1]);
                int j = Integer.parseInt(tokens[2]);
                String v = tokens[3];
                values.add(new MatrixValue(i, j, v));
                sum = sum.add(new BigDecimal(v));
            }
            for (MatrixValue value : values) {
                BigDecimal newValue = new BigDecimal(value.getValue());
                newValue = newValue.add(new BigDecimal(1.0).subtract(sum).divide(new BigDecimal(PageRank.N), BigDecimal.ROUND_HALF_EVEN));
                value.setValue(newValue.toString());
                String output = "R," + value.getI() + "," + value.getJ() + "," + value.getValue();
                outputCollector.collect(null, new Text(output));
            }
        }
    }

    public static void start(String input, String output) {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("Matrix Multiplication");
        jobConf.setJarByClass(PageRank.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(ObjectWritable.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setMapperClass(RankMapper.class);
        jobConf.setReducerClass(RankReducer.class);
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
