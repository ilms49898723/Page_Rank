package com.github.ilms49898723.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class IndexParser {
    private static class IndexMapper
            extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object o, Text text, OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = text.toString().split(",");
            int i = Integer.parseInt(tokens[2]);
            int j = Integer.parseInt(tokens[1]);
            outputCollector.collect(new IntWritable(i), new IntWritable(j));
        }
    }

    private static class IndexReducer
            extends MapReduceBase
            implements Reducer<IntWritable, IntWritable, ObjectWritable, Text> {
        @Override
        public void reduce(IntWritable intWritable, Iterator<IntWritable> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(intWritable.toString());
            while (iterator.hasNext()) {
                stringBuilder.append(",").append(iterator.next().toString());
            }
            outputCollector.collect(null, new Text(stringBuilder.toString()));
        }
    }

    public static void start(String input, String output) {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("Index Parse");
        jobConf.setJarByClass(PageRank.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(IntWritable.class);
        jobConf.setOutputKeyClass(ObjectWritable.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setMapperClass(IndexMapper.class);
        jobConf.setReducerClass(IndexReducer.class);
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
