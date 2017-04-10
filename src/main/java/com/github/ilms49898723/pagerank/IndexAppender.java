package com.github.ilms49898723.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexAppender {
    private static class IndexMapper
            extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, Text> {
        @Override
        public void map(Object o, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = text.toString().split(",");
            if (tokens[0].equalsIgnoreCase("r")) {
                int index = Integer.parseInt(tokens[1]);
                outputCollector.collect(new IntWritable(index), new Text(text.toString()));
            } else {
                int index = Integer.parseInt(tokens[0]);
                List<String> indices = new ArrayList<>();
                for (int i = 1; i < tokens.length; ++i) {
                    indices.add(tokens[i]);
                }
                outputCollector.collect(new IntWritable(index), new Text(String.join(",", indices)));
            }
        }
    }

    private static class IndexReducer
            extends MapReduceBase
            implements Reducer<IntWritable, Text, ObjectWritable, Text> {
        @Override
        public void reduce(IntWritable intWritable, Iterator<Text> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            StringBuilder output = new StringBuilder();
            ArrayList<String> values = new ArrayList<>();
            while (iterator.hasNext()) {
                Text next = iterator.next();
                String str = next.toString();
                if (str.startsWith("R")) {
                    output.append(str);
                } else {
                    values.add(str);
                }
            }
            output.append(",").append(String.join(",", values));
            outputCollector.collect(null, new Text(output.toString()));
        }
    }

    public static void start(String input, String rInput, String output) {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("Index Parse");
        jobConf.setJarByClass(PageRank.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(ObjectWritable.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setMapperClass(IndexMapper.class);
        jobConf.setReducerClass(IndexReducer.class);
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
