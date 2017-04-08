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

public class MatrixParser {
    private static class ParserMapper
            extends MapReduceBase
            implements Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object o, Text text, OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
            if (text.toString().startsWith("#")) {
                return;
            }
            String[] tokens = text.toString().split("\\s+");
            if (tokens.length != 2) {
                throw new IOException("Length of tokens should be 2. Got " + tokens.length);
            }
            IntWritable key = new IntWritable(Integer.parseInt(tokens[0]));
            IntWritable value = new IntWritable(Integer.parseInt(tokens[1]));
            outputCollector.collect(key, value);
        }
    }

    private static class ParserReducer
            extends MapReduceBase
            implements Reducer<IntWritable, IntWritable, ObjectWritable, Text> {
        @Override
        public void reduce(IntWritable intWritable, Iterator<IntWritable> iterator, OutputCollector<ObjectWritable, Text> outputCollector, Reporter reporter) throws IOException {
            List<String> values = new ArrayList<>();
            while (iterator.hasNext()) {
                IntWritable value = new IntWritable(iterator.next().get());
                values.add(value.toString());
            }
            String prefix = intWritable.get() + "," + values.size() + ",";
            Text output = new Text(prefix + String.join(",", values));
            outputCollector.collect(null, output);
        }
    }

    public static void start(String input) {
        JobConf jobConf = new JobConf();
        jobConf.setJobName("Matrix Parse");
        jobConf.setJarByClass(PageRank.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(IntWritable.class);
        jobConf.setOutputKeyClass(ObjectWritable.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setMapperClass(ParserMapper.class);
        jobConf.setReducerClass(ParserReducer.class);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobConf, new Path(input));
        FileOutputFormat.setOutputPath(jobConf, new Path("matrixparse"));
        try {
            JobClient.runJob(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
