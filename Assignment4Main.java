package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Assignment4Main {
    public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            ArrayList<String> arrayListOfStrings = new ArrayList<String>();
            while(tokenizer.hasMoreTokens()) {
                arrayListOfStrings.add(tokenizer.nextToken());
            }

            if(arrayListOfStrings.size()>1) {
                String firstToken = arrayListOfStrings.get(0);
                String secondToken = arrayListOfStrings.get(1);
                word.set(firstToken);
                Integer parsedInteger = Integer.parseInt(secondToken);
                output.collect(word, new IntWritable(parsedInteger));
            }
        }
    }
    
    public static class Reduce1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          int sum = 0;
          int count = 0;
          while (values.hasNext()) {
            sum += values.next().get();
            count += 1;
          }

          int average = sum/count;

          output.collect(key, new IntWritable(average));
        }
    }

    public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            ArrayList<String> arrayListOfStrings = new ArrayList<String>();
            while(tokenizer.hasMoreTokens()) {
                arrayListOfStrings.add(tokenizer.nextToken());
            }

            if(arrayListOfStrings.size()>1) {
                String firstToken = arrayListOfStrings.get(2);
                String secondToken = arrayListOfStrings.get(1);
                word.set(firstToken);
                Integer parsedInteger = Integer.parseInt(secondToken);
                output.collect(word, new IntWritable(parsedInteger));
            }
       }
    }

    public static class Reduce2 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          int count = 0;
          while (values.hasNext()) {
            values.next().get();
            count+=1;
          }

          output.collect(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
    
        String inputPathForJob = args[0];
        String outputPathForJob = args[1];
        String outputPathForJobA = outputPathForJob + "-A";
        String outputPathForJobB = outputPathForJob + "-B";
    
        JobConf conf1 = new JobConf(Assignment4Main.class);
        
        conf1.setJobName("averagescores");
        conf1.setMapOutputKeyClass(Text.class);
        conf1.setMapOutputValueClass(IntWritable.class);
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(IntWritable.class);
        conf1.setMapperClass(Map1.class);
        conf1.setCombinerClass(Reduce1.class);
        conf1.setReducerClass(Reduce1.class);
        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);
    
        FileInputFormat.setInputPaths(conf1, new Path(inputPathForJob));
        FileOutputFormat.setOutputPath(conf1, new Path(outputPathForJobA));
    
        JobClient.runJob(conf1);

        JobConf conf2 = new JobConf(Assignment4Main.class);

        conf2.setJobName("counttotaldatereviews");
        conf2.setMapOutputKeyClass(Text.class);
        conf2.setMapOutputValueClass(IntWritable.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(IntWritable.class);
        conf2.setMapperClass(Map2.class);
        conf2.setCombinerClass(Reduce2.class);
        conf2.setReducerClass(Reduce2.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf2, new Path(inputPathForJob));
        FileOutputFormat.setOutputPath(conf2, new Path(outputPathForJobB));

        JobClient.runJob(conf2);
    }         
}