package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Assignment3MainAandD {
    public static class Map0 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
          
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          String line = value.toString();
          StringTokenizer tokenizer = new StringTokenizer(line);
          String previousToken = "";
          while (tokenizer.hasMoreTokens()) {
            if(previousToken.length() != 0) {
              String nextToken = tokenizer.nextToken();
              String tokenOutput = previousToken + " " + nextToken;
              word.set(tokenOutput);
              output.collect(word, one);
              previousToken = nextToken;
            }
            else {
              previousToken = tokenizer.nextToken();
            }
          }
        }
      }
    
    public static class Reduce0 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          int sum = 0;
          while (values.hasNext()) {
            sum += values.next().get();
          }
     
          output.collect(key, new IntWritable(sum));
        }
    }
       
    public static class MapA extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
    
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          String line = value.toString();
          StringTokenizer tokenizer = new StringTokenizer(line);
          String firstText = tokenizer.nextToken();
          String secondText = tokenizer.nextToken();
          String combinedText = firstText + " " + secondText;
          word.set(combinedText);
          output.collect(word, one);
        }
    }
    
    public static class ReduceB extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private Integer a = 1; 
    
        public void reduce(Text key,Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          output.collect(key, new IntWritable(a));
          a+=1;
        }
    }
    
    public static class MapD extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
        
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String firstText = tokenizer.nextToken();
        String secondText = tokenizer.nextToken();
        String thirdText = tokenizer.nextToken();
        String combinedText = firstText + " " + secondText;
        Integer parsedInteger = Integer.parseInt(thirdText);

        if(parsedInteger == 1) {
          word.set(combinedText);
          output.collect(word, one);
        }
      }
    }

    public static class ReduceD extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
      private Integer a = 1;

      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        output.collect(key, new IntWritable(a));
        a+=1;      
      }
    }
 
    public static void main(String[] args) throws Exception {
    
        String inputPathForJob = args[0];
        String outputPathForJob = args[1];
        String outputPathForJobA = args[1] + "-A";
        String outputPathForJobD = args[1] + "-D";
    
        JobConf conf = new JobConf(Assignment3MainAandD.class);
        
        conf.setJobName("countbigrams");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map0.class);
        conf.setCombinerClass(Reduce0.class);
        conf.setReducerClass(Reduce0.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
    
        FileInputFormat.setInputPaths(conf, new Path(inputPathForJob));
        FileOutputFormat.setOutputPath(conf, new Path(outputPathForJob));
    
        JobClient.runJob(conf);
    
        JobConf confA = new JobConf(Assignment3MainAandD.class);
        
        confA.setJobName("countuniquebigrams");
        confA.setMapOutputKeyClass(Text.class);
        confA.setMapOutputValueClass(IntWritable.class);
        confA.setOutputKeyClass(Text.class);
        confA.setOutputValueClass(IntWritable.class);
        confA.setMapperClass(MapA.class);
        confA.setCombinerClass(ReduceB.class);
        confA.setReducerClass(ReduceB.class);
        confA.setInputFormat(TextInputFormat.class);
        confA.setOutputFormat(TextOutputFormat.class);
    
        FileInputFormat.setInputPaths(confA, new Path(outputPathForJob));
        FileOutputFormat.setOutputPath(confA, new Path(outputPathForJobA));
    
        JobClient.runJob(confA);

        JobConf confD = new JobConf(Assignment3MainAandD.class);

        confD.setJobName("countonebigrams");
        confD.setMapOutputKeyClass(Text.class);
        confD.setMapOutputValueClass(IntWritable.class);
        confD.setOutputKeyClass(Text.class);
        confD.setOutputValueClass(IntWritable.class);
        confD.setMapperClass(MapD.class);
        confD.setCombinerClass(ReduceD.class);
        confD.setReducerClass(ReduceD.class);
        confD.setInputFormat(TextInputFormat.class);
        confD.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(confD, new Path(outputPathForJob));
        FileOutputFormat.setOutputPath(confD, new Path(outputPathForJobD));
    
        JobClient.runJob(confD);      
    }   
}
