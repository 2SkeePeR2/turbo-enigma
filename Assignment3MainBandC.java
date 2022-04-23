package org.myorg2;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer; 

public class Assignment3MainBandC {
    
    public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private TreeMap<Integer, String> tmap1;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            tmap1 = new TreeMap<Integer, String>(Collections.reverseOrder());
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String firstToken = "";
            String secondToken = "";
            String thirdToken = "";
            while(tokenizer.hasMoreTokens()) {
                firstToken = tokenizer.nextToken();
                secondToken = tokenizer.nextToken();
                thirdToken = tokenizer.nextToken();
            }

            Integer parsedInteger = Integer.parseInt(thirdToken);
            String combinedText = firstToken + " " + secondToken;

            if(tmap1.containsKey(parsedInteger)) {
                String value1 = tmap1.get(parsedInteger);
                value1 = value1 + "//" + combinedText;
                tmap1.put(parsedInteger,value1); 
            }
            else {
               tmap1.put(parsedInteger, combinedText);
            }

            if(tmap1.size()>10) {
                tmap1.remove(tmap1.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for(Map.Entry<Integer, String> entry: tmap1.entrySet()) {
                int count = entry.getKey();
                String text = entry.getValue();
                StringTokenizer tokenizer2 = new StringTokenizer(text,"//");
                while(tokenizer2.hasMoreTokens()) {
                    String nextToken = tokenizer2.nextToken();
                    if(cnt<10) {
                        context.write(new Text(nextToken),new IntWritable(count));
                        cnt+=1;
                    }
                }
            }
        }
    }

    public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> tmap2;

        @Override
        public void setup(Context context) throws IOException,InterruptedException {
            tmap2 = new TreeMap<Integer,String>(Collections.reverseOrder());
        }

        public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;

            while(values.hasNext()) {
                sum+=values.next().get();
            }

            if(tmap2.containsKey(sum)) {
                String value = tmap2.get(sum);
                value = value + "//" + key.toString();
                tmap2.put(sum,value); 
            }
            else {
               tmap2.put(sum, key.toString());
            }

            if(tmap2.size()>10) {
                tmap2.remove(tmap2.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for(Map.Entry<Integer, String> entry: tmap2.entrySet()) {
                int count = entry.getKey();
                String text = entry.getValue();
                StringTokenizer tokenizer2 = new StringTokenizer(text,"//");
                while(tokenizer2.hasMoreTokens()) {
                    String nextToken = tokenizer2.nextToken();
                    if(cnt<10) {
                        context.write(new Text(nextToken),new IntWritable(count));
                        cnt+=1;
                    }
                }
            }
        }
    }
    
    public static class Map3 extends Mapper<LongWritable, Text, Text, IntWritable> {
        int sum = 0;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String firstToken = "";
            String secondToken = "";
            String thirdToken = "";
            while(tokenizer.hasMoreTokens()) {
                firstToken = tokenizer.nextToken();
                secondToken = tokenizer.nextToken();
                thirdToken = tokenizer.nextToken();
            }

            Integer parsedInteger = Integer.parseInt(thirdToken);
            String combinedText = firstToken + " " + secondToken;
            sum+=parsedInteger;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException { 
            context.write(new Text("cummulative frequency is - "), new IntWritable(sum));
        }
    }

    public static class Reduce3 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Integer sum;

        @Override
        public void setup(Context context) throws IOException,InterruptedException {
            sum = 0;
        }

        public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
            while(values.hasNext()) {
                sum+=values.next().get();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException,InterruptedException {
            // context.write(new Text("cummulative frequency is - "), new IntWritable(sum));
        }
    }

    public static void main(String [] args) {
        
        try {
            String inputPathForJobB = args[0];
            String outputPathForJobB = args[0] + "-B";
            String outputPathForJobC = args[0] + "-C"; 

            Configuration confB = new Configuration();
            
            Job jobB = Job.getInstance(confB, "top 10");
            jobB.setJarByClass(Assignment3MainBandC.class);
    
            jobB.setMapperClass(Map2.class);
            jobB.setReducerClass(Reduce2.class);
            jobB.setMapOutputKeyClass(Text.class);
            jobB.setMapOutputValueClass(IntWritable.class);
            jobB.setOutputKeyClass(Text.class);
            jobB.setOutputValueClass(IntWritable.class);
    
            FileInputFormat.addInputPath(jobB, new Path(inputPathForJobB));
            FileOutputFormat.setOutputPath(jobB, new Path(outputPathForJobB));
    
            jobB.waitForCompletion(true);

            Configuration confC = new Configuration();

            Job jobC = Job.getInstance(confC, "top 10 cummulative freq");
            jobC.setJarByClass(Assignment3MainBandC.class);

            jobC.setMapperClass(Map3.class);
            jobC.setReducerClass(Reduce3.class);
            jobC.setMapOutputKeyClass(Text.class);
            jobC.setMapOutputValueClass(IntWritable.class);
            jobC.setOutputKeyClass(Text.class);
            jobC.setOutputValueClass(IntWritable.class);
    
            FileInputFormat.addInputPath(jobC, new Path(outputPathForJobB));
            FileOutputFormat.setOutputPath(jobC, new Path(outputPathForJobC));

            jobC.waitForCompletion(true);
        }
        catch(Exception e) {
            System.out.println("Error occured");
        }
        
    }
}
