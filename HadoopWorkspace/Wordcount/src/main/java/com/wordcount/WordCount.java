package com.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
        if(args.length!=2){
            System.err.println("Invalid arguements please enter <Input> <Output>");
        }

       Configuration conf = new Configuration();
       Job job =  Job.getInstance(conf,"M");
       job.setJarByClass(WordCount.class);

       job.setMapperClass(WordCountMapper.class);
       job.setReducerClass(WordCountReducer.class);
       job.setCombinerClass(WordCountReducer.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);


       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));


       job.waitForCompletion(true);

    }
}
