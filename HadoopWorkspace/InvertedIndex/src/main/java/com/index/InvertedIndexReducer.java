package com.index;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

import java.util.HashSet;

public class InvertedIndexReducer extends Reducer <Text,Text,Text,Text>{

    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{

        StringBuilder s = new StringBuilder();

        HashSet<String> set = new HashSet<String>();

        for(Text v:value){
            set.add(v.toString());
        }

        for(String v:set){
            s.append(v.toString());
            s.append(";");
        }

        context.write(key, new Text(s.toString()));

    }
}