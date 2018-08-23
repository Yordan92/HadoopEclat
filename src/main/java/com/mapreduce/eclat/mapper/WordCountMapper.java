package com.mapreduce.eclat.mapper;

import com.mapreduce.eclat.writables.TripleSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, TripleSet> {

    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        // Parse the input string into a nice map
        System.out.println(key);
        for (String set : value.toString().split("[,\\s\\t]")) {
            context.write(new Text(set), new TripleSet("Empty", Integer.parseInt(set), (LongWritable) key));
        }
    }

}
