package com.mapreduce.eclat.reducers;

import com.mapreduce.eclat.writables.TripleSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, TripleSet, Text, TripleSet> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<TripleSet> values, Context context)
            throws IOException, InterruptedException {
        TripleSet reduced = new TripleSet();
        for (TripleSet val : values) {
            if (reduced.getPrefix().toString().isEmpty()) {
                reduced.setPrefix(val.getPrefix());
                reduced.setCurrent(val.getCurrent());
            }
            reduced.addUniquePositions(val.getPositions());
        }
        context.write(key, reduced);
    }

}
