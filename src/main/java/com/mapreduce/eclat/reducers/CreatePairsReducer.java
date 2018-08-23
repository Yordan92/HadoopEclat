package com.mapreduce.eclat.reducers;

import com.mapreduce.eclat.writables.TripleSet;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CreatePairsReducer extends Reducer<Text, TripleSet, Text, TripleSet>  {

    private MultipleOutputs multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs  = new MultipleOutputs(context);
    }

    public void reduce(Text key, Iterable<TripleSet> values, Context context)
            throws IOException, InterruptedException {

        List<TripleSet> array = new ArrayList<>();
        for (TripleSet value : values) {
            array.add(WritableUtils.clone(value, context.getConfiguration()));
           System.out.println(value);
        }

        if (array.size() == 1) {
           return ;
        }
        for (int i = 0 ; i < array.size(); i ++) {
            for (int j = 0; j < array.size(); j++) {
                TripleSet first = array.get(i);
                TripleSet second = array.get(j);
                if (first.getCurrent().compareTo(second.getCurrent())==-1) {
                    TripleSet firstCloned = WritableUtils.clone(first, context.getConfiguration());
                    if (firstCloned.intersectPositions(second.getPositions()) > 0) {
                        byte[] newPrefix = String.format("%d,", first.getCurrent().get()).getBytes(StandardCharsets.UTF_8);
                        firstCloned.getPrefix().append(newPrefix,0, newPrefix.length);
                        firstCloned.setCurrent(second.getCurrent());
                        context.write(firstCloned.getPrefix(), firstCloned);
                        multipleOutputs.write("text", new IntWritable(firstCloned.getPositions().get().length), generteResult(firstCloned));
                    }
                }
            }
        }
    }

    public Text generteResult(TripleSet tripleSet) {
        String prefix = tripleSet.getPrefix().toString() + Integer.toString(tripleSet.getCurrent().get());
        return new Text(prefix);
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
