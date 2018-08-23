package com.mapreduce.eclat.mapper;


import com.mapreduce.eclat.writables.MyTaggedMapOutput;
import com.mapreduce.eclat.writables.TripleSet;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CreatePairsMapper extends Mapper<Object, TripleSet, Text, TripleSet> {

    private Text word = new Text();
    public void map(Object key, TripleSet value, Context context) throws IOException,
            InterruptedException {
      context.write(value.getPrefix(), value);

    }

}


//public class CreatePairsMapper extends DataJoinMapperBase {
//    @Override
//    protected Text generateInputTag(String inputFile) {
//        return new Text(Math.random() >=0.5 ? "1" : "0");
//    }
//
//    @Override
//    protected TaggedMapOutput generateTaggedMapOutput(Object o) {
//        final Text ab = new Text(o.toString());
//        MyTaggedMapOutput mt = new MyTaggedMapOutput(ab);
//       mt.setTag(this.inputTag);
//        return mt;
//
//    }
//
//    @Override
//    protected Text generateGroupKey(TaggedMapOutput taggedMapOutput) {
//        // first column in the input tab separated files becomes the key (to perform the JOIN)
//        String line = ((Text) taggedMapOutput.getData()).toString();
//        Pattern p = Pattern.compile("prefix=(\\w*),");
//        Matcher m = p.matcher(line);
//        boolean b = m.find();
//        if (b) {
//            return new Text(m.group(1));
//        }
//        return new Text();
//    }
//
//}
