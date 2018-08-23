package com.mapreduce.eclat.writables;

import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyTaggedMapOutput extends TaggedMapOutput {
    private Text ab;

    public MyTaggedMapOutput() {
        super();
        ab = new Text();
    }
    public MyTaggedMapOutput(Text ab) {
        this.ab = ab;
    }

    @Override
    public Writable getData() {
        return ab;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ab.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ab.readFields(dataInput);
    }
}
