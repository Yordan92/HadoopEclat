package com.mapreduce.eclat.writables;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;

public class TripleSet implements Writable {

    private Text prefix = new Text();
    private IntWritable current = new IntWritable(0);
    private ArrayWritable positions = new ArrayWritable(LongWritable.class);

    public TripleSet(String prefix, Integer current, LongWritable positions) {
        if (prefix == null) {
            setPrefix("F");
        } else {
            setPrefix(prefix);
        }
        setCurrent(new IntWritable(current));
        add(positions);
    }

    public TripleSet() {
        positions.set(new LongWritable[0]);
    }

    public void setPositions(ArrayWritable positions) {
        this.positions = positions;
    }

    public void setPrefix(Text prefix) {
        this.prefix = prefix;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        prefix.write(dataOutput);
        current.write(dataOutput);
        positions.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        prefix.readFields(dataInput);
        current.readFields(dataInput);
        positions.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "TripleSet{" +
                "prefix=" + prefix +
                ", current=" + current +
                ", positions=" + Arrays.toString(positions.get()) +
                '}';
    }

    public Text getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        if (prefix == null) {
            return ;
        }
        try {
            byte[] bytesPrefix = prefix.getBytes("UTF-8");
            this.prefix.append(bytesPrefix, 0, bytesPrefix.length);
            if (this.prefix.getLength() != 0) {
                this.prefix.append(",".getBytes("UTF-8"),0, ",".getBytes("UTF-8").length);
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public IntWritable getCurrent() {
        return current;
    }

    public void setCurrent(IntWritable current) {
        this.current = current;
    }

    public ArrayWritable getPositions() {
        return positions;
    }

    public void add(LongWritable positions) {
        Writable[] old = this.positions.get();
        Writable[] newWritable;
        if (old == null) {
            newWritable = new Writable[1];
            newWritable[0] = positions;
        } else {
            for (Writable position:old) {
                if (position.equals(positions)) {
                    return;
                }
            }
            newWritable = Arrays.copyOf(old, old.length + 1);
            newWritable[old.length + 1] = positions;
        }
        this.positions.set(newWritable);
    }

    public int intersectPositions(ArrayWritable positionsB) {
        HashSet<Writable> a = new HashSet<Writable>(Arrays.asList(this.positions.get()));
        HashSet<Writable> b = new HashSet<Writable>(Arrays.asList(positionsB.get()));
        intersect(a,b);
        this.positions.set(a.toArray(new LongWritable[0]));
        return a.size();

    }

    public void addUniquePositions(ArrayWritable positionsB) {
        HashSet<Writable> a = new HashSet<Writable>(Arrays.asList(this.positions.get()));
        HashSet<Writable> b = new HashSet<Writable>(Arrays.asList(positionsB.get()));
        a.addAll(b);
        this.positions.set(a.toArray(new LongWritable[0]));

    }

    private void intersect(HashSet<Writable> b, HashSet<Writable> a) {
        b.removeIf( x -> !a.contains(x));
    }
}
