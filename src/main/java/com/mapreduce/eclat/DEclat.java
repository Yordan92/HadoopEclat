package com.mapreduce.eclat;

import com.mapreduce.eclat.mapper.CreatePairsMapper;
import com.mapreduce.eclat.mapper.WordCountMapper;
import com.mapreduce.eclat.reducers.CreatePairsReducer;
import com.mapreduce.eclat.reducers.WordCountReducer;
import com.mapreduce.eclat.writables.MyTaggedMapOutput;
import com.mapreduce.eclat.writables.TripleSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DEclat extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new DEclat(), new String[] {"/home/yordan/workspace/hadoop-dEclat-Map-Reduce/testSets", "/home/yordan/workspace/hadoop-dEclat-Map-Reduce/out"});
		System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		long currentTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		conf.set("","");
		String[] otherArgs = new GenericOptionsParser(conf, arg0).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: DEclat <in> <out>");
			System.exit(2);
		}

		int dir = 1;
		for (int i = 1; ; i++) {
			File f = new File(otherArgs[1]+ i);
			if (f.exists() && f.isDirectory()) {
				FileUtils.deleteDirectory(f);
			} else {
				break;
			}
		}
		Job job = new Job(conf, "StackOverflow Comment Word Count");
		job.setJarByClass(DEclat.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TripleSet.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + dir));
		boolean success = job.waitForCompletion(true);
		System.out.print("abcd");
//
//		JobConf job1 = new JobConf();
//		job1.setMapperClass(CreatePairsMapper.class);
//		job1.setReducerClass(CreatePairsReducer.class);
//
////		job1.setCombinerClass(WordCountReducer.class);
////		job1.setReducerClass(WordCountReducer.class);
//		job1.setOutputKeyClass(Text.class);
//		job1.setOutputValueClass(MyTaggedMapOutput.class);
//		org.apache.hadoop.mapred.FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+ "1/part-r-00000"));
//		org.apache.hadoop.mapred.FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+ "1/part-r-00000"));
//		org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + "2"));
//		JobClient.runJob(job1);
		FileSystem wdfs = FileSystem.get(conf);
		boolean isFinished = false;
		while(!isFinished) {
			Job job1= new Job(conf, "StackOverflow Comment Word Count");
			MultipleOutputs.addNamedOutput(job1, "text", TextOutputFormat.class,
					IntWritable.class, TripleSet.class);
			job1.setInputFormatClass(SequenceFileInputFormat.class);
			job1.setMapperClass(CreatePairsMapper.class);
//		job1.setCombinerClass(WordCountReducer.class);
			job1.setReducerClass(CreatePairsReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(TripleSet.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(job1, new Path(otherArgs[1]+ dir+"/part-r-00000"));
			dir++;
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + dir + "/"));
			job1.waitForCompletion(true);
			isFinished = !new File(otherArgs[1] + dir + "/text-r-00000").exists();
			wdfs.delete(new Path(otherArgs[1]+ (dir - 1) + "/part-r-00000"));
		}

        Path result = new Path(otherArgs[1] + dir + "/text-r-00000");
        FSDataOutputStream resultFile = wdfs.create(result);
//        Path[] path = new Path[dir - 1];
		for(int i=2 ; i < dir; i++) {
			Path path = new Path(otherArgs[1] + i + "/text-r-00000");
			IOUtils.write("\nL" + i + "\n", resultFile);
			try(InputStream is = wdfs.open(path))  {
				IOUtils.copy(is, resultFile);
			} catch (Exception ex) {

			}
        }
        IOUtils.write("Finished in :" + (System.currentTimeMillis() - currentTime) + "ms", resultFile);
        resultFile.close();

        return 0;
	}
}
