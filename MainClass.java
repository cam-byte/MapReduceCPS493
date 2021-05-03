package com.MapReduceCovidNB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.MapReduceCovidNB.MapperNB.TokenizerMapper;
import com.MapReduceCovidNB.ReducerNB.IntSumReducer;

public class MainClass {
	public static void main(String[] args) throws Exception {
		org.apache.log4j.BasicConfigurator.configure();
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf, "test");
	  job.setJarByClass(MapperNB.class);
	  job.setMapperClass(TokenizerMapper.class);
	  job.setCombinerClass(IntSumReducer.class);
	  job.setReducerClass(IntSumReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
