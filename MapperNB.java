package com.MapReduceCovidNB;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapperNB {
		public static class TokenizerMapper
	    extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
	    	//object key = word , text value = count
	    	final String delimeter = ", ";
	    	Text yes = new Text("yes");
	    	Text no = new Text("no");
	    	Text onenum = new Text("1");
	    	Text zero = new Text("0");
	   StringTokenizer itr = new StringTokenizer(value.toString(),delimeter);
	   while (itr.hasMoreTokens()) {
			   //word.set(itr.nextToken());
			   word.set(itr.nextToken());
			   if(word.equals(onenum) || word.equals(zero))
			   {
				   context.write(word,one);
			   }else if(word.equals(yes) || word.equals(no))
				   {
					   context.write(word, one);
			   }else {
				   continue;
			   }
	   		}
	    }
	}

		public static class IntSumReducer
	    extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
			//text key = 
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
	   }
	   result.set(sum);
	   context.write(key, result);
	 }
	}
	public static void main(String[] args) throws Exception {
	 org.apache.log4j.BasicConfigurator.configure();
	 Configuration conf = new Configuration();
	 Job job = Job.getInstance(conf, "word count");
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
