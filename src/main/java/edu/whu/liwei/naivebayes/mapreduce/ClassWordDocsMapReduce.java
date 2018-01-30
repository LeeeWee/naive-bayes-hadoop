package edu.whu.liwei.naivebayes.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.whu.liwei.naivebayes.PathConf;

public class ClassWordDocsMapReduce {

	/**
	 * The second MapReduce, used to count the number of docs containing each word in each class
	 * input source text file(lineNumber:className, word1 word2 ...), and output <<className:word>, wordDocsCountInClass>
	 * @author Liwei
	 *
	 */
	public static class ClassWordDocsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int splitIndex1 = value.toString().indexOf(":");
			int splitIndex2 = value.toString().indexOf(",");
			String className = value.toString().substring(splitIndex1 + 1, splitIndex2);
			StringTokenizer itr = new StringTokenizer(value.toString().substring(splitIndex2 + 1));
			Set<String> wordSet = new HashSet<String>();
			while(itr.hasMoreTokens()){
				wordSet.add(itr.nextToken());
			}
			for (String word : wordSet) {
				context.write(new Text(className + ":" + word), new IntWritable(1));
			}
		}
	}
	
	public static class ClassWordDocsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);			
		}
	}
	
	/**
	 * run ClassWordDocsMapReduce
	 */
	public static boolean run(Configuration conf, PathConf pathConf) {
		System.out.println("ClassWordDocsMapReduce...");
		try {
			FileSystem hdfs = FileSystem.get(conf);
		
			Path trainingDataPath = new Path(pathConf.trainingDataPath);
			Path classWordDocNumsPath = new Path(pathConf.classWordDocNumsPath);
			if (hdfs.exists(classWordDocNumsPath)) // if exists, delete it
				hdfs.delete(classWordDocNumsPath, true); 
			Job job = Job.getInstance(conf);
			job.setJobName("ClassWordDocNums");
			job.setJarByClass(ClassWordDocsMapReduce.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapperClass(ClassWordDocsMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setReducerClass(ClassWordDocsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			// add to controlled Job 
			ControlledJob ctrljob = new  ControlledJob(conf);
			ctrljob.setJob(job);
			// input and output path for job
			FileInputFormat.addInputPath(job, trainingDataPath);
			FileOutputFormat.setOutputPath(job, classWordDocNumsPath);
			
			boolean f = job.waitForCompletion(true);
			return f;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
		
	}
	
}
