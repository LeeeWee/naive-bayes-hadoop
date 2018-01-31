package edu.whu.liwei.naivebayes.mapreduce;

import java.io.IOException;

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


public class ClassDocNumsMapReduce {

	/**
	 * The first MapReduce
	 * process the serialized files to count the number of docs corresponding to each class
	 * input source text file(lineNumber:className, word1 word2 ...), and output <<className>, classDocNums>
	 * @author Liwei
	 */
	public static class ClassNameDocNumsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text newKey = new Text();
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int splitIndex1 = value.toString().indexOf(":");
			int splitIndex2 = value.toString().indexOf(",");
			newKey.set(value.toString().substring(splitIndex1 + 1, splitIndex2));
			context.write(newKey, one);
		}
	}
	
	public static class ClassNameDocNumsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
	 * run ClassNameDocNumsMapReduce
	 */
	public static boolean run(Configuration conf, PathConf pathConf) {
		System.out.println("ClassNameDocNumsMapReduce...");
		System.out.println("Reading training data from " + pathConf.trainingDataPath);
		try {
			FileSystem hdfs = FileSystem.get(conf);
		
			Path trainingDataPath = new Path(pathConf.trainingDataPath);
			Path classDocNumsPath = new Path(pathConf.classDocNumsPath);
			if (hdfs.exists(classDocNumsPath)) // if exists, delete it
				hdfs.delete(classDocNumsPath, true); 
			Job job = Job.getInstance(conf);
			job.setJobName("ClassDocNums");
			job.setJarByClass(ClassDocNumsMapReduce.class);	
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapperClass(ClassNameDocNumsMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setCombinerClass(ClassNameDocNumsReducer.class);
			job.setReducerClass(ClassNameDocNumsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			// add to controlled Job 
			ControlledJob ctrljob = new  ControlledJob(conf);
			ctrljob.setJob(job);
			// input and output path for job1
			FileInputFormat.addInputPath(job, trainingDataPath);
			FileOutputFormat.setOutputPath(job, classDocNumsPath);
			
			boolean f = job.waitForCompletion(true);
			return f;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
		
	}
	
}
