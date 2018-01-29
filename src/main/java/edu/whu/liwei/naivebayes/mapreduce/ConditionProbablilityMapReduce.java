package edu.whu.liwei.naivebayes.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.whu.liwei.naivebayes.PathConf;

public class ConditionProbablilityMapReduce {
	
	/**
	 * the third MapReduce, used to calculate condition probability
	 * input ClassWordDocsReducer file and set conf with ClassNameDocNumsFilePath
	 * output <<className:word>, conditionProbability>
	 * @author liwei
	 *
	 */
	public static class ConditionProbablilityMapper extends Mapper<Text, IntWritable, Text, DoubleWritable> {
		/**
		 * map doc nums to each class
		 */
		HashMap<String, Integer> classDocNumsMap;
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			// get classDocNums
			String ClassNameDocNumsFilePath = conf.get("classDocNumsFilePath");
			try {
				classDocNumsMap = Probability.getClassDocNums(ClassNameDocNumsFilePath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			// calculate probably mapper
			int splitIndex = key.toString().indexOf(":");
			String className = key.toString().substring(0, splitIndex);
			Integer classDocNums = classDocNumsMap.get(className);
			DoubleWritable newValue =  new DoubleWritable(value.get() + 1 / (double) (classDocNums + 2));
			context.write(key, newValue);
		}
	}
	
	public static class ConditionProbablilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));			
		}
	}
	
	/**
	 * run ConditionProbablilityMapReduce
	 */
	public static boolean run(Configuration conf, PathConf pathConf) {
		System.out.println("ConditionProbablilityMapReduce...");
		try {
			FileSystem hdfs = FileSystem.get(conf);
		
			// Job2: count class words doc numbers
			Path classWordDocNumsPath = new Path(pathConf.classWordDocNumsPath);
			Path conditionProbabilityPath = new Path(pathConf.conditionProbabilityPath);
			if (hdfs.exists(conditionProbabilityPath)) // if exists, delete it
				hdfs.delete(conditionProbabilityPath, true); 
			conf.set("classDocNumsFilePath", pathConf.classDocNumsPath);
			Job job = Job.getInstance(conf);
			job.setJobName("ConditionProbablility");
			job.setJarByClass(ConditionProbablilityMapReduce.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapperClass(ConditionProbablilityMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setReducerClass(ConditionProbablilityReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			// add to controlled Job 
			ControlledJob ctrljob = new  ControlledJob(conf);
			ctrljob.setJob(job);
			// input and output path for job3
			FileInputFormat.addInputPath(job, classWordDocNumsPath);
			FileOutputFormat.setOutputPath(job, conditionProbabilityPath);
			
			boolean f = job.waitForCompletion(true);
			return f;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
		
	}
}
