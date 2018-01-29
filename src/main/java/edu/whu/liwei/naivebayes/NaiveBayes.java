package edu.whu.liwei.naivebayes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Include train and evaluation mathod
 * @author Liwei
 *
 */
public class NaiveBayes {
	
	public static class PathConf {
		public String trainingDataPath;
		public String testDataPath;
		public String classDocNumsPath;
		public String classWordDocNumsPath;
		public String conditionProbabilityPath;
		public String predictionResultPath;
		
		public PathConf(String trainingDataPath, String classDocNumsPath, 
				String classWordDocNumsPath, String conditionProbabilityPath) {
			this.trainingDataPath = trainingDataPath;
			this.classDocNumsPath = classDocNumsPath;
			this.classWordDocNumsPath = classWordDocNumsPath;
			this.conditionProbabilityPath = conditionProbabilityPath;
		}
		
		public PathConf(String trainingDataPath, String testDataPath, String classDocNumsPath, String classWordDocNumsPath,
				String conditionProbabilityPath, String predictionResultPath) {
			this.trainingDataPath = trainingDataPath;
			this.testDataPath = testDataPath;
			this.classDocNumsPath = classDocNumsPath;
			this.classWordDocNumsPath = classWordDocNumsPath;
			this.conditionProbabilityPath = conditionProbabilityPath;
			this.predictionResultPath = predictionResultPath;
		}
	}
	
	
	public static void train(PathConf pathConf) throws IOException {
		System.out.println("Training...");
		
		Configuration conf = new Configuration();

		FileSystem hdfs = FileSystem.get(conf);
			
		// Job1: count class document numbers
		Path trainingDataPath = new Path(pathConf.trainingDataPath);
		Path classDocNumsPath = new Path(pathConf.classDocNumsPath);
		if (hdfs.exists(classDocNumsPath)) // if exists, delete it
			hdfs.delete(classDocNumsPath, true); 
		Job job1 = new Job(conf, "ClassDocNums");
		job1.setJarByClass(NaiveBayes.class);	
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(MapReduce.ClassNameDocNumsMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setCombinerClass(MapReduce.ClassNameDocNumsReducer.class);
		job1.setReducerClass(MapReduce.ClassNameDocNumsReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		// add to controlled Job 
		ControlledJob ctrljob1 = new  ControlledJob(conf);
		ctrljob1.setJob(job1);
		// input and output path for job1
		FileInputFormat.addInputPath(job1, trainingDataPath);
		FileOutputFormat.setOutputPath(job1, classDocNumsPath);
		
		// Job2: count class words doc numbers
		Path classWordDocNumsPath = new Path(pathConf.classWordDocNumsPath);
		if (hdfs.exists(classWordDocNumsPath))
			hdfs.delete(classWordDocNumsPath, true);
		Job job2 = new Job(conf, "ClassWordDocNums");
		job2.setJarByClass(NaiveBayes.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(MapReduce.ClassWordDocsMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setReducerClass(MapReduce.ClassWordDocsReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		// add to controlled Job 
		ControlledJob ctrljob2 = new  ControlledJob(conf);
		ctrljob2.setJob(job2);
		// input and output path for job2
		FileInputFormat.addInputPath(job2, trainingDataPath);
		FileOutputFormat.setOutputPath(job2, classWordDocNumsPath);
		
		// Job3: calaulate condition probability
		Path conditionProbabilityPath = new Path(pathConf.conditionProbabilityPath);
		if (hdfs.exists(conditionProbabilityPath))
			hdfs.delete(conditionProbabilityPath, true);
		conf.set("classDocNumsFilePath", pathConf.classDocNumsPath);
		Job job3 = new Job(conf, "ConditionProbablility");
		job2.setJarByClass(NaiveBayes.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(MapReduce.ConditionProbablilityMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setReducerClass(MapReduce.ConditionProbablilityReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		// add to controlled Job 
		ControlledJob ctrljob3 = new  ControlledJob(conf);
		ctrljob2.setJob(job3);
		// input and output path for job3
		FileInputFormat.addInputPath(job3, classWordDocNumsPath);
		FileOutputFormat.setOutputPath(job3, conditionProbabilityPath);
		
		// add dependence between jobs
		ctrljob3.addDependingJob(ctrljob1);
		ctrljob3.addDependingJob(ctrljob2);
		
		// Main job controller 		
		JobControl jobCtrl = new JobControl("NaiveBayes");
		// add all jobs to main job controller
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		jobCtrl.addJob(ctrljob3);
		
		// start the multiThread
	    Thread  theController = new Thread(jobCtrl); 
	    theController.start(); 
	    while(true){
	        if(jobCtrl.allFinished()){ // print job information when job sunccessed 
	        	System.out.println(jobCtrl.getSuccessfulJobList()); 
	        	jobCtrl.stop(); 
	        	break; 
	        }
	    }  
		
	}
	
	public static void evaluation(PathConf pathConf) throws IOException {
		System.out.println("Evaluation...");
		
		Configuration conf = new Configuration();
		
		FileSystem hdfs = FileSystem.get(conf);
		
		// create DocClassPrediciton job
		Path testDataPath = new Path(pathConf.testDataPath);
		Path predictionResultPath = new Path(pathConf.predictionResultPath);
		if (hdfs.exists(predictionResultPath)) // if exists, delete it
			hdfs.delete(predictionResultPath, true); 
		conf.set("classDocNumsFilePath", pathConf.classDocNumsPath);
		conf.set("conditionProbablyFilePath", pathConf.conditionProbabilityPath);
		Job job = new Job(conf, "DocClassPrediciton");
		job.setJarByClass(NaiveBayes.class);	
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(MapReduce.DocClassPredicitonMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MapReduce.DocClassPredicitonReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// add to controlled Job 
		ControlledJob ctrljob = new  ControlledJob(conf);
		ctrljob.setJob(job);
		// input and output path for job1
		FileInputFormat.addInputPath(job, testDataPath);
		FileOutputFormat.setOutputPath(job, predictionResultPath);
		
		// get predicition result
		HashMap<String, String> docPredictionResult = new HashMap<String, String>();
		SequenceFile.Reader reader = null;
		
		reader = new SequenceFile.Reader(hdfs, predictionResultPath, conf); 
		Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while (reader.next(key, value)) {
			docPredictionResult.put(key.toString(), value.toString());
		}
		
		// compare real label of doc and predict label
		int right = 0, total = 0;
		FSDataInputStream inputStream = hdfs.open(testDataPath);
		BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
		String line = "";
		while ((line = buffer.readLine()) != null) {
			int splitIndex1 = value.toString().indexOf(":");
			int splitIndex2 = value.toString().indexOf(",");
			String docId = value.toString().substring(0, splitIndex1);
			String className = value.toString().substring(splitIndex1 + 1, splitIndex2 + 1);
			if (docPredictionResult.get(docId).equals(className))
				right++;
		}
		System.out.println("Accuracy: " + right + "/" + total + " = " + right/(double)total);
	}
	
	public static void main(String[] args) throws IOException {
		
		if (args.length < 6) {
			System.out.println("Usage: java -cp *.jar edu.whu.liwei.naivebayes trainingDataPath testDataPath"
					+ "classDocNumsPath classWordDocNumsPath conditionProbabilityPath predictionResultPath");
			System.out.println("trainingDataPath: training data path on hdfs, each line's format: docId:className word1 word2 word2 ...");
			System.out.println("testDataPath: test data path on hdfs, each line's format: docId:className word1 word2 word2 ...");
			System.out.println("classDocNumsPath: file used to save class docment numbers");
			System.out.println("classWordDocNumsPath: file used to save class word doc numbers");
			System.out.println("conditionProbabilityPath: file used to save condition probability");
			System.out.println("predictionResultPath: file used to save predicition result");
			return;
		}
		
		PathConf pathConf = new PathConf(args[0], args[1], args[2], args[3], args[4], args[5]);
		
		train(pathConf);
		evaluation(pathConf);
	}
}
