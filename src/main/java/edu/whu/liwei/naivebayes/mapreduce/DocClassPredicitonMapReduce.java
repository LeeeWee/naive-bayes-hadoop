package edu.whu.liwei.naivebayes.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class DocClassPredicitonMapReduce {
	
	/**
	 * doc class predict mapper
	 * input test file, each line's form: docId:className, word1 word2 word3 ...
	 * mapper output docId and each class probability
	 * reducer output docId and most probable className
	 * @author liwei
	 *
	 */
	public static class DocClassPredicitonMapper extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * class probably, P(c) = doc_nums(c) / total_doc_nums
		 */
		private static HashMap<String, Double> classProbablility;
		/**
		 * condition probably, P(tk|c) = (doc_containing_tk_nums(c) + 1) / (doc_nums(c) + 2)
		 */
		private static HashMap<String, Double> wordsProbablility;
		
		// initial classProbably and wordsProbably
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			// get classDocNums and conditionProbably
			String ClassNameDocNumsFilePath = conf.get("classDocNumsFilePath");
			String ConditionProbablyFilePath = conf.get("conditionProbablyFilePath");
			try {
				HashMap<String, Integer> classDocNumsMap = Probability.getClassDocNums(ClassNameDocNumsFilePath);
				classProbablility = Probability.getPriorProbablility(classDocNumsMap);
				wordsProbablility = Probability.getWordsProbablility(ConditionProbablyFilePath, classDocNumsMap);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int splitIndex1 = value.toString().indexOf(":");
			int splitIndex2 = value.toString().indexOf(",");
			String docId = value.toString().substring(0, splitIndex1);
//			String className = value.toString().substring(splitIndex1 + 1, splitIndex2);
			for(Entry<String, Double> entry:classProbablility.entrySet()){ // iterate all class
				String mykey = entry.getKey();
				double tempValue = Math.log(entry.getValue()); // convert the predict value calculated by product to sum of log				
				StringTokenizer itr = new StringTokenizer(value.toString().substring(splitIndex2 + 1).toString());				
				while(itr.hasMoreTokens()){ // iterate all words				
					String tempkey = mykey + ":" + itr.nextToken(); // create key-value map <class:word>, and get the probability						
					if (wordsProbablility.containsKey(tempkey)) {
						// if <class:word> exists in wordsProbably, get the probability
						tempValue += Math.log(wordsProbablility.get(tempkey));
					} else { // if doesn't exist, using the probability of class probability
						tempValue += Math.log(wordsProbablility.get(mykey));						
					}
				}
				context.write(new Text(docId), new Text(mykey + ":" + tempValue)); // <docID,<class:probably>>
			}
		}
	}
	
	public static class DocClassPredicitonReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			boolean flag = false; // mark the first iteration
			String tempClass = null;
			double tempProbably = 0.0;
			// get the most probable class for each doc
	        for (Text value : values) { 
	        	int index = value.toString().indexOf(":");	        	
        		if(flag != true){
	        		tempClass = value.toString().substring(0, index);
	        		tempProbably = Double.parseDouble(value.toString().substring(index+1, value.toString().length()));	        		
	        		flag = true;
	        	}else{
	        		if(Double.parseDouble(value.toString().substring(index+1, value.toString().length())) > tempProbably)
	        			tempClass = value.toString().substring(0, index);
	        	}
        	}	        	
	        context.write(key, new Text(tempClass));
		}
	}
	
	/**
	 * run DocClassPredicitonMapReduce
	 */
	public static boolean run(Configuration conf, PathConf pathConf) {
		System.out.println("DocClassPredicitonMapReduce...");
		System.out.println("Reading training data from " + pathConf.testDataPath);
		try {
			FileSystem hdfs = FileSystem.get(conf);
		
			// Job2: count class words doc numbers
			Path testDataPath = new Path(pathConf.testDataPath);
			Path predictionResultPath = new Path(pathConf.predictionResultPath);
			if (hdfs.exists(predictionResultPath)) // if exists, delete it
				hdfs.delete(predictionResultPath, true); 
			conf.set("classDocNumsFilePath", pathConf.classDocNumsPath);
			conf.set("conditionProbablyFilePath", pathConf.conditionProbabilityPath);
			Job job = Job.getInstance(conf);
			job.setJobName("DocClassPrediciton");
			job.setJarByClass(DocClassPredicitonMapReduce.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapperClass(DocClassPredicitonMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(DocClassPredicitonReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// add to controlled Job 
			ControlledJob ctrljob = new  ControlledJob(conf);
			ctrljob.setJob(job);
			// input and output path for job1
			FileInputFormat.addInputPath(job, testDataPath);
			FileOutputFormat.setOutputPath(job, predictionResultPath);
			
			boolean f = job.waitForCompletion(true);
			return f;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
		
	}
}
