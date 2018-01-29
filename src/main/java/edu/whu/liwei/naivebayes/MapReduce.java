package edu.whu.liwei.naivebayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapReduce {
	
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
			newKey.set(value.toString().substring(splitIndex1 + 1, splitIndex2 + 1));
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
	 * The second MapReduce, used to count the number of docs containing each word in each class
	 * input source text file(lineNumber:className, word1 word2 ...), and output <<className:word>, wordDocsCountInClass>
	 * @author Liwei
	 *
	 */
	public static class ClassWordDocsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int splitIndex1 = value.toString().indexOf(":");
			int splitIndex2 = value.toString().indexOf(",");
			String className = value.toString().substring(splitIndex1 + 1, splitIndex2 + 1);
			StringTokenizer itr = new StringTokenizer(value.toString());
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
	 * the third MapReduce, used to calculate condition probability
	 * input ClassNameDocNumsReducer file and set conf with ClassNameDocNumsFilePath
	 * output <<className:word>, conditionProbability>
	 * @author liwei
	 *
	 */
	public static class ConditionProbablyMapper extends Mapper<Text, IntWritable, Text, DoubleWritable> {
		/**
		 * map doc nums to each class
		 */
		HashMap<String, Integer> classDocNumsMap;
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			// get classDocNums
			String ClassNameDocNumsFilePath = conf.get("ClassNameDocNumsFilePath");
			try {
				classDocNumsMap = NaiveBayes.getClassDocNums(ClassNameDocNumsFilePath);
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
	
	public static class ConditionProbablyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));			
		}
	}
	
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
		private static HashMap<String, Double> classProbably;
		/**
		 * condition probably, P(tk|c) = (doc_containing_tk_nums(c) + 1) / (doc_nums(c) + 2)
		 */
		private static HashMap<String, Double> wordsProbably;
		
		// initial classProbably and wordsProbably
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			// get classDocNums and conditionProbably
			String ClassNameDocNumsFilePath = conf.get("ClassNameDocNumsFilePath");
			String ConditionProbablyFilePath = conf.get("ConditionProbablyFilePath");
			try {
				HashMap<String, Integer> classDocNumsMap = NaiveBayes.getClassDocNums(ClassNameDocNumsFilePath);
				classProbably = NaiveBayes.getPriorProbably(classDocNumsMap);
				wordsProbably = NaiveBayes.getWordsProbably(ConditionProbablyFilePath, classDocNumsMap);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int splitIndex1 = value.toString().indexOf(":");
			int splitIndex2 = value.toString().indexOf(",");
			String docId = value.toString().substring(0, splitIndex1);
			String className = value.toString().substring(splitIndex1 + 1, splitIndex2 + 1);
			for(Entry<String, Double> entry:classProbably.entrySet()){ // iterate all class
				String mykey = entry.getKey();
				double tempValue = Math.log(entry.getValue()); // convert the predict value calculated by product to sum of log				
				StringTokenizer itr = new StringTokenizer(value.toString());				
				while(itr.hasMoreTokens()){ // iterate all words				
					String tempkey = mykey + ":" + itr.nextToken(); // create key-value map <class:word>, and get the probability						
					if(wordsProbably.containsKey(tempkey)){
						// if <class:word> exists in wordsProbably, get the probability
						tempValue += Math.log(wordsProbably.get(tempkey));
					}else{ // if doesn't exist, using the probability of class probability
						tempValue += Math.log(wordsProbably.get(mykey));						
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
}
