package edu.whu.liwei.naivebayes;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Get PriorProbablility and WordsProbablility
 * @author Liwei
 *
 */
public class Probability {
	
	/**
	 * get class doc nums from ClassNameDocNumsReducer output
	 * @param filePath input ClassNameDocNumsReducer output
	 */
	public static HashMap<String, Integer> getClassDocNums(String classNameDocNumsFilePath) throws IOException {
		HashMap<String, Integer> classDocNums = new HashMap<String, Integer>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(classNameDocNumsFilePath), conf);
		Path path = new Path(classNameDocNumsFilePath);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf); 
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(key, value)) {
				classDocNums.put(key.toString(), value.get());
			}
		} finally {
			IOUtils.closeStream(reader);
		}			
		return classDocNums;	
	}
	
	/**
	 * get prior probably from ClassNameDocNumsReducer output
	 * @param filePath input ClassNameDocNumsReducer output
	 */
	public static HashMap<String, Double> getPriorProbablility(HashMap<String, Integer> classDocNums) throws IOException {	
		HashMap<String, Double> priorProbablility = new HashMap<String, Double>();
		int totalDocNums = 0;
		for (Integer values : classDocNums.values())
			totalDocNums += values;
		for (Entry<String, Integer> entry : classDocNums.entrySet()) 
			priorProbablility.put(entry.getKey(), entry.getValue() / (double) totalDocNums); // P(c) = doc_nums(c) / total_doc_nums
		return priorProbablility;
	}
	
	/**
	 * get words probably from ConditionProbablyMapper output
	 * @param filePath input ConditionProbablyMapper output
	 */
	public static HashMap<String, Double> getWordsProbablility(String conditionProbablyFilePath) throws IOException {
		HashMap<String, Double> wordsProbablility = new HashMap<String, Double>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(conditionProbablyFilePath), conf);
		Path path = new Path(conditionProbablyFilePath);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf); 
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			DoubleWritable value = (DoubleWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(key, value)) {
				wordsProbablility.put(key.toString(), value.get());
			}
		} finally {
			IOUtils.closeStream(reader);
		}	
		return wordsProbablility;
	} 
	
	/**
	 * get words probably from ConditionProbablyMapper output and add probability for words excluded in class
	 * @param filePath filePath input ConditionProbablyMapper output 
	 * @param classDocNums map className to class doc nums
	 */
	public static HashMap<String, Double> getWordsProbablility(String conditionProbablyFilePath, HashMap<String, Integer> classDocNums) throws IOException {
		HashMap<String, Double> WordsProbablility = getWordsProbablility(conditionProbablyFilePath);
		// add probability for words excluded in class 
		for (Entry<String, Integer> entry : classDocNums.entrySet()) {
			WordsProbablility.put(entry.getKey(), 1.0 / (entry.getValue() + 2)); // P(c) = 1 / total_doc_nums
		}
		return WordsProbablility;
	}
}
