package edu.whu.liwei.naivebayes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import edu.whu.liwei.naivebayes.mapreduce.ClassDocNumsMapReduce;
import edu.whu.liwei.naivebayes.mapreduce.ClassWordDocsMapReduce;
import edu.whu.liwei.naivebayes.mapreduce.ConditionProbablilityMapReduce;
import edu.whu.liwei.naivebayes.mapreduce.DocClassPredicitonMapReduce;

/**
 * Include train and evaluation mathod
 * @author Liwei
 *
 */
public class NaiveBayes {
	
	public static void train(PathConf pathConf) throws Exception {
		
		String dateFormat = "HH:mm:ss:SSS";
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		// get begin time
		long startTime = System.currentTimeMillis();
		System.out.println("Strat training at " + sdf.format(new Date(startTime)));
		
		Configuration conf = new Configuration();

		ClassDocNumsMapReduce.run(conf, pathConf);
		
		ClassWordDocsMapReduce.run(conf, pathConf);
		
		ConditionProbablilityMapReduce.run(conf, pathConf);		
		
		// get end time
		long endTime = System.currentTimeMillis();
		System.out.println("Finished training at " + sdf.format(new Date(endTime)));
		System.out.println("Total cost " + (endTime - startTime)/1000.0 + "s");
	}
	
	
	
	public static void evaluation(PathConf pathConf) throws Exception {
		String dateFormat = "HH:mm:ss:SSS";
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		// get begin time
		long startTime = System.currentTimeMillis();
		// if jobCtrl doesn't all finished, block process
		System.out.println("Start predicting at " + sdf.format(new Date(startTime)));
	
		Configuration conf = new Configuration();
		
		DocClassPredicitonMapReduce.run(conf, pathConf);
		
		// get predicition result
		HashMap<String, String> docPredictionResult = new HashMap<String, String>();
		FileSystem hdfs = FileSystem.get(conf);
		SequenceFile.Reader reader = null;
		reader = new SequenceFile.Reader(hdfs, new Path(pathConf.predictionResultPath + "/part-r-00000"), conf); 
		Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while (reader.next(key, value)) {
			docPredictionResult.put(key.toString(), value.toString());
		}
		
		// get end time
		long endTime = System.currentTimeMillis();
		System.out.println("Finished prediciton at " + sdf.format(new Date(endTime)));
		System.out.println("Total cost " + (endTime - startTime)/1000.0 + "s");
		
		
		System.out.println("Evaluating...");
		// compare real label of doc and predict label
		int right = 0, total = 0;
		FSDataInputStream inputStream = hdfs.open(new Path(pathConf.testDataPath));
		BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
		String line = "";
		while ((line = buffer.readLine()) != null) {
			int splitIndex1 = line.toString().indexOf(":");
			int splitIndex2 = line.toString().indexOf(",");
			String docId = line.toString().substring(0, splitIndex1);
			String className = line.toString().substring(splitIndex1 + 1, splitIndex2);
			if (docPredictionResult.get(docId).equals(className))
				right++;
			total++;
		}
		System.out.println("Accuracy: " + right + "/" + total + " = " + right/(double)total);
	}
	
	
	public static void main(String[] args) throws Exception {
		
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
