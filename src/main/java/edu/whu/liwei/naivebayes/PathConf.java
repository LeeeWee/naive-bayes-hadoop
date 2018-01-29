package edu.whu.liwei.naivebayes;

public class PathConf {

	public String trainingDataPath;
	public String testDataPath;
	public String classDocNumsPath;
	public String classWordDocNumsPath;
	public String conditionProbabilityPath;
	public String predictionResultPath;
	
	public PathConf() {}
	
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
