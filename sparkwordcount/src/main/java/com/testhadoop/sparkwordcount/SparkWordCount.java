package com.testhadoop.sparkwordcount;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkWordCount {
	private static final String APP_NAME = "Spark Word Count";

	public static void main(String[] args) {
		if(args.length < 4){
			System.out.println("<Cluster master:local/FQDN><FS type:local/HDFS><inputFile><outputFile>");
			return;
		}
		String master = args[0];
		String inputFile;
		String outputFile;
		if(args[1].equals("HDFS")){
			inputFile="hdfs://nn1:8020"+args[2];
			outputFile="hdfs://nn1:8020"+args[3];
		}else{
			inputFile=args[2];
			outputFile=args[3];
		}
		
		SparkConf conf = new SparkConf().setMaster(master).setAppName(APP_NAME);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load our input data
		JavaRDD<String> input = sc.textFile(inputFile);

		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String,String>(){
			public Iterator<String> call(String line) throws Exception {
				String result = line.replaceAll("[^a-zA-Z0-9 ]", "").toUpperCase();
				return Arrays.asList(result.split("\\s+")).iterator();
			}	
		});
		
		// Transform into pairs
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String,Integer>(word,1);
			}
		});
		
		//Reduce pairs by count
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
			public Integer call(Integer x, Integer y){
				return x + y;
			}
		});
		
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
	}
}
