package com.testhadoop.reverseindex;

//Including general Java libraries
import java.io.IOException;
import java.util.*;

//Including Hadoop specific Java libraries
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/* Reverse index class implements MapReduce concept to build reverse index of files containing words and number of occurrences given random files in specified HDFS location */
public class ReverseIndex {

/* Map class implements Mapper interface of MapReduce */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
/* map method generates <word, filename> pairs and send them to reducer */
    	public void map(LongWritable key, Text inputFileLine, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Getting input file name
        	FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
        	Text fileName = new Text(fileSplit.getPath().getName());
            //Split input file string into words removing punctuation and converting to lower case
        	String[] words = inputFileLine.toString().replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split("\\s+");
            //For each word in file generating <word, filename> pair
        	for(String word : words)
        		output.collect(new Text(word), fileName);
        }
    }
    
/* Reduce class implements Reducer interface of MapReduce */
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
/* Reduce method for each word creates a Map of all file names and number of occurrences containing this word and print it using default toString method */
        public void reduce(Text word, Iterator<Text> fileNames, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Declare output file names and number of occurrences container
        	java.util.Map<String, Integer> outFileNames = new TreeMap<String, Integer>();
            //Fill container with file names and occurrences. Deduplicate and Sort using default Map behavior
        	while (fileNames.hasNext()){
        		String nextFileName = fileNames.next().toString();
        		int occurence =1;
        		//If input was already combined, split the string to key/value again
        		if(nextFileName.matches("\\{[a-z0-9]+=[a-z0-9]+\\}")){
        			String[] tempStr = nextFileName.substring(1, nextFileName.length()-1).split("=");
        			nextFileName = tempStr[0];
        			occurence = Integer.parseInt(tempStr[1]);
        		}
        		if(!outFileNames.containsKey(nextFileName)){
        			outFileNames.put(nextFileName, occurence);
        		}else{
        			int count = outFileNames.get(nextFileName); 
        			outFileNames.put(nextFileName, count + occurence);
        		}
        	}
            //Sending result to Hadoop
        	output.collect(word, new Text(outFileNames.toString()));
        }
    }
    
/* Start point of application reads input and output folders as argument, configures job and trigger it */
	public static void main(String[] args) throws Exception {
	    //Create new job object and give name to it
		JobConf conf = new JobConf(ReverseIndex.class);
	    conf.setJobName("reverseindex");
	    //Configure mapper output values
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    //Configure job output values types
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	    //Link mapper, reducer and combiner classes to the job
	    conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);
        //Configure input and output formats
    	conf.setInputFormat(TextInputFormat.class);
    	conf.setOutputFormat(TextOutputFormat.class);
        //Parse arguments (Input and output folders)
    	FileInputFormat.setInputPaths(conf, new Path(args[0]));
    	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        //Trigger job to run on cluster
    	JobClient.runJob(conf);
	}	
}
