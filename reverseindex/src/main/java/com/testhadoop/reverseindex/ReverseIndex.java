package com.testhadoop.reverseindex;
//Including general Java libraries
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
//Including Hadoop specific Java libraries
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;

/* Reverse index class implements MapReduce concept to build reverse index of files containing words and number of occurrences given random files in specified 
 * HDFS location. Words contained in skipped words list will be excluded from calculation.
 * */
public class ReverseIndex {
	//Path on job initiator machine to skipped word list file
	private static final String LOCAL_SKIPWORD_LIST ="/home/hduser/SampleFiles/Text/skippedwordslist";
	//Path in HDFS to upload skipped word list file
	private static final String HDFS_SKIPWORD_LIST = "/tmp/skippedwordslist";
	//Method which uploads skipped word list to HDFS, then to Distributed cache and write HDFS file path to the Job configuration
	private static void cacheSkipWordList(JobConf conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(HDFS_SKIPWORD_LIST);
		//Upload the file to hdfs. Overwrite any existing copy.
		fs.copyFromLocalFile(false, true, new Path(LOCAL_SKIPWORD_LIST), hdfsPath);
		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	}
	/* Map class implements Mapper interface of MapReduce */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	//Reporting counters
    	private enum RecordCounters { COUNTED, SKIPPED };
    	//Skipped words list as set of Strings
    	private Set<String> skippedWordsList = new HashSet<String>();
    	//Method which get  skipped words list from distributed cache
    	public void configure(JobConf conf) {
    		try {
    			String skipWordCacheName = new Path(HDFS_SKIPWORD_LIST).getName();
    		    Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
    		    if (null != cacheFiles && cacheFiles.length > 0) {
    		    	for (Path cachePath : cacheFiles) {
    		    		if (cachePath.getName().equals(skipWordCacheName)) {
    		    			loadSkippedWords(cachePath);
    		    			break;
    		    		}
    		        }
    		    }
    		} catch (IOException ioe) {
    			System.err.println("IOException reading from distributed cache");
    		    System.err.println(ioe.toString());
    		}
    	}
    	//Parse skipped words list
    	private void loadSkippedWords(Path cachePath) throws IOException {
    		// note use of regular java.io methods here - this is a local file now
    		BufferedReader wordReader = new BufferedReader(
    		new FileReader(cachePath.toString()));
    		try {
    			String line;
    		    while ((line = wordReader.readLine()) != null) {
    		    	skippedWordsList.add(line);
    		    }
    		} finally {
    			wordReader.close();
    		}
    	}
    	/* map method generates <word, filename> pairs and send them to reducer */
    	public void map(LongWritable key, Text inputFileLine, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Getting input file name
        	FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
        	Text fileName = new Text(fileSplit.getPath().getName());
            //Split input file string into words removing punctuation and converting to lower case
        	String[] words = inputFileLine.toString().replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split("\\s+");
            //For each word in file generating <word, filename> pair
        	for(String word : words){
        		if(!skippedWordsList.contains(word)){
        			output.collect(new Text(word), fileName);
        			reporter.incrCounter(RecordCounters.COUNTED, 1);
        		}else{
        			reporter.incrCounter(RecordCounters.SKIPPED, 1);
        		}
        	}
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
        		if(nextFileName.matches("\\{[a-zA-Z0-9]+=[0-9]+\\}")){
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
    
/* Start point of application reads input and output folders as argument, configures job and trigger it. 
 * args[0] - HDFS path to input data
 * args[1] - HDFS path to output data
 * */
	public static void main(String[] args) throws Exception {
	    //Create new job object and give name to it
		JobConf conf = new JobConf(ReverseIndex.class);
	    conf.setJobName("reverseindex");
	    //Upload skipped word list to distributed cache
	    ReverseIndex.cacheSkipWordList(conf);
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
