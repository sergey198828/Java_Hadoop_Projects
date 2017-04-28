package com.testhadoop.salestotal;

//Including general Java libraries
import java.io.IOException;
import java.util.*;

//Including Hadoop specific Java libraries
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/* Sales total class implements MapReduce concept to calculate total sales per shop given sales files with following format:
 * date<tab>shop name<tab>category<tab>sale in specified HDFS location */
public class SalesTotal {

/* Map class implements Mapper interface of MapReduce */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
/* map method generates <shop, sales> pair and send them to reducer */
    	public void map(LongWritable key, Text inputFileLine, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
    		//Splitting line delimited by <tab> into individual values
    		String words[] = inputFileLine.toString().split("\\t");
    		//Check if string is valid (only 4 values acceptable by format)
       		if(words.length == 4){
       			//Conversion of string to text is always possible
       			Text shop = new Text(words[1].toLowerCase());
       			//Safely convert string to DoubleWritable and send result to reducer, report error to user in case of failure
       			try{
       				DoubleWritable sales = new DoubleWritable(Double.parseDouble(words[3].replaceAll(",", ".")));
           			output.collect(shop, sales);
       			}catch(Exception E){
       				reporter.setStatus(E.getMessage());
       			}
       		}
        }
    }
    
/* Reduce class implements Reducer interface of MapReduce */
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
/* Reduce method for each word creates a string of all file names containing this word and delimited by spaces */    
        public void reduce(Text shop, Iterator<DoubleWritable> sales, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        	//Calculating SUM and sent to user
        	Double sum = 0.0;
            while (sales.hasNext()) {
            	sum += sales.next().get();
            }
            //Conversion of Double to Double Writable is always possible
            output.collect(shop, new DoubleWritable(sum));
        }
    }

/* Start point of application reads input and output folders as argument, configures job and trigger it */
	public static void main(String[] args) throws Exception {
	    //Create new job object and give name to it
	    JobConf conf = new JobConf(SalesTotal.class);
	    conf.setJobName("salestotal");
	    //Configure output values types	
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);
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
