//Including general Java libraries
import java.io.IOException;
import java.util.*;
//Including Hadoop specific Java libraries
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Extend MapWritable class to be comparable and printable well
class MapWritableComparable extends MapWritable implements WritableComparable<Object>{
	@Override
	public String toString(){
		StringBuilder result = new StringBuilder("{");
		Set<Writable> keysSet = this.keySet();
		TreeSet<Writable> sortedKeysSet = new TreeSet<Writable>(keysSet);
		Iterator<Writable> iterator = sortedKeysSet.iterator();
		while (iterator.hasNext()){
			result.append("{");
			Writable key = iterator.next();
			Writable value = this.get(key);
			result.append(key.toString()+":"+value.toString());
			if(iterator.hasNext()){
				result.append("},");
			}else{
				result.append("}");
			}
		}
		result.append("}");
		return result.toString();
	}
	@Override
	public boolean equals(Object arg0){
		return this.toString().equals(arg0.toString());
	}
	@Override
	public int hashCode(){
		return this.toString().hashCode();
	}
	
	public int compareTo(Object arg0){
		return this.toString().compareTo(arg0.toString());
	}
}

/* Sales total class implements MapReduce concept to calculate total sales per shop per category given sales files with following format:
 * DATE<tab>SHOP_NAME<tab>CATEGORY<tab>SALE</n> in specified HDFS location */
public class SalesTotal {
	/* Map class implements Mapper interface of MapReduce */
    public static class Map extends Mapper<Object, Text, Text, MapWritableComparable> {
    	private Text shop;
    	private Text category;
    	private DoubleWritable sale;
    	private MapWritableComparable result;
    	//Reporting counters
    	public static enum RecordCounters { 
    		BAD_LINE, 
    		MAP_EXCEPTION 
    	};
    	/* Map method generates <shop, <category,sales>> pair and send them to reducer */
    	public void map(Object key, Text inputFileLine, Context context) throws IOException, InterruptedException {
    		//Splitting line delimited by <tab> into individual values
    		String words[] = inputFileLine.toString().split("\\t");
    		//Check if string is valid (only 4 values acceptable by format)
       		if(words.length == 4){
       			//Conversion of string to text is always possible
       			shop = new Text(words[1].toLowerCase());
       			category = new Text(words[2].toLowerCase());
       			//Safely convert string to DoubleWritable and send result to reducer, report error to user in case of failure
       			try{
       				sale = new DoubleWritable(Double.parseDouble(words[3].replaceAll(",", ".")));
       				result = new MapWritableComparable();
       				result.put(category, sale);
       				context.write(shop, result);
       			}catch(Exception E){
       				context.getCounter(RecordCounters.MAP_EXCEPTION).increment(1);
       			}
       		}else{
       			context.getCounter(RecordCounters.BAD_LINE).increment(1);
       		}
        }
    }
    
    /* Reduce class implements Reducer interface of MapReduce */
    public static class Reduce extends Reducer<Text, MapWritableComparable, Text, MapWritableComparable> {
    	MapWritableComparable result;
    	/* Reduce method for each shop calculates total sales in each category*/    
        public void reduce(Text shop, Iterable<MapWritableComparable> catSales, Context context) throws IOException, InterruptedException {
        	//Creating container for resultant MAP
        	result = new MapWritableComparable();
            //Merging multiple MAPs stored in catSales iterator to resultant container
            //For each MAP in catSales iterator
        	for(MapWritableComparable currentCatSaleMap : catSales) {
            	//Fetching SET of keys for current iteration MAP
            	Set<Writable> currentCatSaleKeysSet = currentCatSaleMap.keySet();
            	//For each key which represent category in current iteration MAP
            	for(Writable category: currentCatSaleKeysSet){
            		//Get this category as key
            		Text categoryName = (Text)category;
            		//Get this category sales value
            		DoubleWritable sale = (DoubleWritable)currentCatSaleMap.get(categoryName);
            		//If this key doesnt exist in resultant MAP already
            		if(!result.containsKey(categoryName)){
            			//Just putting it and value
            			result.put(categoryName, sale);
            		}else{
            			//Add current keys value to stacked one
            			DoubleWritable stackedSalesWritable = (DoubleWritable)result.get(categoryName);
            			Double stackedSales = stackedSalesWritable.get() + sale.get();
            			result.put(categoryName, new DoubleWritable(stackedSales));
            		}
            	}
            }
            context.write(shop, result);
        }
    }

/* Start point of application reads input and output folders as argument, configures job and trigger it */
	public static void main(String[] args) throws Exception {
		//Create new job object and give name to it
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "salestotal");
	    job.setJarByClass(SalesTotal.class);
	    //Configure output values types	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MapWritableComparable.class);
	    //Link mapper, reducer and combiner classes to the job	
	    job.setMapperClass(Map.class);
	    //job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
        //Configure input and output formats
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
        //Parse arguments (Input and output folders)
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Trigger job to run on cluster
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
