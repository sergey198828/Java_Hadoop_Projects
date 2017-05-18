//Including general Java libraries
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

//Including Hadoop specific Java libraries
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

/* Reverse index class implements MapReduce concept to build reverse index of files containing words and number of occurrences given random files in specified 
 * HDFS location. Words contained in skipped words list will be excluded from calculation.
 * */
public class ReverseIndex {
	//Path on job initiator machine to skipped word list file
	private static final String LOCAL_SKIPWORD_LIST ="/home/hduser/SampleFiles/Text/skippedwordslist";
	
	//Path in HDFS to upload skipped word list file
	private static final String HDFS_SKIPWORD_LIST = "/tmp/skippedwordslist";
	
	//Method which uploads skipped word list to HDFS, then to Distributed cache and write HDFS file path to the Job configuration
	private static void cacheSkipWordList(Configuration conf, Job job) throws IOException, URISyntaxException{
			FileSystem fs = FileSystem.get(conf);
			//Upload the file to HDFS. Overwrite any existing copy.
			fs.copyFromLocalFile(false, true, new Path(LOCAL_SKIPWORD_LIST), new Path(HDFS_SKIPWORD_LIST));
			//Add file to distributed cache
			job.addCacheFile(new URI(HDFS_SKIPWORD_LIST));
	}
	
	/* Map class implements Mapper interface of MapReduce */
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritableComparable> {
    	//Resultant container
    	MapWritableComparable entry;
    	//Reporting counters
    	private enum RecordCounters { 
    		COUNTED, 
    		SKIPPED 
    	};
    	//Cache counters
    	private enum CacheCounters {
    		SETUP_ENTERED,
    		SOMETHING_WAS_CACHED,
    		LOAD_SKIPPED_WORDS_LAUNCHED,
    		WORDS_LOADED_FROM_CACHE
    	};
    	//Skipped words list as set of Strings
    	private Set<String> skippedWordsList = new HashSet<String>();
    	//Method which get  skipped words list from distributed cache
    	public void setup(Context context) throws IOException {
    		context.getCounter(CacheCounters.SETUP_ENTERED).increment(1);
			URI [] cacheFiles = context.getCacheFiles();
			if (null != cacheFiles && cacheFiles.length > 0) {
				context.getCounter(CacheCounters.SOMETHING_WAS_CACHED).increment(1);
				for (URI cacheFile : cacheFiles) {
					if (cacheFile.toString().equals(HDFS_SKIPWORD_LIST)) {
						context.getCounter(CacheCounters.LOAD_SKIPPED_WORDS_LAUNCHED).increment(1);
						loadSkippedWords(cacheFile, context);
						break;
					}
				}
    		}
    	}
    	//Parse skipped words list
    	private void loadSkippedWords(URI cacheFile, Context context) throws IOException {
    		FileSystem fs = FileSystem.get(context.getConfiguration());
    		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFile))));
    		try {
    			String line;
    			line = br.readLine();
    		    while (line != null) {
    		    	skippedWordsList.add(line);
    		    	context.getCounter(CacheCounters.WORDS_LOADED_FROM_CACHE).increment(1);
        			line = br.readLine();	
    		    }
    		} finally {
    			br.close();
    		}
    	}
    	/* map method generates <word, filename> pairs and send them to reducer */
    	public void map(LongWritable key, Text inputFileLine, Context context) throws IOException, InterruptedException {
    		//Getting input file name
        	FileSplit fileSplit = (FileSplit)context.getInputSplit();
        	Text fileName = new Text(fileSplit.getPath().getName());
        	entry = new MapWritableComparable();
        	entry.put(fileName, new LongWritable(1));
            //Split input file string into words removing punctuation and converting to lower case
        	String[] words = inputFileLine.toString().replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split("\\s+");
            //For each word in file generating <word, filename> pair
        	for(String word : words){
        		if(!skippedWordsList.contains(word)){
        			context.write(new Text(word), entry);
        			context.getCounter(RecordCounters.COUNTED).increment(1);
        		}else{
        			context.getCounter(RecordCounters.SKIPPED).increment(1);
        		}
        	}
        }
    }
    
    /* Reduce class implements Reducer interface of MapReduce */
    public static class Reduce extends Reducer<Text, MapWritableComparable, Text, MapWritableComparable> {
    	/* Reduce method for each word creates a Map of all file names and number of occurrences containing this word and print it using default toString method */
        public void reduce(Text word, Iterable<MapWritableComparable> entries, Context context) throws IOException, InterruptedException {
            //Declare output file names and number of occurrences container
        	MapWritableComparable outFileNames = new MapWritableComparable();
            //Fill container with file names and occurrences. Deduplicate and Sort using default Map behavior
        	for(MapWritableComparable entry : entries){
            	//Fetching SET of keys for current iteration MAP
            	Set<Writable> currentEntryFileSet = entry.keySet();
            	//For each key which represent filename in current iteration MAP
            	for(Writable file: currentEntryFileSet){
            		//Get this filename as key
            		Text fileName = (Text)file;
            		//Get this filename value
            		LongWritable occurence = (LongWritable)entry.get(fileName);
            		//If this key doesnt exist in resultant MAP already
            		if(!outFileNames.containsKey(fileName)){
            			//Just putting it and value
            			outFileNames.put(fileName, occurence);
            		}else{
            			//Add current keys value to stacked one
            			LongWritable stackedOccurencesWritable = (LongWritable)outFileNames.get(fileName);
            			Long stackedOccurences = stackedOccurencesWritable.get() + occurence.get();
            			outFileNames.put(fileName, new LongWritable(stackedOccurences));
            		}
            	}
        	}
            //Sending result to Hadoop
        	context.write(word, outFileNames);
        }
    }
    
/* Start point of application reads input and output folders as argument, configures job and trigger it. 
 * args[0] - HDFS path to input data
 * args[1] - HDFS path to output data
 * */
	public static void main(String[] args) throws Exception {
	    //Create new job object and give name to it
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "salestotal");
	    job.setJarByClass(ReverseIndex.class);
	    //Upload skipped word list to distributed cache
	    ReverseIndex.cacheSkipWordList(conf, job);
	    //Configure mapper output values
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(MapWritableComparable.class);
	    //Configure job output values types
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MapWritableComparable.class);
	    //Link mapper, reducer and combiner classes to the job
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
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
