import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WindowsLogParser {
	
	/* Map class extends Mapper class of MapReduce */
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
    	private static final String ATTRIBUTE_NAME = "windows.log.pasrser.attribute";
    	private static final LongWritable ONE = new LongWritable(1);
    	private Text eventID; 
    	private enum RecordCounters{
    		NUM_EVENTS;
    	};
    	public static void setAttribute(String attribute, Configuration conf){
    		conf.set(ATTRIBUTE_NAME, attribute);
    	}
    	/* map method generates <EventID, 1> pairs and send them to reducer/combiner */
    	public void map(LongWritable key, Text inputXmlObject, Context context) throws IOException, InterruptedException {
    		eventID = new Text(XmlHelper.getAttributeData(inputXmlObject.toString(), context.getConfiguration().get(ATTRIBUTE_NAME)));
    		context.getCounter(RecordCounters.NUM_EVENTS).increment(1);
    		context.write(eventID, ONE);
    	}
    }
    /*Reduce class extends Reducer class of MapReduce*/
    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
    	/*Reduce/Combine method summs up amount of occurences of each eventID*/
    	private enum GroupCounters{
    		NUM_EVENT_IDS;
    	};
    	public void reduce(Text eventID, Iterable<LongWritable> occurences, Context context) throws IOException, InterruptedException {
    		long sum = 0;
    		for(LongWritable occurence: occurences){
    			sum += occurence.get();
    		}
    		context.getCounter(GroupCounters.NUM_EVENT_IDS).increment(1);
    		context.write(eventID, new LongWritable(sum));
    	}
    }
	public static void main(String[] args) throws Exception {
	    //Create new job object and give name to it
		Configuration conf = new Configuration();
	    //Write args to configuration
	    XmlInputFormat.setTags(args[2], args[3], conf);
	    Map.setAttribute(args[4], conf);
	    //Create a job
	    Job job = Job.getInstance(conf, "windowslogparser");
	    //Assign class to job
	    job.setJarByClass(WindowsLogParser.class);
	    //Configure MAP/REDUCE/COMBINE classes
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);   
	    //Configure mapper output values
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    //Configure job output values types
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
        //Configure input and output formats
	    job.setInputFormatClass(XmlInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
        //Parse arguments (Input and output folders)
	    XmlInputFormat.addInputPath(job, new Path(args[0]));
	    TextOutputFormat.setOutputPath(job, new Path(args[1]));
	    //Writing args to console
	    System.out.println("Reading files from: "+args[0]);
	    System.out.println("Writing result to: "+args[1]);
	    System.out.println("Start tag: "+conf.get(XmlInputFormat.START_TAG_KEY));
	    System.out.println("End tag: "+conf.get(XmlInputFormat.END_TAG_KEY));
        //Trigger job to run on cluster
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
