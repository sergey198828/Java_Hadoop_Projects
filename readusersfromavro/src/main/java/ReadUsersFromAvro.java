import java.io.IOException;
import java.net.URI;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadUsersFromAvro extends Configured implements Tool {

  public static class AvroUserMapper extends Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {
    @Override
    public void map(AvroKey<User> user, NullWritable voidValue, Context context) throws IOException, InterruptedException {
    	Text userText = new Text(user.datum().toString());
    	String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
		int begin = filePath.indexOf("regyear=")+8;
		int end = begin + 4;
		IntWritable regYear = new IntWritable(Integer.parseInt(filePath.substring(begin, end)));
    	context.write(userText, regYear);
    }
  }

  public static class AvroUserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text userText, Iterable<IntWritable> regYears, Context context) throws IOException, InterruptedException {
      for(IntWritable regYear : regYears){
    	context.write(userText, regYear);
      }
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: MapReduceColorCount <input path 1> <input path 2> ... <input path N> <output path> <schema path>");
      return -1;
    }
    //Create new job object and give name to it
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "readusersfromavro");
    job.setJarByClass(ReadUsersFromAvro.class);
    
    for(int i=0; i<=args.length-3; i++){
    	FileInputFormat.addInputPath(job, new Path(args[i]));
    }
        
    FileOutputFormat.setOutputPath(job, new Path(args[args.length-2]));
    URI schemaPath = new URI(args[args.length-1]);

    //Configure input
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapperClass(AvroUserMapper.class);
    AvroJob.setInputKeySchema(job, User.getClassSchema(schemaPath, conf));
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    //Configure output
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setCombinerClass(AvroUserReducer.class);
    job.setReducerClass(AvroUserReducer.class);    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);    

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ReadUsersFromAvro(), args);
    System.exit(res);
  }
}