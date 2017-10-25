import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;
import junit.framework.TestCase;

public class WindowsLogParserTest extends TestCase {
	MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;	
	MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
	static final LongWritable ONE = new LongWritable(1);
	
	@Before
	public void setUp()
	{
		WindowsLogParser.Map mapper = new WindowsLogParser.Map();
		WindowsLogParser.Reduce reducer = new WindowsLogParser.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);		
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}	
	
	@Test
	public void testXmlHelper() throws IOException{
		String input = "<Event><System><EventID>4672</EventID></System></Event>";
		String expectedOutput = "4672";
		String output = XmlHelper.getAttributeData(input,"EventID");
		assertEquals(output,expectedOutput);
	}
	
	@Test
	public void testWindowsLogParserMapper() throws IOException{
		Text expectedResultKey = new Text("4672");
		LongWritable expectedResultValue = ONE;
		mapDriver.getConfiguration().set("windows.log.pasrser.attribute", "EventID");
		mapDriver.withInput(new LongWritable(), new Text("<Event><System><EventID>4672</EventID></System></Event>"));
		mapDriver.withOutput(expectedResultKey, expectedResultValue);
		mapDriver.runTest();
	}
	
	@Test
	public void testWindowsLogParserReducer() throws IOException{
		List<LongWritable> inputValues = new ArrayList<LongWritable>();
		inputValues.add(ONE);
		inputValues.add(ONE);
		inputValues.add(ONE);
		inputValues.add(ONE);
		
		LongWritable expectedResult = new LongWritable(4);	
		
		reduceDriver.withInput(new Text("4672"), inputValues);
		reduceDriver.addInput(new Text("4673"), inputValues);
		reduceDriver.withOutput(new Text("4672"),expectedResult);
		reduceDriver.addOutput(new Text("4673"),expectedResult);
		reduceDriver.runTest();
	}
	
	@Test
	public void testWindowsLogParserMapReduce() throws IOException{
		mapReduceDriver.getConfiguration().set("windows.log.pasrser.attribute", "EventID");
		mapReduceDriver.withInput(new LongWritable(), new Text("<Event><System><EventID>4672</EventID></System></Event>"));
		mapReduceDriver.addInput(new LongWritable(), new Text("<Event><System><EventID>4672</EventID></System></Event>"));		
		mapReduceDriver.addInput(new LongWritable(), new Text("<Event><System><EventID>4672</EventID></System></Event>"));
		mapReduceDriver.addInput(new LongWritable(), new Text("<Event><System><EventID>4672</EventID></System></Event>"));
		mapReduceDriver.addInput(new LongWritable(), new Text("<Event><System><EventID>4673</EventID></System></Event>"));
		mapReduceDriver.addInput(new LongWritable(), new Text("<Event><System><EventID>4673</EventID></System></Event>"));		
		mapReduceDriver.addInput(new LongWritable(), new Text("<Event><System><EventID>4673</EventID></System></Event>"));
		mapReduceDriver.addInput(new LongWritable(), new Text("<Event><System><EventID>4673</EventID></System></Event>"));
		
		LongWritable expectedResult = new LongWritable(4);		
		
		mapReduceDriver.withOutput(new Text("4672"),expectedResult);
		mapReduceDriver.addOutput(new Text("4673"),expectedResult);		
		mapReduceDriver.runTest();
	}
	
}
