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

public class SalesTotalTest extends TestCase
{
	MapDriver<Object, Text, Text, MapWritableComparable> mapDriver;
	ReduceDriver<Text, MapWritableComparable, Text, MapWritableComparable> reduceDriver;
	MapReduceDriver<Object, Text, Text, MapWritableComparable, Text, MapWritableComparable> mapReduceDriver;


	
	@Before
	public void setUp()
	{
		SalesTotal.Map mapper = new SalesTotal.Map();
		SalesTotal.Reduce reducer = new SalesTotal.Reduce();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
	    
	}	  
	/*
	 * Test MapWritableComoarable class
	 * 
	 */
	@Test
	public void testMapWritableComparable()
	{
		 MapWritableComparable map1 = new MapWritableComparable();
		 MapWritableComparable map2 = new MapWritableComparable();
		 map1.put(new Text("Hello"), new DoubleWritable(1.3d));
		 map1.put(new Text("GoodBye"), new DoubleWritable(1.4d));
		 map2.put(new Text("GoodBye"), new DoubleWritable(1.4d));
		 map2.put(new Text("Hello"), new DoubleWritable(1.3d));
		 assertEquals("{{GoodBye:1.4},{Hello:1.3}}",map1.toString());
		 assertEquals("{{GoodBye:1.4},{Hello:1.3}}",map2.toString());
		 assertEquals(0, map1.compareTo(map2));
		 assertEquals(true, map1.equals(map2));
		 assertEquals(map1.hashCode(), map2.hashCode());
	}
	/*
	 * Test Mapper
	 * 
	 */
	@Test
	public void testMaperNormalOperation()
	{
		MapWritableComparable expectedResult = new MapWritableComparable();
		expectedResult.put(new Text("drinks"), new DoubleWritable(716.12d));
		mapDriver.withInput(new LongWritable(), new Text("10/10/2014	Shop1	Drinks	716.12"));
		mapDriver.withOutput(new Text("shop1"),expectedResult);
		mapDriver.runTest();
	}
	@Test
	public void testMaperBadLineException()
	{
		mapDriver.withInput(new LongWritable(), new Text("10/10/2015 Unable to connect to database"));
		mapDriver.runTest();
		assertEquals(1, mapDriver.getCounters().findCounter(SalesTotal.Map.RecordCounters.BAD_LINE).getValue());
	}
	@Test
	public void testMaperException()
	{
		mapDriver.withInput(new LongWritable(), new Text("10/10/2014	Shop1	Drinks	test"));
		mapDriver.runTest();
		assertEquals(1, mapDriver.getCounters().findCounter(SalesTotal.Map.RecordCounters.MAP_EXCEPTION).getValue());		
	}	
	/*
	 * Test Reducer
	 * 
	 */
	@Test
	public void testReducer()
	{
		List<MapWritableComparable> inputMaps = new ArrayList<MapWritableComparable>();
		MapWritableComparable map1 = new MapWritableComparable();
		MapWritableComparable map2 = new MapWritableComparable();
		MapWritableComparable map3 = new MapWritableComparable();
		MapWritableComparable map4 = new MapWritableComparable();
		map1.put(new Text("drinks"), new DoubleWritable(10.1d));
		map2.put(new Text("chips"), new DoubleWritable(11.1d));
		map3.put(new Text("chips"), new DoubleWritable(12.1d));
		map4.put(new Text("drinks"), new DoubleWritable(13.1d));
		inputMaps.add(map1);
		inputMaps.add(map2);
		inputMaps.add(map3);
		inputMaps.add(map4);
		
		MapWritableComparable expectedResult = new MapWritableComparable();
		expectedResult.put(new Text("drinks"), new DoubleWritable(23.2d));
		expectedResult.put(new Text("chips"), new DoubleWritable(23.2d));		
		
		reduceDriver.withInput(new Text("shop1"), inputMaps);
		reduceDriver.withOutput(new Text("shop1"),expectedResult);
		reduceDriver.runTest();
	}
	/*
	 * Test MapReduce
	 */
	public void testMapReduce(){
		mapReduceDriver.withInput(new LongWritable(), new Text("10/10/2014	Shop1	Drinks	716.12"));
		mapReduceDriver.addInput(new LongWritable(), new Text("10/10/2014	shop1	drinks	716.12"));
		mapReduceDriver.addInput(new LongWritable(), new Text("10/10/2014	Shop2	Drinks	716.12"));
		mapReduceDriver.addInput(new LongWritable(), new Text("10/10/2014	shop2	drinks	716.12"));
		mapReduceDriver.addInput(new LongWritable(), new Text("10/10/2014	Shop1	Chips	716.12"));
		mapReduceDriver.addInput(new LongWritable(), new Text("10/10/2014	shop1	chips	716.12"));
		mapReduceDriver.addInput(new LongWritable(), new Text("10/10/2014	Shop2	Chips	716.12"));
		mapReduceDriver.addInput(new LongWritable(), new Text("10/10/2014	shop2	chips	716.12"));
		mapReduceDriver.setCombiner(new SalesTotal.Reduce());
		MapWritableComparable expectedShop1 = new MapWritableComparable();
		MapWritableComparable expectedShop2 = new MapWritableComparable();

		expectedShop1.put(new Text("chips"), new DoubleWritable(1432.24d));
		expectedShop1.put(new Text("drinks"), new DoubleWritable(1432.24d));
		expectedShop2.put(new Text("drinks"), new DoubleWritable(1432.24d));
		expectedShop2.put(new Text("chips"), new DoubleWritable(1432.24d));
	
		mapReduceDriver.withOutput(new Text("shop1"), expectedShop1);
		mapReduceDriver.addOutput(new Text("shop2"), expectedShop2);
		mapReduceDriver.runTest();
	}
}
