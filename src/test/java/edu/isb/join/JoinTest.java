package edu.isb.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class JoinTest {
	
	private MapDriver<LongWritable, Text, Text, Text> addressJoinMapDriver;
	private MapDriver<LongWritable, Text, Text, Text> taxJoinMapDriver;
	private ReduceDriver<Text, Text, Text, Text> taxJoinReduceDriver;
	private Text addressLine1;
	private Text addressLine2;

	private Text aggregrateTaxLine1;
	private Text aggregrateTaxLine2;

	private List<Text> values;
	
	@Before
	public void setUp() {
		//Setup the input
		addressLine1 = new Text("601109050055,ALBERNI ST,V6G 3G7");
		addressLine2 = new Text("615118040024,BUTE ST,V6E 3Z9");

		aggregrateTaxLine1 = new Text("601109050055	5025.0");
		aggregrateTaxLine2 = new Text("615118040024	8500.0");

		//Setup the output
		values = new ArrayList<Text>();
		values.add(new Text("AALBERNI ST,V6G 3G7"));
		values.add(new Text("ABUTE ST,V6E 3Z9"));
		
		//Setup the test drivers
		AddressJoinMapper mapper = new AddressJoinMapper();
		addressJoinMapDriver = MapDriver.newMapDriver(mapper);

		TaxJoinMapper taxJoinMapper = new TaxJoinMapper();
		taxJoinMapDriver= MapDriver.newMapDriver(taxJoinMapper);
		
		TaxJoinReducer reducer = new TaxJoinReducer();
		taxJoinReduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void addressJoinMapperTest() throws IOException {
		addressJoinMapDriver.addInput(new LongWritable(0), addressLine1);
		addressJoinMapDriver.addInput(new LongWritable(1), addressLine2);
		addressJoinMapDriver.addOutput(new Text("601109050055"), new Text("A601109050055,ALBERNI ST,V6G 3G7"));
		addressJoinMapDriver.addOutput(new Text("615118040024"), new Text("A615118040024,BUTE ST,V6E 3Z9"));
		
		Pair<Text, Text> expectedTupple0 = new Pair<Text, Text>(new Text("601109050055"), new Text("A601109050055,ALBERNI ST,V6G 3G7"));
		Pair<Text, Text> expectedTupple1 = new Pair<Text, Text>(new Text("615118040024"), new Text("A615118040024,BUTE ST,V6E 3Z9"));
		
		List<Pair<Text, Text>> result = addressJoinMapDriver.run();
		
		assertNotNull(result);
		assertEquals(result.size(), 2);
		assertEquals(result.get(0), expectedTupple0);
		assertEquals(result.get(1), expectedTupple1);
	}

	@Test
	public void taxJoinMapperTest() throws IOException {
		taxJoinMapDriver.addInput(new LongWritable(0), aggregrateTaxLine1);
		taxJoinMapDriver.addInput(new LongWritable(1), aggregrateTaxLine2);
		taxJoinMapDriver.addOutput(new Text("601109050055"), new Text("B5025.0"));
		taxJoinMapDriver.addOutput(new Text("615118040024"), new Text("B8500.0"));
		
		Pair<Text, Text> expectedTupple0 = new Pair<Text, Text>(new Text("601109050055"), new Text("B5025.0"));
		Pair<Text, Text> expectedTupple1 = new Pair<Text, Text>(new Text("615118040024"), new Text("B8500.0"));
		
		List<Pair<Text, Text>> result = taxJoinMapDriver.run();
		
		assertNotNull(result);
		assertEquals(result.size(), 2);
		assertEquals(result.get(0), expectedTupple0);
		assertEquals(result.get(1), expectedTupple1);

	}

	@Test
	public void taxJoinReducerTest() throws IOException {
		
		List<Text> input1 = new ArrayList<Text>();
		input1.add(new Text("A601109050055,ALBERNI ST,V6G 3G7"));
		input1.add(new Text("B5025.0"));
		
		List<Text> input2 = new ArrayList<Text>();
		input2.add(new Text("B8500.0"));
		input2.add(new Text("A615118040024,BUTE ST,V6E 3Z9"));
		
		taxJoinReduceDriver.getConfiguration().set("join.type", "inner");
		taxJoinReduceDriver.addInput(new Text("601109050055"), input1);
		taxJoinReduceDriver.addInput(new Text("615118040024"), input2);
		
		taxJoinReduceDriver.addOutput(new Text("601109050055,ALBERNI ST,V6G 3G7"), new Text("5025.0"));
		taxJoinReduceDriver.addOutput(new Text("615118040024,BUTE ST,V6E 3Z9"), new Text("8500.0"));
		
		
		Pair<Text, Text> expectedTupple0 = new Pair<Text, Text>(new Text("601109050055,ALBERNI ST,V6G 3G7"), new Text("5025.0"));
		Pair<Text, Text> expectedTupple1 = new Pair<Text, Text>(new Text("615118040024,BUTE ST,V6E 3Z9"), new Text("8500.0"));
		
		List<Pair<Text, Text>> result = taxJoinReduceDriver.run();
		
		assertNotNull(result);
		assertEquals(result.size(), 2);
		assertEquals(result.get(0), expectedTupple0);
		assertEquals(result.get(1), expectedTupple1);

	}
	
	@Test
	  public void testJoinAgainstLocalFile() throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("fs.defaultFS", "file:///");
	    conf.set("mapreduce.framework.name", "local");
	    conf.setInt("mapreduce.task.io.sort.mb", 1);
	    
	    Path input1 = new Path("/home/ubuntu/work/eclipse-workspace-project/dmproject/src/test/resources/edu/isb/join/input/address/");
	    Path input2 = new Path("/home/ubuntu/work/eclipse-workspace-project/dmproject/src/test/resources/edu/isb/join/input/tax/");
	    Path output = new Path("/home/ubuntu/work/eclipse-workspace-project/dmproject/src/test/resources/edu/isb/join/output");
	    String joinType = "inner";
	    
	    FileSystem fs = FileSystem.getLocal(conf);
	    fs.delete(output, true); // delete old output
	    
	    JoinDriver joinDriver = new JoinDriver();
	    joinDriver.setConf(conf);
	    
	    int exitCode = joinDriver.run(new String[] {input1.toString(), input2.toString(), output.toString(), joinType});
	    assertEquals(exitCode, 0);
	  }
}
