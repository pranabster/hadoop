package edu.isb.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TaxTest {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	private Text value;
	private Text value1;
	private List<DoubleWritable> values;
	
	@Before
	public void setUp() {
		//Setup the input
		value = new Text("005-237-262,STRATA,615118040024,61511804,RM-5A,Multiple Family Dwelling,VAS666,166000.00,2011,1000.00");
		value1 = new Text("005-237-262,STRATA,615118040024,61511804,RM-5A,Multiple Family Dwelling,VAS666,166000.00,2011,2000.00");

		//Setup the output
		values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1000.00));
		values.add(new DoubleWritable(2000.00));
		
		//Setup the test drivers
		TaxMapper mapper = new TaxMapper();
		TaxReducer reducer = new TaxReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void taxMapperTest() throws IOException {
		mapDriver.addInput(new LongWritable(0), value);
		mapDriver.addInput(new LongWritable(1), value1);
		mapDriver.addOutput(new Text("615118040024"), new DoubleWritable(1000.00));
		mapDriver.addOutput(new Text("615118040024"), new DoubleWritable(2000.00));
		mapDriver.runTest();
	}

	@Test
	public void taxReducertest() throws IOException {
		reduceDriver.withInput(new Text("615118040024"), values);
		reduceDriver.withOutput(new Text("615118040024"), new DoubleWritable(3000.00));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(0), value);
		mapReduceDriver.withInput(new LongWritable(1), value1);
		mapReduceDriver.withOutput(new Text("615118040024"), new DoubleWritable(3000.00));
		mapReduceDriver.runTest();
	}
	
	@Test
	  public void testAgainstLocalFile() throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("fs.defaultFS", "file:///");
	    conf.set("mapreduce.framework.name", "local");
	    conf.setInt("mapreduce.task.io.sort.mb", 1);
	    
	    Path input = new Path("/home/ubuntu/work/eclipse-workspace-project/dmproject/src/test/resources/edu/isb/mapred/input/taxinput.csv");
	    Path output = new Path("/home/ubuntu/work/eclipse-workspace-project/dmproject/src/test/resources/edu/isb/mapred/output");
	    
	    FileSystem fs = FileSystem.getLocal(conf);
	    fs.delete(output, true); // delete old output
	    
	    TaxCalculationDriver taxCalculaterDriver = new TaxCalculationDriver();
	    taxCalculaterDriver.setConf(conf);
	    
	    int exitCode = taxCalculaterDriver.run(new String[] {input.toString(), output.toString()});
	    assertEquals(exitCode, 0);
	  }
}
