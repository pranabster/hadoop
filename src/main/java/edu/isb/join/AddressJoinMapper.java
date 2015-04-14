package edu.isb.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddressJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Record columns : FOLIO,STREET_NAME,PROPERTY_POSTAL_CODE
		
		String line = value.toString();
		String[] tokens = line.split("\\,");
		
		String folio = tokens[0];
		StringBuilder outString = new StringBuilder("A");
		outString.append(tokens[0]);
		outString.append(",");
		outString.append(tokens[1]);
		outString.append(",");
		outString.append(tokens[2]);
		
		context.write(new Text(folio), new Text(outString.toString()));
	}
}