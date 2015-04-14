package edu.isb.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] tokens = line.split("\\s");
		
		//FOLIO-TAX_PAID
		String folio = tokens[0];
		String outValue = "B"+tokens[1].toString(); 
		context.write(new Text(folio), new Text(outValue));
	}
}
