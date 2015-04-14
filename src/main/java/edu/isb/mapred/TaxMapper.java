package edu.isb.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//PID-LEGAL_TYPE-FOLIO-LAND_COORDINATE-ZONE_NAME-ZONE_CATEGORY-PLAN-CURRENT_LAND_VALUE-TAX_ASSESSMENT_YEAR-TAX_PAID
		
		String line = value.toString();
		String[] tokens = line.split("\\,");
				
		String folio = tokens[2];
		String taxPaid = tokens[9];
		
		context.write(new Text(folio), new DoubleWritable(new Double(taxPaid).doubleValue()));
	}
}
