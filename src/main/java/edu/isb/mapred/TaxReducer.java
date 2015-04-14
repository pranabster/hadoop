package edu.isb.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  
  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    
    double taxPaid = Double.MIN_VALUE;
    
    for (DoubleWritable value : values) {
      taxPaid = taxPaid + value.get();
    }
    context.write(key, new DoubleWritable(taxPaid));
  }
}