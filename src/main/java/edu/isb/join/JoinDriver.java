package edu.isb.join;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.isb.mapred.TaxCalculationDriver;

public class JoinDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TaxCalculationDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.printf("Usage: %s [generic options] <input1> <input2> <output> <joinType(Currently supports only inner join(inner))>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance(getConf(), "Tax calculation final result");
		job.setJarByClass(getClass());
		
		// Set the multiple input and the corresponding mapper classes 
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AddressJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaxJoinMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.getConfiguration().set("join.type", args[3]);

		// Set the reducer classes
		job.setReducerClass(TaxJoinReducer.class);

		// map output class types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
