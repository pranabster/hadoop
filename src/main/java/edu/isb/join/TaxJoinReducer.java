package edu.isb.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxJoinReducer  extends Reducer<Text, Text, Text, Text> {

	private static String  JOIN_TYPE_KEY = "join.type";
	private static String  JOIN_TYPE_VALUE = "inner";
	
	private String joinType;
	
	private List<Text> addressListWithAasExtension = new ArrayList<Text>();
	private List<Text> calculatedTaxListWithBasExtension = new ArrayList<Text>();

	public void setup(Context context) {
		// Get the type of join from our configuration
		joinType = context.getConfiguration().get("join.type");
		if (null == joinType) {
			context.getConfiguration().set(JOIN_TYPE_KEY, JOIN_TYPE_VALUE);
		}
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Clear our lists
		addressListWithAasExtension.clear();
		calculatedTaxListWithBasExtension.clear();
		
		// iterate through all our values
		for (Text tmp : values) {
			if (tmp.charAt(0) == 'A') {
				addressListWithAasExtension.add(new Text(tmp.toString().substring(1)));
			} else if (tmp.charAt(0) == 'B') {
				calculatedTaxListWithBasExtension.add(new Text(tmp.toString().substring(1)));
			}
		}
		
		// Execute our join logic now that the lists are filled
		executeJoinLogic(context);
	}

	
	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		if (joinType.equalsIgnoreCase("inner")) {
			// If both lists are not empty, join A with B
			if (!addressListWithAasExtension.isEmpty() && !calculatedTaxListWithBasExtension.isEmpty()) {
				for (Text address : addressListWithAasExtension) {
					for (Text calculatedTax : calculatedTaxListWithBasExtension) {
						context.write(address, calculatedTax);
					}
				}

			}
		}
	}

}
