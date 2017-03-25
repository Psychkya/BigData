package BigData.MaxCancellation;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class MaxCancellationReducer extends
Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		    int sum = 0;
		    int max = 0;
		    String maxVal = ""; //Will contain the route (orig/dest) with max number of cancellation
		    String maxRoute = ""; //temporary variable to store previous route for comparison
		    HashMap<String, Integer> valHash = new HashMap<String,Integer> ();
		    for (Text value: values){
		    	if (valHash.containsKey(value.toString())){
		    		valHash.put(value.toString(), (Integer)valHash.get(value.toString()) + 1);
		    	} else {
		    		valHash.put(value.toString(), 1);
		    	}
		    	sum = (Integer) valHash.get(value.toString());
		    	if ( sum > max) {
		    		max = sum;
		    		maxRoute = value.toString();
		    	}
		    }
		    maxVal = maxRoute.toString() + "\t" + max;
		    //Write the key to output
		    context.write(key, new Text(maxVal));
		  }

}