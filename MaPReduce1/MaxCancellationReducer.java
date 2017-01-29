package BigData.MaxCancellation;
import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class MaxCancellationReducer extends
Reducer<Text, IntWritable, Text, IntWritable> {
/*
 * The reducer will not write the values as soon as they are aggregated. Rather, the airline
 * part of the key will be compared to the airline part of the previous key. If they are the same,
 * the sum will be compared. If the sum is greater, the new key and the new sum will be stored as
 * the key and value for max cancellation for that airline. If the airline part of the current and
 * previous keys are different, then the max cancellation for previous airline has already been 
 * determined and will be written out.
 */
	//The following two variables will store the key and sum for max cancellation for given airline
	Text maxKey = new Text();
    int max = 0;
    //The following variable will be a flag for the first time the reducer runs
    boolean firstTime = true;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		      throws IOException, InterruptedException {
		//Aggregate the sum of cancellation
		    int sum = 0;
		    for (IntWritable value : values) {
		      sum += value.get();
		    }
		//The first time reducer runs, there is no prior knowledge of cancellation
		//So we will treat the number of cancellation of the first route as max
		    if (firstTime){
		    	maxKey.set(key);
		    	max = sum;
		    	firstTime = false;
		    }
		/*
		 * The prior max Key Value pair for given airline is stored in maxKey and max.
		 * Split the airline part of the prior max key and that of the current key and
		 * compare
		 */
		    String [] chkKeyNew = key.toString().split("\t");
		    String [] chkKeyOld = maxKey.toString().split("\t");
		    if (chkKeyNew[0].equals(chkKeyOld[0])){
		  /*
		   * If the airline matches, check if the number f cancellation for current key is 
		   * greater. If yes, set this key value pair as max cancellation for the airline.
		   * Else leave the max key value pair as is	
		   */
		    	if(sum > max){
		    		max = sum;
		    		maxKey.set(key);
		    	}
		    } else {
		    	/*
		    	 * If the airline do not match, then max cancellation for the previous airline
		    	 * is already determined. So write that out. For the new airline, set the 
		    	 * current key and value as max, since this is the only pair we know so far
		    	 */
		    	context.write(maxKey, new IntWritable(max));
		    	max = sum;
		    	maxKey.set(key);
		    }
		    
		    
		  }
	/*
	 * Call the cleanup method to write the max cancellation for the last airline
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
	      context.write(maxKey, new IntWritable(max));
	}
}