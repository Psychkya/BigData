package BigData.CommonFollower;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;

public class CommonFollowerReducer1 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		
		ArrayList<String> outVals = new ArrayList<String>();
		
		for (Text val: values){
			outVals.add(val.toString());
		}
		
		for (int i = 0; i < outVals.size(); i++){
			for (int j = i + 1; j < outVals.size(); j++){
				String out;
				if (outVals.get(i).compareTo(outVals.get(j)) < 0 ){
					out = outVals.get(i) + "\t " + outVals.get(j) + "\t";
				}else {
					out = outVals.get(j) + "\t " + outVals.get(i) + "\t";
				}
				context.write(new Text(out), key);
			}
		}
		
	}

}
